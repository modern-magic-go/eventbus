package otel_test

import (
	"context"
	"errors"
	"testing"

	eventbus "github.com/modern-magic-go/eventbus"
	otelbus "github.com/modern-magic-go/eventbus/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestPublishCreatesProducerSpanAndInjectsTraceparent(t *testing.T) {
	sr, tp := setupTracer(t)
	useGlobalProvider(t, tp)
	useW3CPropagator(t)

	rt := &fakeRuntime{}
	bus := eventbus.NewBus(rt)

	parentCtx, parent := tp.Tracer("test").Start(context.Background(), "parent")
	defer parent.End()

	const topic = "orders.created"
	inHeaders := map[string]string{
		"event_type":    "order.created",
		"event_version": "1",
		"aggregate_id":  "agg-1",
		"dedup_key":     "dedup-1",
	}
	err := otelbus.Publish(parentCtx, bus, topic, map[string]string{"id": "o-1"}, inHeaders, eventbus.WithMessageID("msg-1"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(rt.published) != 1 {
		t.Fatalf("runtime received %d messages, want 1", len(rt.published))
	}
	msg := rt.published[0]
	if msg.Topic != topic {
		t.Fatalf("message topic = %q, want %q", msg.Topic, topic)
	}
	if msg.ID != "msg-1" {
		t.Fatalf("message ID = %q, want %q (WithMessageID not forwarded)", msg.ID, "msg-1")
	}

	// 业务 header 全部保留。
	for k, v := range inHeaders {
		if got := msg.Headers[k]; got != v {
			t.Errorf("message header %q = %q, want %q", k, got, v)
		}
	}
	// 调用方传入的 map 不能被改写。
	if _, ok := inHeaders["traceparent"]; ok {
		t.Fatal("Publish mutated the caller's headers map")
	}

	producerSC := spanContextFromTraceparent(t, msg.Headers)
	if got, want := producerSC.TraceID(), parent.SpanContext().TraceID(); got != want {
		t.Fatalf("traceparent trace ID = %s, want parent trace ID %s", got, want)
	}

	producer := endedSpan(t, sr, "eventbus.publish "+topic)
	if got := producer.SpanKind(); got != trace.SpanKindProducer {
		t.Fatalf("producer span kind = %v, want %v", got, trace.SpanKindProducer)
	}
	if got := producer.InstrumentationScope().Name; got != instrumentationScope {
		t.Fatalf("instrumentation scope = %q, want %q", got, instrumentationScope)
	}
	if got, want := producerSC.SpanID(), producer.SpanContext().SpanID(); got != want {
		t.Fatalf("traceparent span ID = %s, want producer span ID %s", got, want)
	}
	if got := producer.Parent(); got.TraceID() != parent.SpanContext().TraceID() || got.SpanID() != parent.SpanContext().SpanID() {
		t.Fatalf("producer parent = %s/%s, want %s/%s",
			got.TraceID(), got.SpanID(), parent.SpanContext().TraceID(), parent.SpanContext().SpanID())
	}
	if got, _ := stringAttribute(producer, "messaging.system"); got != "redis" {
		t.Fatalf("messaging.system = %q, want %q", got, "redis")
	}
	if got, _ := stringAttribute(producer, "messaging.destination.name"); got != topic {
		t.Fatalf("messaging.destination.name = %q, want %q", got, topic)
	}
	if got := producer.Status().Code; got != codes.Unset {
		t.Fatalf("producer span status = %v, want unset on success", got)
	}
}

func TestPublishWithoutParentSpan(t *testing.T) {
	sr, tp := setupTracer(t)
	useGlobalProvider(t, tp)
	useW3CPropagator(t)

	rt := &fakeRuntime{}
	bus := eventbus.NewBus(rt)

	const topic = "audit.log"
	if err := otelbus.Publish(context.Background(), bus, topic, nil, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(rt.published) != 1 {
		t.Fatalf("runtime received %d messages, want 1", len(rt.published))
	}

	producerSC := spanContextFromTraceparent(t, rt.published[0].Headers)
	producer := endedSpan(t, sr, "eventbus.publish "+topic)
	if producer.Parent().IsValid() {
		t.Fatalf("producer span unexpectedly has parent %s", producer.Parent().SpanID())
	}
	if got, want := producerSC.TraceID(), producer.SpanContext().TraceID(); got != want {
		t.Fatalf("traceparent trace ID = %s, want producer trace ID %s", got, want)
	}
}

// 全局 TracerProvider 为 noop 时没有有效 span，InjectTrace 不应写入非法
// traceparent；业务 header 仍应透传。
func TestPublishWithoutTracerProviderWritesNoTraceparent(t *testing.T) {
	useGlobalProvider(t, noop.NewTracerProvider())
	useW3CPropagator(t)

	rt := &fakeRuntime{}
	bus := eventbus.NewBus(rt)

	headers := map[string]string{"event_type": "order.created"}
	if err := otelbus.Publish(context.Background(), bus, "orders.created", nil, headers); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(rt.published) != 1 {
		t.Fatalf("runtime received %d messages, want 1", len(rt.published))
	}
	if _, ok := rt.published[0].Headers["traceparent"]; ok {
		t.Fatalf("traceparent written without an active tracer: %v", rt.published[0].Headers)
	}
	if got := rt.published[0].Headers["event_type"]; got != "order.created" {
		t.Fatalf("business header = %q, want %q", got, "order.created")
	}
}

func TestPublishRecordsError(t *testing.T) {
	sr, tp := setupTracer(t)
	useGlobalProvider(t, tp)
	useW3CPropagator(t)

	boom := errors.New("redis unavailable")
	rt := &fakeRuntime{publishErr: boom}
	bus := eventbus.NewBus(rt)

	err := otelbus.Publish(context.Background(), bus, "orders.created", nil, map[string]string{"event_type": "order.created"})
	if !errors.Is(err, boom) {
		t.Fatalf("Publish error = %v, want %v", err, boom)
	}
	producer := endedSpan(t, sr, "eventbus.publish orders.created")
	if got := producer.Status().Code; got != codes.Error {
		t.Fatalf("producer span status = %v, want %v", got, codes.Error)
	}
	if got := producer.Status().Description; got != boom.Error() {
		t.Fatalf("producer span status description = %q, want %q", got, boom.Error())
	}
	if len(producer.Events()) == 0 {
		t.Fatal("expected the publish error to be recorded as a span event")
	}
	// traceparent 在调用 Runtime 之前注入，发布失败时 headers 仍然完整。
	if len(rt.published) != 1 {
		t.Fatalf("runtime received %d messages, want 1", len(rt.published))
	}
	spanContextFromTraceparent(t, rt.published[0].Headers)
}

func TestPublishNilBus(t *testing.T) {
	err := otelbus.Publish(context.Background(), nil, "orders.created", nil, nil)
	if !errors.Is(err, otelbus.ErrNilBus) {
		t.Fatalf("Publish(nil bus) error = %v, want %v", err, otelbus.ErrNilBus)
	}
}
