package otel_test

import (
	"context"
	"errors"
	"testing"

	eventbus "github.com/modern-magic-go/eventbus"
	otelbus "github.com/modern-magic-go/eventbus/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// 验证 producer → consumer 链路：WrapHandler 从 Publish 写入的 traceparent
// 恢复 context，consumer span 的 parent 是 producer span。
func TestWrapHandlerContinuesTrace(t *testing.T) {
	sr, tp := setupTracer(t)
	useGlobalProvider(t, tp)
	useW3CPropagator(t)

	rt := &fakeRuntime{}
	bus := eventbus.NewBus(rt)

	const topic = "orders.created"
	rootCtx, root := tp.Tracer("test").Start(context.Background(), "root")
	if err := otelbus.Publish(rootCtx, bus, topic, []byte("payload"),
		map[string]string{"event_type": "order.created"}, eventbus.WithMessageID("msg-1")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	root.End()
	if len(rt.published) != 1 {
		t.Fatalf("runtime received %d messages, want 1", len(rt.published))
	}
	producer := endedSpan(t, sr, "eventbus.publish "+topic)

	msg := rt.published[0]
	msg.ConsumerGroup = "workers"
	beforeHeaders := make(map[string]string, len(msg.Headers))
	for k, v := range msg.Headers {
		beforeHeaders[k] = v
	}

	called := false
	var handlerSC trace.SpanContext
	wrapped := otelbus.WrapHandler(func(ctx context.Context, got eventbus.Message) error {
		called = true
		span := trace.SpanFromContext(ctx)
		handlerSC = span.SpanContext()
		if !handlerSC.IsValid() {
			t.Error("handler context has no valid consumer span")
		}
		if got.Headers["event_type"] != "order.created" {
			t.Errorf("handler message header event_type = %q, want %q", got.Headers["event_type"], "order.created")
		}
		return nil
	})

	if err := wrapped(context.Background(), msg); err != nil {
		t.Fatalf("wrapped handler: %v", err)
	}
	if !called {
		t.Fatal("wrapped handler was not called")
	}

	consumer := endedSpan(t, sr, "eventbus.consume "+topic)
	if got := consumer.SpanKind(); got != trace.SpanKindConsumer {
		t.Fatalf("consumer span kind = %v, want %v", got, trace.SpanKindConsumer)
	}
	if got := consumer.InstrumentationScope().Name; got != instrumentationScope {
		t.Fatalf("instrumentation scope = %q, want %q", got, instrumentationScope)
	}

	producerSC := producer.SpanContext()
	gotParent := consumer.Parent()
	if gotParent.TraceID() != producerSC.TraceID() || gotParent.SpanID() != producerSC.SpanID() {
		t.Fatalf("consumer parent = %s/%s, want producer %s/%s",
			gotParent.TraceID(), gotParent.SpanID(), producerSC.TraceID(), producerSC.SpanID())
	}
	if handlerSC.SpanID() != consumer.SpanContext().SpanID() {
		t.Fatalf("handler span ID = %s, want consumer span ID %s",
			handlerSC.SpanID(), consumer.SpanContext().SpanID())
	}
	if handlerSC.TraceID() != producerSC.TraceID() {
		t.Fatalf("handler trace ID = %s, want producer trace ID %s", handlerSC.TraceID(), producerSC.TraceID())
	}

	if got, _ := stringAttribute(consumer, "messaging.system"); got != "redis" {
		t.Fatalf("messaging.system = %q, want %q", got, "redis")
	}
	if got, _ := stringAttribute(consumer, "messaging.destination.name"); got != topic {
		t.Fatalf("messaging.destination.name = %q, want %q", got, topic)
	}
	if got, _ := stringAttribute(consumer, "messaging.consumer.group.name"); got != "workers" {
		t.Fatalf("messaging.consumer.group.name = %q, want %q", got, "workers")
	}
	if got, _ := stringAttribute(consumer, "messaging.message.id"); got != "msg-1" {
		t.Fatalf("messaging.message.id = %q, want %q", got, "msg-1")
	}

	// WrapHandler 按值接收 msg，不应修改调用方的 headers。
	if len(msg.Headers) != len(beforeHeaders) {
		t.Fatalf("message headers changed: %v, want %v", msg.Headers, beforeHeaders)
	}
	for k, v := range beforeHeaders {
		if got := msg.Headers[k]; got != v {
			t.Errorf("message header %q = %q, want %q", k, got, v)
		}
	}
}

// headers 中没有 traceparent 时，consumer span 作为 root span 正常工作，
// 且不会向 msg.Headers 写入任何东西。
func TestWrapHandlerWithoutTraceContext(t *testing.T) {
	sr, tp := setupTracer(t)

	msg := eventbus.Message{
		ID:            "msg-1",
		Topic:         "orders.created",
		ConsumerGroup: "workers",
		Headers:       map[string]string{"event_type": "order.created"},
	}

	called := false
	wrapped := otelbus.WrapHandler(func(ctx context.Context, got eventbus.Message) error {
		called = true
		span := trace.SpanFromContext(ctx)
		if !span.SpanContext().IsValid() {
			t.Error("handler context has no valid consumer span")
		}
		return nil
	}, otelbus.WithTracerProvider(tp))

	if err := wrapped(context.Background(), msg); err != nil {
		t.Fatalf("wrapped handler: %v", err)
	}
	if !called {
		t.Fatal("wrapped handler was not called")
	}

	consumer := endedSpan(t, sr, "eventbus.consume orders.created")
	if consumer.Parent().IsValid() {
		t.Fatalf("consumer span should be a root span, got parent %s", consumer.Parent().SpanID())
	}
	if _, ok := msg.Headers["traceparent"]; ok {
		t.Fatal("WrapHandler must not mutate msg.Headers")
	}
	if len(msg.Headers) != 1 || msg.Headers["event_type"] != "order.created" {
		t.Fatalf("message headers changed: %v", msg.Headers)
	}
}

// 验证 WithTracerProvider / WithPropagator / WithSpanNameFormatter 生效：
// 自定义 formatter 决定 span 名；替换 propagator 后即使 headers 里有
// traceparent 也不会恢复成 parent。
func TestWrapHandlerOptions(t *testing.T) {
	sr, tp := setupTracer(t)

	ctx, producer := tp.Tracer("test").Start(context.Background(), "producer")
	defer producer.End()
	headers := map[string]string{}
	propagation.TraceContext{}.Inject(ctx, propagation.MapCarrier(headers))
	if _, ok := headers["traceparent"]; !ok {
		t.Fatal("test setup: no traceparent injected")
	}

	wrapped := otelbus.WrapHandler(
		func(context.Context, eventbus.Message) error { return nil },
		otelbus.WithTracerProvider(tp),
		otelbus.WithPropagator(dropPropagator{}),
		otelbus.WithSpanNameFormatter(func(operation, topic string) string {
			return "custom." + operation + "/" + topic
		}),
	)

	err := wrapped(context.Background(), eventbus.Message{
		ID:            "msg-1",
		Topic:         "orders.created",
		ConsumerGroup: "workers",
		Headers:       headers,
	})
	if err != nil {
		t.Fatalf("wrapped handler: %v", err)
	}

	consumer := endedSpan(t, sr, "custom.consume/orders.created")
	if consumer.Parent().IsValid() {
		t.Fatalf("WithPropagator override ignored: consumer span has parent %s", consumer.Parent().SpanID())
	}
}

func TestWrapHandlerNilHandler(t *testing.T) {
	handler := otelbus.WrapHandler(nil)
	if handler == nil {
		t.Fatal("WrapHandler(nil) returned a nil handler")
	}
	err := handler(context.Background(), eventbus.Message{ID: "msg-1", Topic: "orders.created"})
	if !errors.Is(err, otelbus.ErrNilHandler) {
		t.Fatalf("nil handler error = %v, want %v", err, otelbus.ErrNilHandler)
	}
}

func TestWrapHandlerRecordsError(t *testing.T) {
	sr, tp := setupTracer(t)

	boom := errors.New("handler failed")
	wrapped := otelbus.WrapHandler(func(context.Context, eventbus.Message) error {
		return boom
	}, otelbus.WithTracerProvider(tp))

	err := wrapped(context.Background(), eventbus.Message{
		ID:            "msg-1",
		Topic:         "orders.created",
		ConsumerGroup: "workers",
	})
	if !errors.Is(err, boom) {
		t.Fatalf("handler error = %v, want %v", err, boom)
	}

	consumer := endedSpan(t, sr, "eventbus.consume orders.created")
	if got := consumer.Status().Code; got != codes.Error {
		t.Fatalf("consumer span status = %v, want %v", got, codes.Error)
	}
	if got := consumer.Status().Description; got != boom.Error() {
		t.Fatalf("consumer span status description = %q, want %q", got, boom.Error())
	}
	if len(consumer.Events()) == 0 {
		t.Fatal("expected the handler error to be recorded as a span event")
	}
}
