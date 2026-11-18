package otel_test

import (
	"context"
	"testing"

	otelbus "github.com/modern-magic-go/eventbus/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestInjectTraceWithoutSpan(t *testing.T) {
	useGlobalPropagator(t, propagation.TraceContext{})

	headers := map[string]string{"event_type": "order.created"}
	got := otelbus.InjectTrace(context.Background(), headers)

	if got["event_type"] != "order.created" {
		t.Fatalf("business header lost: %v", got)
	}
	if _, ok := got["traceparent"]; ok {
		t.Fatalf("InjectTrace wrote traceparent without an active span: %v", got)
	}
	if len(got) != 1 {
		t.Fatalf("unexpected extra headers: %v", got)
	}

	empty := otelbus.InjectTrace(context.Background(), nil)
	if empty == nil {
		t.Fatal("InjectTrace(nil) should return a non-nil map")
	}
	if len(empty) != 0 {
		t.Fatalf("InjectTrace(nil) should stay empty without an active span: %v", empty)
	}
}

func TestInjectExtractRoundTrip(t *testing.T) {
	_, tp := setupTracer(t)
	useGlobalPropagator(t, propagation.TraceContext{})

	ctx, span := tp.Tracer("test").Start(context.Background(), "op")
	defer span.End()

	headers := otelbus.InjectTrace(ctx, nil)
	sc := spanContextFromTraceparent(t, headers)
	if got, want := sc.TraceID(), span.SpanContext().TraceID(); got != want {
		t.Fatalf("trace ID = %s, want %s", got, want)
	}
	if got, want := sc.SpanID(), span.SpanContext().SpanID(); got != want {
		t.Fatalf("span ID = %s, want %s", got, want)
	}

	restored := otelbus.ExtractTrace(context.Background(), headers)
	got := trace.SpanContextFromContext(restored)
	if !got.IsValid() {
		t.Fatal("ExtractTrace did not restore a valid span context")
	}
	if got.TraceID() != sc.TraceID() || got.SpanID() != sc.SpanID() {
		t.Fatalf("restored span context = %s/%s, want %s/%s",
			got.TraceID(), got.SpanID(), sc.TraceID(), sc.SpanID())
	}

	// 空 headers 不 panic，并原样返回 ctx。
	base := context.Background()
	if gotCtx := otelbus.ExtractTrace(base, nil); gotCtx != base {
		t.Fatal("ExtractTrace with empty headers should return ctx unchanged")
	}
}
