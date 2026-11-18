package otel_test

import (
	"context"
	"strings"
	"testing"

	eventbus "github.com/modern-magic-go/eventbus"
	otelapi "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const instrumentationScope = "github.com/modern-magic-go/eventbus/otel"

// fakeRuntime 只实现测试需要的 Runtime.Publish；其余 Runtime 方法由嵌入的
// nil 接口占位，测试中不会调用。这样即使 eventbus.Runtime 后续新增方法，
// 该 fake 也不需要跟着改。
type fakeRuntime struct {
	eventbus.Runtime
	published  []eventbus.Message
	publishErr error
}

func (f *fakeRuntime) Publish(_ context.Context, msg eventbus.Message) error {
	f.published = append(f.published, msg)
	return f.publishErr
}

// dropPropagator 用于验证 WithPropagator 生效：不注入也不恢复任何字段。
type dropPropagator struct{}

func (dropPropagator) Inject(context.Context, propagation.TextMapCarrier) {}

func (dropPropagator) Extract(ctx context.Context, _ propagation.TextMapCarrier) context.Context {
	return ctx
}

func (dropPropagator) Fields() []string { return nil }

func setupTracer(t *testing.T) (*tracetest.SpanRecorder, *sdktrace.TracerProvider) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown tracer provider: %v", err)
		}
	})
	return sr, tp
}

// useGlobalProvider 把 OTel 全局 TracerProvider 切到 tp；测试结束后重置为
// noop，避免测试之间通过全局状态互相影响（Publish 只认全局配置）。
func useGlobalProvider(t *testing.T, tp trace.TracerProvider) {
	t.Helper()
	otelapi.SetTracerProvider(tp)
	t.Cleanup(func() {
		otelapi.SetTracerProvider(noop.NewTracerProvider())
	})
}

// useGlobalPropagator 设置全局 TextMapPropagator；测试结束后恢复 OTel 默认的
// TraceContext + Baggage 组合。
func useGlobalPropagator(t *testing.T, p propagation.TextMapPropagator) {
	t.Helper()
	otelapi.SetTextMapPropagator(p)
	t.Cleanup(func() {
		otelapi.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
	})
}

// useW3CPropagator 把全局传播器配置为 W3C TraceContext + Baggage，模拟应用
// 启动时的标准配置。OTel Go 的全局 propagator 默认是空实现，不配置时不会
// 注入 traceparent。
func useW3CPropagator(t *testing.T) {
	t.Helper()
	useGlobalPropagator(t, propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
}

// endedSpan 返回第一个已结束且名称为 name 的 span，找不到时直接失败。
func endedSpan(t *testing.T, sr *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, span := range sr.Ended() {
		if span.Name() == name {
			return span
		}
	}
	ended := make([]string, 0, len(sr.Ended()))
	for _, span := range sr.Ended() {
		ended = append(ended, span.Name())
	}
	t.Fatalf("ended span %q not found; ended spans: %v", name, ended)
	return nil
}

func stringAttribute(span sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsString(), true
		}
	}
	return "", false
}

// spanContextFromTraceparent 校验 headers 中的 traceparent 是合法 W3C 格式，
// 并返回解析出的 SpanContext。
func spanContextFromTraceparent(t *testing.T, headers map[string]string) trace.SpanContext {
	t.Helper()
	raw, ok := headers["traceparent"]
	if !ok {
		t.Fatalf("headers missing traceparent: %v", headers)
	}
	if len(raw) != 55 || !strings.HasPrefix(raw, "00-") {
		t.Fatalf("invalid W3C traceparent: %q", raw)
	}
	ctx := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier(headers))
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		t.Fatalf("traceparent %q extracted to invalid span context", raw)
	}
	return sc
}
