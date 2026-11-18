package otel

import (
	otelapi "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// tracerName 是创建 tracer 时使用的 instrumentation scope 名称。
const tracerName = "github.com/modern-magic-go/eventbus/otel"

// 生成 span 名称时的操作名，也是 SpanNameFormatter 的第一个参数。
const (
	// OperationPublish 对应 Publish 创建的 producer span。
	OperationPublish = "publish"
	// OperationConsume 对应 WrapHandler 创建的 consumer span。
	OperationConsume = "consume"
)

// messagingSystem 是写入 messaging.system 属性的消息系统标识。
const messagingSystem = "redis"

// SpanNameFormatter 生成 span 名称。operation 为 OperationPublish 或
// OperationConsume，topic 为消息主题。默认格式为
// "eventbus.<operation> <topic>"。
type SpanNameFormatter func(operation, topic string) string

func defaultSpanName(operation, topic string) string {
	return "eventbus." + operation + " " + topic
}

// config 是 WrapHandler 的一次性配置，在包装 handler 时固定下来。
type config struct {
	tracerProvider trace.TracerProvider
	propagator     propagation.TextMapPropagator
	spanName       SpanNameFormatter
}

// newConfig 返回以 OTel 全局默认值填充、再应用 opts 后的配置。
func newConfig(opts ...Option) config {
	cfg := config{
		tracerProvider: otelapi.GetTracerProvider(),
		propagator:     otelapi.GetTextMapPropagator(),
		spanName:       defaultSpanName,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// Option 配置 WrapHandler 返回的 handler。Option 只在包装时生效一次，
// 之后修改 OTel 全局配置不会影响已经包装好的 handler。
type Option func(*config)

// WithTracerProvider 指定创建 consumer span 使用的 TracerProvider。
// tp 为 nil 时忽略该选项，保留全局默认值。
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(cfg *config) {
		if tp != nil {
			cfg.tracerProvider = tp
		}
	}
}

// WithPropagator 指定从 msg.Headers 恢复 trace context 使用的
// TextMapPropagator（应与发布端使用的传播格式一致，通常为 W3C TraceContext）。
// 未设置时使用 OTel 全局 propagator；OTel Go 在应用未显式设置全局 propagator
// 时为空实现，因此生产使用前应在启动时调用 otel.SetTextMapPropagator。
// p 为 nil 时忽略该选项，保留全局默认值。
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(cfg *config) {
		if p != nil {
			cfg.propagator = p
		}
	}
}

// WithSpanNameFormatter 自定义 consumer span 名称。fn 为 nil 时忽略该选项，
// 保留默认的 "eventbus.consume <topic>" 格式。
func WithSpanNameFormatter(fn SpanNameFormatter) Option {
	return func(cfg *config) {
		if fn != nil {
			cfg.spanName = fn
		}
	}
}
