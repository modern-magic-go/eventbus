package otel

import (
	"context"

	otelapi "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// InjectTrace 把 ctx 中的 trace context 注入 headers 并返回该 map。
//
// 注入格式由 OTel 全局 TextMapPropagator 决定：常见配置为 W3C TraceContext，
// 写入 traceparent/tracestate。OTel Go 在应用未调用
// otel.SetTextMapPropagator 时全局 propagator 是空实现，不会写入任何字段，
// 因此接入方需在进程启动时配置一次传播器。headers 为 nil 时会创建新 map；
// ctx 中没有有效 span 时同样不写入，且不会 panic。
//
// 传入的 map 会被直接写入并返回；需要保留调用方原始 map 时请先自行复制。
// 在 eventbus 中通常由 Publish 调用，返回值随后通过 eventbus.WithHeaders
// 交给 Bus.Publish。
func InjectTrace(ctx context.Context, headers map[string]string) map[string]string {
	if headers == nil {
		headers = make(map[string]string, 2)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	otelapi.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(headers))
	return headers
}

// ExtractTrace 从 headers 中恢复 trace context 并返回携带它的新 context。
//
// 使用 OTel 全局 TextMapPropagator（通常配置为 W3C TraceContext）。headers
// 为空或传播器为空实现时原样返回 ctx，不会 panic。恢复出的 SpanContext 标记
// 为 remote，可在其基础上创建 consumer span 形成跨进程链路。
func ExtractTrace(ctx context.Context, headers map[string]string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(headers) == 0 {
		return ctx
	}
	return otelapi.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(headers))
}

// cloneHeaders 复制 headers 并预留 OTel 传播字段的空间；nil 时返回空 map，
// 确保注入 traceparent 时不会向 nil map 写入。
func cloneHeaders(headers map[string]string) map[string]string {
	out := make(map[string]string, len(headers)+2)
	for k, v := range headers {
		out[k] = v
	}
	return out
}
