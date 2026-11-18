package otel

import (
	"context"
	"errors"

	eventbus "github.com/modern-magic-go/eventbus"
	otelapi "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ErrNilBus 是 Publish 收到 nil 总线时返回的错误。
var ErrNilBus = errors.New("eventbus/otel: nil bus")

// Publish 在一个 producer span 内调用 bus.Publish 发布消息。
//
// 行为：
//   - 用 OTel 全局 TracerProvider 创建 SpanKindProducer 类型的 span，名称默认
//     为 "eventbus.publish <topic>"，属性包含 messaging.system=redis 与
//     messaging.destination.name=<topic>；
//   - 复制 headers 并用全局 TextMapPropagator 注入当前 trace context
//     （通常写入 traceparent/tracestate），再以 eventbus.WithHeaders 合并进
//     Bus.Publish 的选项，调用方的业务 header 全部保留；
//   - 发布失败时把错误记录到 span（RecordError）并把 span status 置为
//     Error，错误原样返回。
//
// headers 参数优先于 opts 中已有的 eventbus.WithHeaders：合并后的 headers 在
// opts 之后应用。需要透传业务 header 时请通过 headers 参数传入。
//
// 与 WrapHandler 不同，Publish 不接受本包的 Option，始终使用 OTel 全局默认
// TracerProvider / TextMapPropagator；请在进程启动时通过
// otel.SetTracerProvider、otel.SetTextMapPropagator 统一配置。bus 为 nil 时
// 返回 ErrNilBus。
func Publish(ctx context.Context, bus *eventbus.Bus, topic string, payload any, headers map[string]string, opts ...eventbus.PublishOption) error {
	if bus == nil {
		return ErrNilBus
	}
	if ctx == nil {
		ctx = context.Background()
	}

	tracer := otelapi.GetTracerProvider().Tracer(tracerName)
	ctx, span := tracer.Start(ctx, defaultSpanName(OperationPublish, topic),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(producerAttributes(topic)...),
	)
	defer span.End()

	merged := InjectTrace(ctx, cloneHeaders(headers))
	publishOpts := make([]eventbus.PublishOption, 0, len(opts)+1)
	publishOpts = append(publishOpts, opts...)
	publishOpts = append(publishOpts, eventbus.WithHeaders(merged))

	err := bus.Publish(ctx, topic, payload, publishOpts...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func producerAttributes(topic string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("messaging.system", messagingSystem),
		attribute.String("messaging.destination.name", topic),
	}
}
