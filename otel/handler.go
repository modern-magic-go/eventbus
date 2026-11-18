package otel

import (
	"context"
	"errors"
	"fmt"

	eventbus "github.com/modern-magic-go/eventbus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ErrNilHandler 是 WrapHandler 收到 nil handler 时返回的 handler 产生的错误。
var ErrNilHandler = errors.New("eventbus/otel: nil handler")

// WrapHandler 返回一个带 consumer span 的 eventbus.Handler。
//
// 包装时固定配置（Option），之后每次调用该 handler 都会：
//  1. 用配置的 TextMapPropagator 从 msg.Headers 恢复上游 trace context
//     （通常由 Publish 注入的 traceparent/tracestate）；
//  2. 以 SpanKindConsumer 类型创建 consumer span，名称默认
//     "eventbus.consume <topic>"，属性包含 messaging.system=redis、
//     messaging.destination.name=<msg.Topic>、
//     messaging.consumer.group.name=<msg.ConsumerGroup>、
//     messaging.message.id=<msg.ID>；
//  3. 用携带 trace context 的 ctx 调用 next；
//  4. next 返回错误时把错误记录到 span（RecordError）并把 span status 置为
//     Error，最后结束 span 并原样返回错误。
//
// msg 按值传递，WrapHandler 不会修改 msg 本身或其 Headers。headers 中没有
// traceparent（例如消息来自未接入 OTel 的上游）时会创建新的 root consumer
// span，不会 panic。next 为 nil 时返回一个只返回 ErrNilHandler 的 handler，
// 不会 panic。
func WrapHandler(next eventbus.Handler, opts ...Option) eventbus.Handler {
	if next == nil {
		return func(context.Context, eventbus.Message) error {
			return ErrNilHandler
		}
	}

	cfg := newConfig(opts...)
	return func(ctx context.Context, msg eventbus.Message) error {
		if ctx == nil {
			ctx = context.Background()
		}
		ctx = cfg.propagator.Extract(ctx, propagation.MapCarrier(msg.Headers))
		ctx, span := cfg.tracerProvider.Tracer(tracerName).Start(
			ctx,
			cfg.spanName(OperationConsume, msg.Topic),
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(consumerAttributes(msg)...),
		)
		defer span.End()
		defer func() {
			// handler panic 也必须在 span 上可见，然后原样抛给 Bus 的 recover 兜底。
			if r := recover(); r != nil {
				span.RecordError(fmt.Errorf("eventbus/otel: handler panic: %v", r))
				span.SetStatus(codes.Error, "handler panic")
				panic(r)
			}
		}()

		err := next(ctx, msg)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	}
}

func consumerAttributes(msg eventbus.Message) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("messaging.system", messagingSystem),
		attribute.String("messaging.destination.name", msg.Topic),
		attribute.String("messaging.consumer.group.name", msg.ConsumerGroup),
		attribute.String("messaging.message.id", msg.ID),
	}
}
