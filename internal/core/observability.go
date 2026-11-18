package core

import (
	"context"
	"log/slog"
)

// slogObserver 是基于标准库 log/slog 的 Observer 实现，零第三方依赖。
//
// 它同时实现 ErrorObserver 与 DuplicateObserver：生命周期事件按 Debug/Info
// 记录，失败与内部错误按 Warn/Error 记录。未注入 logger 时默认使用 slog.Default()。
type slogObserver struct {
	logger *slog.Logger
}

// NewSlogObserver 创建基于 slog 的 Observer；logger 为 nil 时使用 slog.Default()。
func NewSlogObserver(logger *slog.Logger) Observer {
	if logger == nil {
		logger = slog.Default()
	}
	return &slogObserver{logger: logger}
}

func (o *slogObserver) OnPublished(ctx context.Context, msg Message) {
	o.logger.LogAttrs(ctx, slog.LevelDebug, "eventbus published",
		slog.String("topic", msg.Topic), slog.String("message_id", msg.ID))
}

func (o *slogObserver) OnPublishFailed(ctx context.Context, msg Message, cause error) {
	o.logger.LogAttrs(ctx, slog.LevelError, "eventbus publish failed",
		slog.String("topic", msg.Topic), slog.String("message_id", msg.ID), slog.String("error", cause.Error()))
}

func (o *slogObserver) OnHandled(ctx context.Context, msg Message) {
	o.logger.LogAttrs(ctx, slog.LevelDebug, "eventbus handled",
		slog.String("topic", msg.Topic), slog.String("message_id", msg.ID))
}

func (o *slogObserver) OnHandleFailed(ctx context.Context, msg Message, cause error) {
	o.logger.LogAttrs(ctx, slog.LevelWarn, "eventbus handle failed",
		slog.String("topic", msg.Topic), slog.String("message_id", msg.ID), slog.String("error", cause.Error()))
}

func (o *slogObserver) OnRetryScheduled(ctx context.Context, task RetryTask, cause error) {
	attrs := []slog.Attr{
		slog.String("topic", task.Topic),
		slog.String("message_id", task.MessageID),
		slog.Int("attempt", task.Attempt),
	}
	if cause != nil {
		attrs = append(attrs, slog.String("error", cause.Error()))
	}
	o.logger.LogAttrs(ctx, slog.LevelWarn, "eventbus retry scheduled", attrs...)
}

func (o *slogObserver) OnDeadLettered(ctx context.Context, msg DeadLetterMessage) {
	o.logger.LogAttrs(ctx, slog.LevelError, "eventbus dead lettered",
		slog.String("topic", msg.Topic), slog.String("message_id", msg.MessageID), slog.String("error", msg.Error))
}

func (o *slogObserver) OnError(ctx context.Context, event ErrorEvent) {
	attrs := []slog.Attr{
		slog.String("op", event.Op),
		slog.String("topic", event.Topic),
		slog.String("group", event.Group),
		slog.String("message_id", event.MessageID),
	}
	if event.Err != nil {
		attrs = append(attrs, slog.String("error", event.Err.Error()))
	}
	o.logger.LogAttrs(ctx, slog.LevelError, "eventbus error", attrs...)
}

func (o *slogObserver) OnDuplicate(ctx context.Context, msg Message) {
	o.logger.LogAttrs(ctx, slog.LevelDebug, "eventbus duplicate skipped",
		slog.String("topic", msg.Topic), slog.String("message_id", msg.ID))
}
