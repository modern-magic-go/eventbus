package core

import "context"

type Observer interface {
	OnPublished(ctx context.Context, msg Message)
	OnPublishFailed(ctx context.Context, msg Message, cause error)
	OnHandled(ctx context.Context, msg Message)
	OnHandleFailed(ctx context.Context, msg Message, cause error)
	OnRetryScheduled(ctx context.Context, task RetryTask, cause error)
	OnDeadLettered(ctx context.Context, msg DeadLetterMessage)
}

// ErrorEvent 描述总线内部错误（消费拉取、认领、Ack、重试/死信存储操作等），
// 用于把原本被静默吞掉的错误暴露给可观测性组件。
type ErrorEvent struct {
	// Op 是出错的操作名，例如 consume / reclaim / ack / schedule_retry /
	// remove_retry / move_dead_letter / dedup_acquire / dedup_release。
	Op        string
	Topic     string
	Group     string
	MessageID string
	Err       error
}

// ErrorObserver 是 Observer 的可选扩展：实现后总线会把内部错误回调给它。
type ErrorObserver interface {
	OnError(ctx context.Context, event ErrorEvent)
}

// DuplicateObserver 是 Observer 的可选扩展：幂等去重命中时回调。
type DuplicateObserver interface {
	OnDuplicate(ctx context.Context, msg Message)
}

type noopObserver struct{}

func (n noopObserver) OnPublished(context.Context, Message)               {}
func (n noopObserver) OnPublishFailed(context.Context, Message, error)    {}
func (n noopObserver) OnHandled(context.Context, Message)                 {}
func (n noopObserver) OnHandleFailed(context.Context, Message, error)     {}
func (n noopObserver) OnRetryScheduled(context.Context, RetryTask, error) {}
func (n noopObserver) OnDeadLettered(context.Context, DeadLetterMessage)  {}
