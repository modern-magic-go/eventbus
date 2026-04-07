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

type noopObserver struct{}

func (n noopObserver) OnPublished(context.Context, Message)               {}
func (n noopObserver) OnPublishFailed(context.Context, Message, error)    {}
func (n noopObserver) OnHandled(context.Context, Message)                 {}
func (n noopObserver) OnHandleFailed(context.Context, Message, error)     {}
func (n noopObserver) OnRetryScheduled(context.Context, RetryTask, error) {}
func (n noopObserver) OnDeadLettered(context.Context, DeadLetterMessage)  {}
