package eventbus

import (
	"context"
	"time"
)

// Runtime 定义事件总线运行时端口。
type Runtime interface {
	Publish(ctx context.Context, msg Message) error
	EnsureConsumerGroup(ctx context.Context, topic, group string) error
	Consume(ctx context.Context, cfg ConsumerConfig) ([]Message, error)
	Ack(ctx context.Context, msg Message) error

	ScheduleRetry(ctx context.Context, task RetryTask) error
	PollRetry(ctx context.Context, limit int, now time.Time) ([]RetryTask, error)
	RemoveRetry(ctx context.Context, task RetryTask) error
	MoveToDeadLetter(ctx context.Context, msg DeadLetterMessage) error

	Close() error
}

