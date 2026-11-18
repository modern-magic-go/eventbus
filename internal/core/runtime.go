package core

import (
	"context"
	"time"
)

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

// PendingReclaimer 是 Runtime 的可选能力：认领消费组 PEL 中空闲超过 MinIdle 的消息。
//
// 启用 ConsumerConfig.PendingReclaim 时，Bus 通过类型断言检查 Runtime 是否实现该接口。
// RedisRuntime 基于 XAUTOCLAIM 实现（需要 Redis 6.2+）。
//
// 实现契约：返回的 Message 必须带上 Ack 所需信息（DeliveryTag 与 ConsumerGroup），
// 并应已把消息所有权转移到当前消费者（例如 XAUTOCLAIM 的语义），否则失败重试后
// 原消息不会被 XACK、仍会留在 PEL 中。
type PendingReclaimer interface {
	ClaimPending(ctx context.Context, cfg ConsumerConfig) ([]Message, error)
}
