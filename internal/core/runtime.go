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

	// ScheduleRetry 写入 / 覆盖一条重试任务。
	// 实现契约：MessageID 非空时必须按 (Group, MessageID) 幂等覆盖已有任务，
	// 而不是追加重复任务；MessageID 为空时保持旧语义（追加）。
	ScheduleRetry(ctx context.Context, task RetryTask) error
	// PollRetry 返回已到期的重试任务，不应删除未到期任务。
	PollRetry(ctx context.Context, limit int, now time.Time) ([]RetryTask, error)
	// RemoveRetry 删除一条重试任务（同时清理其 payload 存储）。
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

// ConsumerGroupInitializer 是 Runtime 的可选能力：按指定 offset 创建消费组。
//
// 设置 ConsumerConfig.StartFrom（"$" 或 unix 毫秒时间戳）时，Bus 通过类型断言
// 检查该接口；未实现时 Start 返回 ErrStartFromUnsupported。
type ConsumerGroupInitializer interface {
	EnsureConsumerGroupFrom(ctx context.Context, topic, group, start string) error
}

// DeadLetterStore 是 Runtime 的可选能力：查询、弹出入队死信，供总线提供
// DeadLetters（只读查询）与 ReplayDeadLetters（重放）能力。
//
// 实现契约：PopDeadLetters 返回后应已从存储中移除这些记录，调用方会对找不到
// 订阅的记录重新写回（MoveToDeadLetter），因此实现需保证 ID 稳定或可重新生成。
type DeadLetterStore interface {
	ListDeadLetters(ctx context.Context, offset, limit int) ([]DeadLetterMessage, error)
	PopDeadLetters(ctx context.Context, limit int) ([]DeadLetterMessage, error)
	DeadLetterLen(ctx context.Context) (int64, error)
}

// RuntimeStats 是 Runtime 的可选能力：暴露队列/消费组指标，供可观测性组件
// （如内置的 Prometheus collector）采集。
type RuntimeStats interface {
	// StreamLag 返回消费组的待消费消息数（last-delivered-id 之后的条目数）；
	// Redis 版本不支持 lag 时返回 -1。
	StreamLag(ctx context.Context, topic, group string) (int64, error)
	// PendingCount 返回消费组 PEL 中的消息数。
	PendingCount(ctx context.Context, topic, group string) (int64, error)
	// RetryQueueLen 返回重试队列中的任务数。
	RetryQueueLen(ctx context.Context) (int64, error)
}
