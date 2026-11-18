package core

import "errors"

var (
	ErrInvalidConsumerConfig = errors.New("invalid consumer config")
	ErrDuplicateSubscription = errors.New("duplicate subscription")

	// ErrHandlerPanic 表示 handler 内部发生了 panic；该 panic 已被总线兜底捕获，
	// 并按普通处理失败进入重试 / 死信流程。
	ErrHandlerPanic = errors.New("handler panic")

	// ErrPendingReclaimUnsupported 表示启用了 ConsumerConfig.PendingReclaim，
	// 但注入的 Runtime 没有实现 PendingReclaimer 接口。
	ErrPendingReclaimUnsupported = errors.New("runtime does not support pending reclaim")
)
