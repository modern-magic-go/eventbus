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

	// ErrInvalidStartFrom 表示 ConsumerConfig.StartFrom 取值非法。
	ErrInvalidStartFrom = errors.New("invalid consumer start offset")
	// ErrStartFromUnsupported 表示设置了非默认 StartFrom，但 Runtime 没有实现
	// ConsumerGroupInitializer 接口。
	ErrStartFromUnsupported = errors.New("runtime does not support custom start offset")
	// ErrDeadLetterStoreUnsupported 表示 Runtime 没有实现 DeadLetterStore 接口，
	// 无法查询 / 重放死信。
	ErrDeadLetterStoreUnsupported = errors.New("runtime does not support dead letter store")
	// ErrStopTimeout 表示优雅停机等待在途 handler 超时。
	ErrStopTimeout = errors.New("stop timeout: in-flight handlers did not finish in time")
)
