package core

import (
	"log/slog"
	"time"
)

type Option func(*Bus)

func WithCodec(codec Codec) Option {
	return func(b *Bus) {
		if codec != nil {
			b.codec = codec
		}
	}
}

func WithObserver(observer Observer) Option {
	return func(b *Bus) {
		if observer != nil {
			b.observer = observer
		}
	}
}

func WithRetryPollInterval(interval time.Duration) Option {
	return func(b *Bus) {
		if interval > 0 {
			b.config.RetryPollInterval = interval
		}
	}
}

func WithCloseRuntime(close bool) Option {
	return func(b *Bus) {
		b.config.CloseRuntime = close
	}
}

// WithLogger 注入 slog logger，用于输出总线内部错误（消费拉取、认领、Ack、
// 重试/死信存储操作等）。未注入时不输出日志，错误可通过 ErrorObserver 观测。
func WithLogger(logger *slog.Logger) Option {
	return func(b *Bus) {
		if logger != nil {
			b.logger = logger
		}
	}
}

// WithDedup 开启消费端幂等去重。store 为 nil 时忽略。
//
// 去重键默认由 topic|group|messageID 组成，TTL 默认 DefaultDedupTTL。
// handler 成功后去重键保留 TTL；handler 失败会释放占位，允许重试继续处理。
func WithDedup(store DedupStore, cfg DedupConfig) Option {
	return func(b *Bus) {
		if store != nil {
			b.dedupStore = store
			b.dedupConfig = cfg
		}
	}
}
