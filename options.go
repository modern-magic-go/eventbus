package eventbus

import "time"

// Option 定义 Bus 构造选项。
type Option func(*Bus)

// WithCodec 注入自定义编解码器。
func WithCodec(codec Codec) Option {
	return func(b *Bus) {
		if codec != nil {
			b.codec = codec
		}
	}
}

// WithObserver 注入可选观察者。
func WithObserver(observer Observer) Option {
	return func(b *Bus) {
		if observer != nil {
			b.observer = observer
		}
	}
}

// WithRetryPollInterval 设置重试轮询间隔。
func WithRetryPollInterval(interval time.Duration) Option {
	return func(b *Bus) {
		if interval > 0 {
			b.config.RetryPollInterval = interval
		}
	}
}

