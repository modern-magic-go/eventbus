package core

import "time"

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
