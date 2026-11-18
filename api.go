package eventbus

import (
	"time"

	"github.com/modern-magic-go/eventbus/internal/core"
)

const (
	DefaultMaxRetries     = core.DefaultMaxRetries
	DefaultPollBatchSize  = core.DefaultPollBatchSize
	DefaultPollBlock      = core.DefaultPollBlock
	DefaultRetryBaseDelay = core.DefaultRetryBaseDelay

	DefaultReclaimMinIdle  = core.DefaultReclaimMinIdle
	DefaultReclaimInterval = core.DefaultReclaimInterval
)

var (
	ErrInvalidConsumerConfig     = core.ErrInvalidConsumerConfig
	ErrDuplicateSubscription     = core.ErrDuplicateSubscription
	ErrHandlerPanic              = core.ErrHandlerPanic
	ErrPendingReclaimUnsupported = core.ErrPendingReclaimUnsupported
)

type Config = core.Config
type Bus = core.Bus
type Codec = core.Codec
type Runtime = core.Runtime
type PendingReclaimer = core.PendingReclaimer
type Observer = core.Observer
type Option = core.Option
type Message = core.Message
type Handler = core.Handler
type ConsumerConfig = core.ConsumerConfig
type PendingReclaimConfig = core.PendingReclaimConfig
type RetryTask = core.RetryTask
type DeadLetterMessage = core.DeadLetterMessage
type PublishOptions = core.PublishOptions
type PublishOption = core.PublishOption

func NewBus(rt Runtime, opts ...Option) *Bus {
	return core.NewBus(rt, opts...)
}

func WithCodec(codec Codec) Option {
	return core.WithCodec(codec)
}

func WithObserver(observer Observer) Option {
	return core.WithObserver(observer)
}

func WithRetryPollInterval(interval time.Duration) Option {
	return core.WithRetryPollInterval(interval)
}

func WithCloseRuntime(close bool) Option {
	return core.WithCloseRuntime(close)
}

func WithMessageID(id string) PublishOption {
	return core.WithMessageID(id)
}

func WithHeaders(headers map[string]string) PublishOption {
	return core.WithHeaders(headers)
}
