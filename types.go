package eventbus

import (
	"context"
	"time"
)

const (
	// DefaultMaxRetries 是默认重试次数。
	DefaultMaxRetries = 3
	// DefaultPollBatchSize 是默认单次拉取数量。
	DefaultPollBatchSize = 10
	// DefaultPollBlock 是默认消费阻塞时长。
	DefaultPollBlock = 2 * time.Second
	// DefaultRetryBaseDelay 是默认重试基准延迟。
	DefaultRetryBaseDelay = 5 * time.Second
)

// Message 是 eventbus 的标准消息模型。
type Message struct {
	ID            string
	Topic         string
	Payload       []byte
	Headers       map[string]string
	PublishedAt   time.Time
	ConsumerGroup string
	Attempt       int
	MaxRetries    int
	DeliveryTag   string
}

// Handler 是订阅方处理函数。
type Handler func(ctx context.Context, msg Message) error

// ConsumerConfig 描述一个订阅消费配置。
type ConsumerConfig struct {
	Topic          string
	Group          string
	ConsumerName   string
	Handler        Handler
	MaxRetries     int
	PollBatchSize  int
	PollBlock      time.Duration
	RetryBaseDelay time.Duration
}

// RetryTask 表示失败后待重试的任务。
type RetryTask struct {
	Topic       string            `json:"topic"`
	Group       string            `json:"group"`
	Payload     []byte            `json:"payload"`
	Headers     map[string]string `json:"headers,omitempty"`
	Attempt     int               `json:"attempt"`
	MaxRetries  int               `json:"max_retries"`
	AvailableAt time.Time         `json:"available_at"`
	LastError   string            `json:"last_error,omitempty"`

	rawMember string
}

// DeadLetterMessage 表示死信消息。
type DeadLetterMessage struct {
	Topic      string            `json:"topic"`
	Group      string            `json:"group"`
	Payload    []byte            `json:"payload"`
	Headers    map[string]string `json:"headers,omitempty"`
	Attempt    int               `json:"attempt"`
	MaxRetries int               `json:"max_retries"`
	Error      string            `json:"error"`
	OccurredAt time.Time         `json:"occurred_at"`
}

// PublishOptions 描述发布时附加参数。
type PublishOptions struct {
	MessageID string
	Headers   map[string]string
}

// PublishOption 应用于 PublishOptions。
type PublishOption func(*PublishOptions)

// WithMessageID 设置消息 ID。
func WithMessageID(id string) PublishOption {
	return func(o *PublishOptions) {
		o.MessageID = id
	}
}

// WithHeaders 设置消息 headers。
func WithHeaders(headers map[string]string) PublishOption {
	return func(o *PublishOptions) {
		o.Headers = headers
	}
}

func normalizeConsumerConfig(cfg ConsumerConfig) ConsumerConfig {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = DefaultMaxRetries
	}
	if cfg.PollBatchSize <= 0 {
		cfg.PollBatchSize = DefaultPollBatchSize
	}
	if cfg.PollBlock <= 0 {
		cfg.PollBlock = DefaultPollBlock
	}
	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = DefaultRetryBaseDelay
	}
	return cfg
}
