package core

import (
	"context"
	"time"
)

const (
	DefaultMaxRetries     = 3
	DefaultPollBatchSize  = 10
	DefaultPollBlock      = 2 * time.Second
	DefaultRetryBaseDelay = 5 * time.Second

	// DefaultReclaimMinIdle 是 pending 消息被认领前的最小空闲时间。
	// 必须大于一批消息的处理总耗时（约 PollBatchSize × 单条 handler 最大耗时），
	// 否则可能被同一实例的 reclaim goroutine 并发重复处理。
	DefaultReclaimMinIdle = 30 * time.Second
	// DefaultReclaimInterval 是 pending 消息回收的默认扫描间隔。
	DefaultReclaimInterval = 10 * time.Second
)

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

type Handler func(ctx context.Context, msg Message) error

type ConsumerConfig struct {
	Topic          string
	Group          string
	ConsumerName   string
	Handler        Handler
	MaxRetries     int
	PollBatchSize  int
	PollBlock      time.Duration
	RetryBaseDelay time.Duration

	// PendingReclaim 配置 pending 消息回收（at-least-once 补偿）。
	// 零值表示关闭；任一字段非零即开启。开启后需要 Runtime 实现 PendingReclaimer，
	// 否则 Subscribe 返回 ErrPendingReclaimUnsupported。
	PendingReclaim PendingReclaimConfig
}

// PendingReclaimConfig 控制消费组 PEL 中长时间未 Ack 消息的回收。
//
// 启用后总线会周期性调用 Runtime.ClaimPending（Redis 实现为 XAUTOCLAIM），
// 把空闲超过 MinIdle 的消息重新认领给当前消费者，并走与正常消费一致的处理路径
// （同样受 MaxRetries / 死信约束）。
type PendingReclaimConfig struct {
	// MinIdle 是消息被认领前的最小空闲时间，默认 DefaultReclaimMinIdle。
	//
	// 注意：XREADGROUP 会一次预取同一批消息，idle 从投递时刻开始计算。为避免
	// 「本实例还没轮到处理」的消息被 reclaim 并发重复处理，MinIdle 应大于
	// 一批消息的处理总耗时（约 PollBatchSize × 单条 handler 最大耗时）；
	// 否则应把 PollBatchSize 调小（例如 1）。跨实例场景同样依赖该约束。
	MinIdle time.Duration
	// Batch 是单次认领的最大消息数，默认 DefaultPollBatchSize。
	Batch int
	// Interval 是回收扫描间隔，默认 DefaultReclaimInterval。
	Interval time.Duration
}

// enabled 报告是否启用了 pending 消息回收。
func (c PendingReclaimConfig) enabled() bool {
	return c.MinIdle > 0 || c.Batch > 0 || c.Interval > 0
}

type RetryTask struct {
	MessageID   string            `json:"message_id,omitempty"`
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

type DeadLetterMessage struct {
	MessageID  string            `json:"message_id,omitempty"`
	Topic      string            `json:"topic"`
	Group      string            `json:"group"`
	Payload    []byte            `json:"payload"`
	Headers    map[string]string `json:"headers,omitempty"`
	Attempt    int               `json:"attempt"`
	MaxRetries int               `json:"max_retries"`
	Error      string            `json:"error"`
	OccurredAt time.Time         `json:"occurred_at"`
}

type PublishOptions struct {
	MessageID string
	Headers   map[string]string
}

type PublishOption func(*PublishOptions)

func WithMessageID(id string) PublishOption {
	return func(o *PublishOptions) {
		o.MessageID = id
	}
}

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
	if cfg.PendingReclaim.enabled() {
		if cfg.PendingReclaim.MinIdle <= 0 {
			cfg.PendingReclaim.MinIdle = DefaultReclaimMinIdle
		}
		if cfg.PendingReclaim.Batch <= 0 {
			cfg.PendingReclaim.Batch = DefaultPollBatchSize
		}
		if cfg.PendingReclaim.Interval <= 0 {
			cfg.PendingReclaim.Interval = DefaultReclaimInterval
		}
	}
	return cfg
}
