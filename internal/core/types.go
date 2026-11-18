package core

import (
	"context"
	"fmt"
	"strings"
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
	// DefaultRetryMaxDelay 是指数退避的默认上限。
	DefaultRetryMaxDelay = 5 * time.Minute
	// DefaultDedupTTL 是幂等去重的默认窗口。
	DefaultDedupTTL = 24 * time.Hour
)

// 消费组新建时的初始 offset 取值。
const (
	// StartFromBeginning 从头开始消费（默认，兼容现有行为）。
	StartFromBeginning = "0"
	// StartFromLatest 只消费订阅之后的新消息。
	StartFromLatest = "$"
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
	// RetryMaxDelay 是指数退避的上限，默认 DefaultRetryMaxDelay。
	RetryMaxDelay time.Duration
	// HandlerTimeout 是单次 handler 调用的超时时间；0 表示不限制（默认）。
	// handler 必须尊重 ctx 才能在超时后真正中断。
	HandlerTimeout time.Duration
	// StartFrom 是新建消费组时的初始 offset：""/"0"（从头，默认）、"$"（最新）
	// 或 unix 毫秒时间戳（如 "1700000000000"）。已存在的消费组不受影响。
	// 设置非默认值时要求 Runtime 实现 ConsumerGroupInitializer。
	StartFrom string

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
	// Key 是可选的重试任务 member 覆盖值，用于「同一条消息需要独立重试任务」的场景
	// （例如死信重放），避免与正在清理的旧任务使用同一 member 而互相覆盖。
	Key         string            `json:"key,omitempty"`
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
	// ID 是死信记录自身的唯一标识，由 Runtime 在写入时生成（用于查询与重放）。
	ID         string            `json:"id,omitempty"`
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

// DedupConfig 配置消费端幂等去重。
type DedupConfig struct {
	// TTL 是去重键的保留时间，默认 DefaultDedupTTL。
	TTL time.Duration
	// KeyFunc 由消息计算去重键；默认 topic|group|messageID。
	KeyFunc func(Message) string
}

// DedupStatus 是去重占位（Acquire）的结果。
type DedupStatus int

const (
	// DedupAcquired 表示首次占位成功，当前处理者负责执行 handler 并 MarkDone / Release。
	DedupAcquired DedupStatus = iota
	// DedupDone 表示该消息在此前已成功处理过（去重窗口内），可以安全跳过。
	DedupDone
	// DedupInProgress 表示占位已存在但尚未完成（其他实例处理中，或上次处理崩溃/释放失败残留）。
	// 为保证 at-least-once 不丢消息，总线会继续执行 handler（可能重复处理）。
	DedupInProgress
)

func (s DedupStatus) String() string {
	switch s {
	case DedupAcquired:
		return "acquired"
	case DedupDone:
		return "done"
	case DedupInProgress:
		return "in_progress"
	default:
		return "unknown"
	}
}

// DedupStore 是消费端幂等去重的存储抽象（两阶段：占位 → 完成）。
//
// Acquire 尝试占位并返回状态：
//   - DedupAcquired：首次占位成功；
//   - DedupDone：此前已成功处理（去重命中，可跳过）；
//   - DedupInProgress：占位存在但未完成（可能处理中/崩溃残留，调用方应继续处理以免丢消息）。
//
// MarkDone 在 handler 成功后把占位标记为已完成并保留 TTL；
// Release 在 handler 失败后释放「未完成」占位，供重试继续处理（应使用不随停机取消的 context 调用）。
// Release 不得删除已完成的标记，否则并发场景下会把已成功处理的消息重新变为可处理。
type DedupStore interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (DedupStatus, error)
	MarkDone(ctx context.Context, key string, ttl time.Duration) error
	Release(ctx context.Context, key string) error
}

// ReplayResult 是死信重放结果。
type ReplayResult struct {
	// Replayed 是成功重新入队（进入重试队列）的数量。
	Replayed int
	// Skipped 是找不到对应订阅、被放回死信队列的数量。
	Skipped int
	// Errors 是逐条重放失败的错误集合。
	Errors []error
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
	if cfg.RetryMaxDelay <= 0 {
		cfg.RetryMaxDelay = DefaultRetryMaxDelay
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

// normalizeStartFrom 归一化并校验 ConsumerConfig.StartFrom。
//
// 支持："0"（从头，默认）、"$"（最新）、unix 毫秒时间戳（纯数字，自动补 "-0"）、
// 以及 Redis stream ID（"<ms>-<seq>"）。
func normalizeStartFrom(start string) (string, error) {
	s := strings.TrimSpace(start)
	switch s {
	case "", StartFromBeginning:
		return StartFromBeginning, nil
	case StartFromLatest:
		return StartFromLatest, nil
	}
	if isDigits(s) {
		if len(s) < 10 {
			return "", fmt.Errorf("start offset %q is too short for a unix millisecond timestamp", s)
		}
		return s + "-0", nil
	}
	if i := strings.IndexByte(s, '-'); i > 0 && isDigits(s[:i]) && isDigits(s[i+1:]) {
		return s, nil
	}
	return "", fmt.Errorf("start offset %q must be \"0\", \"$\", a unix millisecond timestamp or a stream id", s)
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
