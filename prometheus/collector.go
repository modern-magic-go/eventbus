package prometheus

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/modern-magic-go/eventbus"
)

const (
	// defaultCollectTimeout 是单次 Collect 调用 RuntimeStats 的总超时。
	defaultCollectTimeout = 5 * time.Second

	// UnavailableValue 是统计能力不可用时 gauge 输出的占位值。
	// 使用 -1 而不是省略指标，便于 dashboard/告警区分「值为 0」与「采不到」。
	UnavailableValue = -1.0
)

// Subscription 描述一个需要采集运行态指标的消费订阅。
//
// Topic 与 Group 必须与 Bus.Subscribe 使用的 (topic, group) 完全一致，
// 否则采集到的是不存在或已废弃消费组的数据（通常表现为 0 或查询报错）。
type Subscription struct {
	Topic string
	Group string
}

// deadLetterLener 是 Runtime 提供死信队列长度的可选能力，
// 与 eventbus.DeadLetterStore.DeadLetterLen 方法签名一致。
// 单独声明最小接口，避免要求 Runtime 实现完整的 DeadLetterStore。
type deadLetterLener interface {
	DeadLetterLen(ctx context.Context) (int64, error)
}

// Collector 实现 prometheus.Collector，从 Runtime 的可选能力
// eventbus.RuntimeStats 采集运行态 gauge。
//
// 每次 Collect 都用带超时的 context 调用 stats（默认 5s，可用
// WithCollectTimeout 调整）。单个调用失败时跳过对应指标并把
// eventbus_collect_errors_total{op} 加一，不会 panic，也不影响其余指标。
// Runtime 未实现统计能力时所有统计指标输出 UnavailableValue（-1）。
type Collector struct {
	rt            eventbus.Runtime
	subscriptions []Subscription
	timeout       time.Duration

	streamLag     *prometheus.Desc
	pending       *prometheus.Desc
	retryQueue    *prometheus.Desc
	deadLetter    *prometheus.Desc
	collectErrors *prometheus.CounterVec
}

var _ prometheus.Collector = (*Collector)(nil)

type collectorOptions struct {
	timeout time.Duration
}

// CollectorOption 配置 Collector 的创建。
type CollectorOption func(*collectorOptions)

// WithCollectTimeout 设置单次 Collect 调用 RuntimeStats 的总超时。
// timeout <= 0 时使用默认值 5s。
func WithCollectTimeout(timeout time.Duration) CollectorOption {
	return func(o *collectorOptions) {
		o.timeout = timeout
	}
}

// NewCollector 创建 Runtime 指标 Collector。
//
// rt 通常就是传给 eventbus.NewBus 的 Runtime。Collect 时通过类型断言检查
// rt 是否实现 eventbus.RuntimeStats；未实现（或 rt 为 nil）时指标输出 -1，
// 并记录 eventbus_collect_errors_total{op="runtime_stats"}。
//
// 死信长度额外通过可选方法 DeadLetterLen(context.Context) (int64, error) 获取；
// 未实现该方法时 eventbus_dead_letter_length 输出 -1，但不计为采集错误。
//
// subscriptions 会被复制，调用方后续修改切片不会影响 Collector；
// 列表内请不要包含重复的 (topic, group)，否则会输出重复指标导致抓取失败。
func NewCollector(rt eventbus.Runtime, subscriptions []Subscription, opts ...CollectorOption) *Collector {
	cfg := collectorOptions{timeout: defaultCollectTimeout}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.timeout <= 0 {
		cfg.timeout = defaultCollectTimeout
	}

	subs := make([]Subscription, len(subscriptions))
	copy(subs, subscriptions)

	return &Collector{
		rt:            rt,
		subscriptions: subs,
		timeout:       cfg.timeout,
		streamLag: prometheus.NewDesc(
			"eventbus_stream_lag",
			"消费组待消费消息数（last-delivered-id 之后的条目数）。",
			[]string{"topic", "group"}, nil,
		),
		pending: prometheus.NewDesc(
			"eventbus_pending_messages",
			"消费组 PEL 中未 Ack 的消息数。",
			[]string{"topic", "group"}, nil,
		),
		retryQueue: prometheus.NewDesc(
			"eventbus_retry_queue_length",
			"重试队列中的任务数。",
			nil, nil,
		),
		deadLetter: prometheus.NewDesc(
			"eventbus_dead_letter_length",
			"死信队列中的消息数。",
			nil, nil,
		),
		collectErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "eventbus_collect_errors_total",
				Help: "Runtime 指标采集失败次数，按失败的操作（op）分类。",
			},
			[]string{"op"},
		),
	}
}

// Describe 发送 Collector 的全部指标描述。
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.streamLag
	ch <- c.pending
	ch <- c.retryQueue
	ch <- c.deadLetter
	c.collectErrors.Describe(ch)
}

// Collect 采集 Runtime 统计指标并写入 ch。
//
// 所有 stats 调用共享同一个带超时的 context，因此一次抓取的最长耗时为
// c.timeout，不会因为某个慢查询而无限阻塞 registry.Gather。
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	c.collectStats(ctx, ch)

	// 采集错误计数在 stats 调用之后输出，保证本次新增的错误也能在同一次抓取中看到。
	c.collectErrors.Collect(ch)
}

// collectStats 调用 RuntimeStats 并输出状态指标，采集失败时只累加错误计数。
func (c *Collector) collectStats(ctx context.Context, ch chan<- prometheus.Metric) {
	stats, ok := c.rt.(eventbus.RuntimeStats)
	if !ok {
		c.reportError("runtime_stats")
		c.collectUnavailable(ch)
		return
	}

	for _, sub := range c.subscriptions {
		if lag, err := stats.StreamLag(ctx, sub.Topic, sub.Group); err != nil {
			c.reportError("stream_lag")
		} else {
			ch <- prometheus.MustNewConstMetric(c.streamLag, prometheus.GaugeValue, float64(lag), sub.Topic, sub.Group)
		}

		if pending, err := stats.PendingCount(ctx, sub.Topic, sub.Group); err != nil {
			c.reportError("pending_messages")
		} else {
			ch <- prometheus.MustNewConstMetric(c.pending, prometheus.GaugeValue, float64(pending), sub.Topic, sub.Group)
		}
	}

	if n, err := stats.RetryQueueLen(ctx); err != nil {
		c.reportError("retry_queue_length")
	} else {
		ch <- prometheus.MustNewConstMetric(c.retryQueue, prometheus.GaugeValue, float64(n))
	}

	if dl, ok := c.rt.(deadLetterLener); ok {
		if n, err := dl.DeadLetterLen(ctx); err != nil {
			c.reportError("dead_letter_length")
		} else {
			ch <- prometheus.MustNewConstMetric(c.deadLetter, prometheus.GaugeValue, float64(n))
		}
	} else {
		// Runtime 未实现死信长度查询：输出不可用占位值，不视为采集错误。
		ch <- prometheus.MustNewConstMetric(c.deadLetter, prometheus.GaugeValue, UnavailableValue)
	}
}

// collectUnavailable 输出所有统计指标的不可用占位值。
func (c *Collector) collectUnavailable(ch chan<- prometheus.Metric) {
	for _, sub := range c.subscriptions {
		ch <- prometheus.MustNewConstMetric(c.streamLag, prometheus.GaugeValue, UnavailableValue, sub.Topic, sub.Group)
		ch <- prometheus.MustNewConstMetric(c.pending, prometheus.GaugeValue, UnavailableValue, sub.Topic, sub.Group)
	}
	ch <- prometheus.MustNewConstMetric(c.retryQueue, prometheus.GaugeValue, UnavailableValue)
	ch <- prometheus.MustNewConstMetric(c.deadLetter, prometheus.GaugeValue, UnavailableValue)
}

// reportError 记录一次采集失败。
func (c *Collector) reportError(op string) {
	c.collectErrors.WithLabelValues(op).Inc()
}
