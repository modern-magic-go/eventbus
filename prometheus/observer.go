package prometheus

import (
	"context"
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/modern-magic-go/eventbus"
)

// 指标状态 label 的取值。
const (
	statusSuccess = "success"
	statusFailed  = "failed"
)

// Observer 把 eventbus 的生命周期事件转换成 Prometheus 计数器。
//
// 它实现 eventbus.Observer、eventbus.ErrorObserver 与 eventbus.DuplicateObserver：
// 注入到 Bus 后，发布、处理、重试、死信、内部错误与幂等命中都会被统计。
// 所有方法都只做无阻塞的 label 取值与计数自增，可安全被总线并发调用。
type Observer struct {
	published      *prometheus.CounterVec
	handled        *prometheus.CounterVec
	retryScheduled *prometheus.CounterVec
	deadLettered   *prometheus.CounterVec
	errors         *prometheus.CounterVec
	duplicates     *prometheus.CounterVec
}

var (
	_ eventbus.Observer          = (*Observer)(nil)
	_ eventbus.ErrorObserver     = (*Observer)(nil)
	_ eventbus.DuplicateObserver = (*Observer)(nil)
)

type observerOptions struct {
	constLabels prometheus.Labels
}

// ObserverOption 配置 Observer 的创建。
type ObserverOption func(*observerOptions)

// WithConstLabels 给 Observer 产出的全部指标追加固定 label（如 service、instance）。
// 传入的 map 会被复制，调用方后续修改不会影响已创建的 Observer；
// label 名非法时注册阶段会 panic（与 prometheus.MustRegister 一致）。
func WithConstLabels(labels prometheus.Labels) ObserverOption {
	return func(o *observerOptions) {
		o.constLabels = cloneLabels(labels)
	}
}

// NewObserver 创建 Prometheus Observer 并注册其指标。
//
// reg 为 nil 时使用 prometheus.DefaultRegisterer。每个指标注册一次；若 registry
// 中已存在同名同 label 布局的 CounterVec（例如重复调用 NewObserver），则复用已有
// collector，事件仍然计入同一组指标。若注册因其它原因失败（例如同名指标已由
// 不同类型注册），与 prometheus.MustRegister 一样 panic，便于启动阶段尽早暴露
// 配置错误。
func NewObserver(reg prometheus.Registerer, opts ...ObserverOption) *Observer {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	var cfg observerOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	constLabels := cloneLabels(cfg.constLabels)

	return &Observer{
		published: newCounterVec(reg,
			prometheus.CounterOpts{
				Name:        "eventbus_published_total",
				Help:        "eventbus 发布操作总数，按 topic 与结果（success/failed）分类。",
				ConstLabels: constLabels,
			},
			[]string{"topic", "status"},
		),
		handled: newCounterVec(reg,
			prometheus.CounterOpts{
				Name:        "eventbus_handled_total",
				Help:        "eventbus 消费处理总数，按 topic 与结果（success/failed）分类。",
				ConstLabels: constLabels,
			},
			[]string{"topic", "status"},
		),
		retryScheduled: newCounterVec(reg,
			prometheus.CounterOpts{
				Name:        "eventbus_retry_scheduled_total",
				Help:        "eventbus 进入重试队列的次数，按 topic 分类。",
				ConstLabels: constLabels,
			},
			[]string{"topic"},
		),
		deadLettered: newCounterVec(reg,
			prometheus.CounterOpts{
				Name:        "eventbus_dead_lettered_total",
				Help:        "eventbus 进入死信队列的次数，按 topic 分类。",
				ConstLabels: constLabels,
			},
			[]string{"topic"},
		),
		errors: newCounterVec(reg,
			prometheus.CounterOpts{
				Name:        "eventbus_errors_total",
				Help:        "eventbus 内部错误次数，按出错操作（ErrorEvent.Op）分类。",
				ConstLabels: constLabels,
			},
			[]string{"op"},
		),
		duplicates: newCounterVec(reg,
			prometheus.CounterOpts{
				Name:        "eventbus_duplicates_total",
				Help:        "eventbus 幂等去重命中、被跳过的消息数，按 topic 分类。",
				ConstLabels: constLabels,
			},
			[]string{"topic"},
		),
	}
}

// OnPublished 记录发布成功。
func (o *Observer) OnPublished(_ context.Context, msg eventbus.Message) {
	o.published.WithLabelValues(msg.Topic, statusSuccess).Inc()
}

// OnPublishFailed 记录发布失败。
func (o *Observer) OnPublishFailed(_ context.Context, msg eventbus.Message, _ error) {
	o.published.WithLabelValues(msg.Topic, statusFailed).Inc()
}

// OnHandled 记录一次 handler 成功。
func (o *Observer) OnHandled(_ context.Context, msg eventbus.Message) {
	o.handled.WithLabelValues(msg.Topic, statusSuccess).Inc()
}

// OnHandleFailed 记录一次 handler 失败。
func (o *Observer) OnHandleFailed(_ context.Context, msg eventbus.Message, _ error) {
	o.handled.WithLabelValues(msg.Topic, statusFailed).Inc()
}

// OnRetryScheduled 记录一次进入重试队列。
func (o *Observer) OnRetryScheduled(_ context.Context, task eventbus.RetryTask, _ error) {
	o.retryScheduled.WithLabelValues(task.Topic).Inc()
}

// OnDeadLettered 记录一次进入死信队列。
func (o *Observer) OnDeadLettered(_ context.Context, msg eventbus.DeadLetterMessage) {
	o.deadLettered.WithLabelValues(msg.Topic).Inc()
}

// OnError 记录一次总线内部错误，按操作名聚合。
func (o *Observer) OnError(_ context.Context, event eventbus.ErrorEvent) {
	o.errors.WithLabelValues(event.Op).Inc()
}

// OnDuplicate 记录一次幂等去重命中。
func (o *Observer) OnDuplicate(_ context.Context, msg eventbus.Message) {
	o.duplicates.WithLabelValues(msg.Topic).Inc()
}

// cloneLabels 复制 label 集合，避免调用方后续修改影响已注册指标。
func cloneLabels(labels prometheus.Labels) prometheus.Labels {
	if len(labels) == 0 {
		return nil
	}
	cloned := make(prometheus.Labels, len(labels))
	for k, v := range labels {
		cloned[k] = v
	}
	return cloned
}

// newCounterVec 创建并注册 CounterVec。
//
// 重复注册（同名同 label 布局）时复用 registry 中已有的 CounterVec，使多次
// NewObserver 指向同一组指标；其它注册错误直接 panic，尽早暴露指标名冲突。
func newCounterVec(reg prometheus.Registerer, opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	vec := prometheus.NewCounterVec(opts, labelNames)
	if err := reg.Register(vec); err != nil {
		var already prometheus.AlreadyRegisteredError
		if errors.As(err, &already) {
			if existing, ok := already.ExistingCollector.(*prometheus.CounterVec); ok {
				return existing
			}
		}
		panic(fmt.Sprintf("eventbus/prometheus: 注册指标 %s 失败: %v", opts.Name, err))
	}
	return vec
}
