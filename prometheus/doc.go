// Package prometheus 为 eventbus 提供可选的 Prometheus 监控集成。
//
// 本包是独立 Go module（github.com/modern-magic-go/eventbus/prometheus），
// 通过 replace 指向仓库根 module。这样根 module 不新增 Prometheus 依赖（internal/core 仅标准库，根包仅依赖 go-redis），
// 只有显式引入本 module 的使用方才会拉取 prometheus/client_golang。
//
// # 组件
//
// Observer 实现 eventbus.Observer，同时是 eventbus.ErrorObserver 与
// eventbus.DuplicateObserver，把总线生命周期事件转成计数器：
//
//	eventbus_published_total{topic,status}      发布结果，status=success/failed
//	eventbus_handled_total{topic,status}        处理结果，status=success/failed
//	eventbus_retry_scheduled_total{topic}       进入重试队列的次数
//	eventbus_dead_lettered_total{topic}         进入死信队列的次数
//	eventbus_errors_total{op}                   总线内部错误次数（op 见 ErrorEvent.Op）
//	eventbus_duplicates_total{topic}            幂等去重命中、被跳过的次数
//
// 空 topic 统一记为 ""。使用方按需调用 NewObserver 并把它传给
// eventbus.WithObserver：
//
//	reg := prometheus.NewRegistry()
//	bus := eventbus.NewBus(rt, eventbus.WithObserver(prometheus.NewObserver(reg)))
//
// Collector 实现 prometheus.Collector，从 Runtime 的可选能力
// eventbus.RuntimeStats 采集运行态 gauge：
//
//	eventbus_stream_lag{topic,group}            消费组待消费消息数
//	eventbus_pending_messages{topic,group}      消费组 PEL 中的消息数
//	eventbus_retry_queue_length                 重试队列长度
//	eventbus_dead_letter_length                 死信队列长度
//
// 订阅列表（[]Subscription）必须与 Bus.Subscribe 使用的 (topic, group)
// 保持一致，否则采集到的是不存在或已废弃消费组的数据。
//
// # 采集语义
//
// 每次 Collect 都使用带超时的 context（默认 5s，可用 WithCollectTimeout
// 调整）调用 RuntimeStats，因此单个慢查询不会无限阻塞抓取。
//
// 单次 stats 调用失败时跳过对应指标，不输出错误值也不 panic，同时把
// eventbus_collect_errors_total{op} 加一；op 取值为 runtime_stats /
// stream_lag / pending_messages / retry_queue_length / dead_letter_length。
//
// Runtime 未实现 eventbus.RuntimeStats（或 NewCollector 收到 nil）时，
// 所有依赖统计能力的 gauge 输出 UnavailableValue（-1），
// 并记录 eventbus_collect_errors_total{op="runtime_stats"}。
// 死信长度优先使用 Runtime 的可选方法 DeadLetterLen(context.Context) (int64, error)
// （与 eventbus.DeadLetterStore 的方法签名一致）；Runtime 未实现该方法时
// eventbus_dead_letter_length 输出 -1，但不计为采集错误。
//
// # 注册行为
//
// NewObserver 接收 prometheus.Registerer，nil 表示使用 prometheus.DefaultRegisterer。
// 与 prometheus.MustRegister 一致，若注册失败（例如同名指标已由其他类型注册）
// NewObserver 会 panic；但若同名同 label 的 CounterVec 已注册（例如重复调用
// NewObserver），则复用已注册的 collector，不会 panic。
//
// Collector 由使用方自行注册，多个 Collector 注册到同一 registry 会导致
// eventbus_collect_errors_total 描述符冲突，应只注册一个。
package prometheus
