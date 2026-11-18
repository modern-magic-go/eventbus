# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

计划随 v0.3.0 发布，对应 [issue #1](https://github.com/modern-magic-go/eventbus/issues/1) 剩余的 P1/P2 项、[issue #3](https://github.com/modern-magic-go/eventbus/issues/3)（header 契约建议）与 [issue #4](https://github.com/modern-magic-go/eventbus/issues/4)（PEL / 重试存储可靠性）。

### Added
- `ConsumerConfig.RetryMaxDelay`：指数退避上限，默认 `DefaultRetryMaxDelay`（5m）
- `ConsumerConfig.HandlerTimeout`：单次 handler 调用超时，0 表示不限（默认）；handler 必须尊重 ctx 才能被真正中断
- `ConsumerConfig.StartFrom`：新建消费组初始 offset（`""`/`"0"` 从头、`"$"` 最新、unix 毫秒时间戳，另支持显式 stream ID）；新增 `StartFromBeginning` / `StartFromLatest` 常量
- 可选接口 `ConsumerGroupInitializer`（`EnsureConsumerGroupFrom`），`RedisRuntime` 已实现；新增 `ErrInvalidStartFrom` / `ErrStartFromUnsupported`
- `DedupConfig` / `DedupStore` / `WithDedup`：消费端两阶段幂等去重；新增根包导出 `DedupStatus` 类型与 `DedupAcquired` / `DedupDone` / `DedupInProgress` 常量（`String()` 返回 `acquired` / `done` / `in_progress`，自定义 `DedupStore` 实现需要）与 `DedupStore.MarkDone`；默认 key `topic|group|messageID`、TTL `DefaultDedupTTL`（24h）；仅 Done 跳过，InProgress 继续处理（at-least-once），`KeyFunc` 返回空 key 时 fail-open 并上报 `dedup_key`
- `RedisDedupStore` / `RedisDedupStoreConfig` / `NewRedisDedupStore`：基于 Redis 的两阶段去重存储（占位 `pending` / 完成 `done`；`SET NX PX` / `SET PX` / `DEL`）
- 可选接口 `DuplicateObserver`（`OnDuplicate`）：去重命中时回调
- 死信查询与重放：`Bus.DeadLetters`（只读分页，index 0 为最新）、`Bus.ReplayDeadLetters`（从最旧记录弹出并重新写入重试队列，异步重放）、`ReplayResult`（`Replayed` / `Skipped` / `Errors`）
- 可选接口 `DeadLetterStore`（`ListDeadLetters` / `PopDeadLetters` / `DeadLetterLen`），`RedisRuntime` 已实现；新增 `ErrDeadLetterStoreUnsupported`
- `DeadLetterMessage.ID`：死信记录自身的 ID，由 Runtime 写入时生成（v0.2 旧条目无 ID）
- `RetryTask.Key`：可选的重试任务 member 覆盖；DLQ 重放使用独立 `key:` member，避免与旧任务的 `RemoveRetry` 冲突
- `ErrorEvent` / `ErrorObserver`：可选接口，暴露 consume / reclaim / ack / schedule_retry / remove_retry / move_dead_letter / dedup_key / dedup_acquire / dedup_mark_done / dedup_release 等内部错误（老 `Observer` 无需实现，非破坏）
- `WithLogger(*slog.Logger)`：注入标准库 slog 输出内部错误（未注入则完全静默），错误仍可通过 `ErrorObserver` 观测
- `NewSlogObserver(logger)`：内置 slog Observer，同时实现 `ErrorObserver` / `DuplicateObserver`，零第三方依赖
- 可选接口 `RuntimeStats`（`StreamLag` / `PendingCount` / `RetryQueueLen`），`RedisRuntime` 已实现
- 新增嵌套 module `github.com/modern-magic-go/eventbus/otel`（独立 `go.mod`，依赖 OTel v1.41.0）：`InjectTrace` / `ExtractTrace` / `Publish`（producer span）/ `WrapHandler`（consumer span），以及 `WithTracerProvider` / `WithPropagator` / `WithSpanNameFormatter` 选项
- 新增嵌套 module `github.com/modern-magic-go/eventbus/prometheus`：`NewObserver`（实现 `Observer` / `ErrorObserver` / `DuplicateObserver`）、`NewCollector`（实现 `prometheus.Collector`）、`Subscription`、`WithConstLabels`、`WithCollectTimeout`；指标含 counters / gauges，统计不可用时值为 `-1`
- `ErrStopTimeout`：优雅停机等待在途 handler 超时
- `RedisRuntimeConfig.RetryDataKey`：重试 payload HASH key（默认 `<Namespace>:retry:data`）

### Changed
- **行为变更**：`StopWithTimeout` 改为 drain 语义：先停止拉取新消息，再等待在途 handler 完成；等待超时返回 `ErrStopTimeout`（不再返回 nil），且超时后**不关闭**注入的 Runtime（避免 handler 使用已关闭连接）；`WithCloseRuntime(true)` 仅在 worker 全部退出后关闭 runtime
- **行为变更**：指数退避加入 equal jitter（实际延迟 `[delay/2, delay]`）与上限 `RetryMaxDelay`（默认 5m），防止多实例同时重试；名义延迟仍为 `base × 2^(attempt-1)`
- **行为变更**：重试存储改为 ZSET member 索引 + HASH payload；`MessageID` 非空时 member 稳定为 `mid:<group>:<id>`，同一消息重复调度幂等覆盖；v0.2 旧 JSON member 自动兼容并在重新调度时原子迁移
- **行为变更**：去重语义改为两阶段：`Acquire` 返回 `DedupAcquired` / `DedupDone` / `DedupInProgress`，handler 成功后 `MarkDone`、失败后（仅 Acquired）`Release`。只有 Done 跳过，InProgress 继续执行 handler（可能重复处理，避免崩溃 / Release 失败丢消息）；`MarkDone` / `Release` 使用 `context.WithoutCancel`，停机取消后仍可完成
- 内部错误改为可观测：consume / reclaim / ack / 重试 / 死信 / 去重等错误通过 `ErrorObserver` 或 `WithLogger` 上报，不再静默
- PEL 孤儿表述修正：Redis 7 的 `XAUTOCLAIM` 自身会移除被 `XTRIM` / `StreamMaxLen` 裁剪的 PEL 孤儿；`ClaimPending` 改用 raw 命令解析回复第三个元素（deleted IDs）并对这些 ID 幂等 `XACK` 作为防御（兼容 Redis 6.2 两元素回复）；Redis 6.2 无 deleted IDs，无法清理
- `RedisRuntime.StreamLag` / `PendingCount` 在 stream 或 group 不存在时返回 0（视为无积压），不返回错误
- 重试路径中 `MessageID` 非空的任务不再「先写新任务再删旧任务」，依赖幂等覆盖；无 `MessageID` 的旧任务保持旧语义
- `PollRetry` 对无法反序列化的坏 member 主动清理（新格式同时 `HDEL` payload），坏数据被清理，不会永久阻塞队列；DLQ 读取仍跳过坏条目
- 死信重放：`ReplayDeadLetters` 写入的重试任务使用独立 `RetryTask.Key`；`ReplayResult.Errors` 改为带 `topic` / `group` / `message_id` / `dlq_id` 的 wrapped error；`PopDeadLetters` 部分成功后报错仍会处理已弹出的记录
- `otel` / `prometheus` 子模块补充版本 tag 安装方式（`otel/v0.3.0`、`prometheus/v0.3.0`），各自 `go.mod` require 根 module v0.3.0
- README 新增/重写章节：死信查询与重放、消费端幂等去重、可观测性、StartFrom、PEL 孤儿清理、顺序与 partition、header 契约、outbox 建议、drain 停机、升级/兼容性；`examples/basic` 补充 header 契约、`WithDedup`、StartFrom 注释与 `ErrStopTimeout` 处理
- 根 module 依赖集合保持不变（自 v0.2 起即依赖 go-redis / miniredis / testify，本次未新增）；`otel` / `prometheus` 为独立嵌套 module，不导入即不引入额外依赖

### Fixed
- 修复 DLQ 只能写入、无法查询/重放的问题（issue #1 · P1-4）
- 补齐消费端幂等去重能力，避免 at-least-once 下业务重复实现（issue #1 · P1-5 消费端部分；outbox 仍由业务侧实现）
- 修复去重 `Release` 失败 / 停机取消 ctx 导致占位残留、重试被误判重复而丢消息的问题（`MarkDone` / `Release` 使用 `context.WithoutCancel`，InProgress 继续处理）
- 修复并发场景下 `Release` 可能删除其他处理者写入的 Done 标记、导致已成功消息被重复处理的问题（Release 仅删除 `pending` 占位，`done` 为终态）
- 修复去重两实例并发 / 崩溃残留场景下把 InProgress 误判为 Done 而跳过消息的风险（两阶段语义，仅 Done 跳过）
- 修复退避无 jitter / 无上限导致的惊群与延迟无限增长（issue #1 · P2）
- 修复 handler 无独立超时的问题（issue #1 · P2）
- 修复 consume / reclaim / ack 等内部错误静默、无日志/无指标的问题（issue #1 · P2 可观测性）
- 修复 `StopWithTimeout` 无 drain、取消即中断在途 handler 的问题
- 修复新建消费组初始 offset 固定为从头、无法选择 `$` / 时间戳的问题
- 修复 `StreamMaxLen` 裁剪后 PEL 孤儿无法感知/清理的问题（Redis 7 由 `XAUTOCLAIM` 自动移除，本实现解析 deleted IDs 并幂等 `XACK` 防御；Redis 6.2 无法清理，issue #4 · 4-2）
- 修复 `ReplayDeadLetters` 在 `PopDeadLetters` 部分成功后报错、已弹出记录被丢弃（丢消息）的问题
- 修复 DLQ 重放与 retry worker 清理共用 member 导致刚重放的任务被误删的问题（重放任务使用独立 `RetryTask.Key`）
- 修复 `ReplayResult.Errors` 缺少上下文的问题（改为 wrapped error，含 topic / group / message_id / dlq_id）
- 修复 `PollRetry` 坏 member 永久占据队首、阻塞合法任务的问题（主动清理 ZSET / HASH）
- 修复重试任务换位非原子可能产生重复任务的问题（`MessageID` 非空任务通过稳定 member 幂等覆盖；无 `MessageID` 旧任务保持兼容）
- 修正 README 中已解决项仍被列为限制的文档问题（无幂等去重、无 DLQ 重放、退避无 jitter/上限、无 handler 超时、默认完全静默、reclaim 错误静默、Stop 无 drain、初始 offset 固定、PEL 孤儿无法清理等）

## [0.2.0] - 2026-11-18

对应 [issue #1](https://github.com/modern-magic-go/eventbus/issues/1)（v0.1.1 可靠性 review）的 P0 修复。

### Added
- `ConsumerConfig.PendingReclaim` 与 `PendingReclaimConfig`（`MinIdle` / `Batch` / `Interval`），支持 pending 消息自动回收；零值禁用，任一字段非零即启用
- 可选接口 `PendingReclaimer`（`ClaimPending`），根包 re-export；`RedisRuntime` 基于 Redis 6.2+ 的 `XAUTOCLAIM` 实现
- `ErrPendingReclaimUnsupported`：启用 `PendingReclaim` 但运行时未实现 `PendingReclaimer` 时，`Subscribe` 返回该错误
- `RetryTask.MessageID` / `DeadLetterMessage.MessageID`：重试与死信消息保留原始消息 ID，便于审计与追踪
- `ErrHandlerPanic`：handler panic 被 recover 后返回的错误，可用 `errors.Is` 判断
- `DefaultReclaimMinIdle`（30s）、`DefaultReclaimInterval`（10s）默认值常量
- `StopWithTimeout(timeout)` 支持优雅停机超时控制
- `WithCloseRuntime(bool)` 选项控制 Stop 时是否关闭底层 Runtime
- `examples/` 目录提供完整可运行示例

### Changed
- **Breaking**: 失败处理语义变更：handler 失败（含 panic）不再立即 `XACK`，而是先写重试/死信任务（达到 `MaxRetries` 时写 DLQ），任务写入成功后才 `XACK` 原 stream 消息（所有权转移，避免 PEL 泄漏）；任务写入失败则不 `XACK`，留给 reclaim 或人工处理
- handler panic 不再导致进程退出，统一走失败处理路径（retry → DLQ），并触发 `OnHandleFailed`（首次投递与 pending 认领路径；retry worker 路径保持基线行为）
- `RedisRuntime` 默认 consumer name 在构造时生成一次（`eventbus-<pid>-<随机后缀>`），同一 Runtime 生命周期内保持稳定
- README 补充投递语义、pending 回收、已知限制章节；`examples/basic` 修复 module 路径并加入 pending 回收配置示例
- `Stop()` 默认 30s 超时等待 worker 结束
- 升级注记：v0.1.1 已入队的 `RetryTask` 不含 `message_id`，升级后这些旧任务重试时 `Message.ID` 为空、进入 DLQ 时 `MessageID` 为空；旧任务处理完后恢复

### Fixed
- 修复消费者崩溃/重启后 pending 消息永久滞留 PEL、无副本接管的问题
- 修复 handler 失败后原消息未 `XACK`，导致 PEL 泄漏且与重试任务重复消费的问题
- 修复 handler panic 导致整个进程退出的问题
- 修复 `RetryTask` / `DeadLetterMessage` 丢失原始消息 ID 的问题
- 修复 pending 认领丢弃 `XAUTOCLAIM` 返回的 next cursor、PEL 尾部消息可能长期无法被认领的问题
- 修复 retry worker 在 `ScheduleRetry` / `MoveToDeadLetter` 失败时仍删除旧重试任务、可能导致消息丢失的问题
- 修正 `Message.PublishedAt` 语义：消费时取 stream 中记录的发布时间，而不是消费时刻（重试路径仍为当前时间；属可观测行为变化）
- `PollRetry` 使用 `ZRangeArgs` 替代已废弃的 `ZRangeByScore`
- `cloneHeaders` 使用 `maps.Copy` 替代手动循环
