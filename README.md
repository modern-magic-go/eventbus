# eventbus

可复用的 Go 事件总线模块，支持：

- 显式构造的 `Bus` 实例，无包级单例
- 发布 / 订阅 / 消费生命周期管理
- 失败重试与死信路由编排；死信查询与重放（Runtime 支持时）
- 消费端幂等去重（可选 `DedupStore`，内置 Redis 实现）
- 指数退避带 jitter 与上限、handler 超时、drain 优雅停机
- 新建消费组初始 offset 可选（`StartFrom`）
- pending 消息自动回收（可选，基于 Redis `XAUTOCLAIM`；Redis 7 由 `XAUTOCLAIM` 自动移除 PEL 孤儿）
- 可观测性：`Observer`、可选 `ErrorObserver` / `DuplicateObserver`、标准库 `slog`，以及可选的 Prometheus / OpenTelemetry 子模块
- 基于 Redis 的默认运行时实现

投递语义取决于配置：启用 pending 回收时为 at-least-once（至少一次，可能重复），详见 [投递语义](#投递语义)。

> handler 仍需按 `Message.ID` 幂等；需要开箱即用的去重可启用 [消费端幂等去重](#消费端幂等去重)。

## 安装

```bash
go get github.com/modern-magic-go/eventbus
```

当前 module 路径为 `github.com/modern-magic-go/eventbus`，发布到对应仓库后，其他项目即可直接 `go get`。

可选子模块（独立 Go module，按需引入）：

```bash
# OpenTelemetry 链路追踪（producer / consumer span、W3C traceparent 传播）
# 标签形如 otel/v0.3.0，引入时按模块 tag 锁定版本
go get github.com/modern-magic-go/eventbus/otel@v0.3.0

# Prometheus 指标（Observer + Runtime Collector）
# 标签形如 prometheus/v0.3.0
go get github.com/modern-magic-go/eventbus/prometheus@v0.3.0
```

两个子模块的 `go.mod` 均以 `github.com/modern-magic-go/eventbus v0.3.0` 引用根 module（通过 `replace ../` 在仓库内开发时指向本地目录）。

## 目录结构

- 根目录：稳定的对外 API，保持导入路径为 `github.com/modern-magic-go/eventbus`（`api.go` 为公开入口，`redis.go` 为 Redis 相关 re-export）
- `internal/core/`：事件总线核心实现（只依赖 Go 标准库）
- `internal/redisruntime/`：Redis 运行时实现（依赖 go-redis）
- `internal/integration/`：Redis 集成测试与可选的真实 Redis e2e 测试
- `otel/`：可选的 OpenTelemetry 子模块，独立 `go.mod`（依赖 OTel v1.41.0）
- `prometheus/`：可选的 Prometheus 子模块，独立 `go.mod`（依赖 prometheus/client_golang）
- `examples/basic/`：可运行示例

> 两个可选子模块都是**嵌套 module**：各自的第三方依赖（OTel / Prometheus）不会传导给根 module，也不会被未导入它们的应用下载或编译。
>
> 依赖口径说清楚：`internal/core` 只依赖 Go 标准库；但**根 module 本身自 v0.2 起就依赖 go-redis**（根包 `redis.go` 引用 `internal/redisruntime`），测试依赖 miniredis / testify，本次未新增依赖。因此即使业务只使用自定义 Runtime，导入根包仍会带入 go-redis 模块依赖（只是不会建立 Redis 连接）。

## 快速开始

```go
package main

import (
	"context"
	"log"

	redis "github.com/redis/go-redis/v9"

	"github.com/modern-magic-go/eventbus"
)

func main() {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

	rt := eventbus.NewRedisRuntime(client, eventbus.RedisRuntimeConfig{
		Namespace:    "demo",
		ConsumerName: "worker-1",
	})

	bus := eventbus.NewBus(rt)
	if err := bus.Subscribe(eventbus.ConsumerConfig{
		Topic: "order.created",
		Group: "order-worker",
		Handler: func(ctx context.Context, msg eventbus.Message) error {
			log.Printf("received: %s", string(msg.Payload))
			return nil
		},
	}); err != nil {
		log.Fatal(err)
	}

	if err := bus.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer bus.Stop()

	if err := bus.Publish(ctx, "order.created", map[string]any{"id": 1}); err != nil {
		log.Fatal(err)
	}
}
```

## 投递语义

本模块**不无条件承诺** at-least-once，需要按配置判断：

- **启用 `PendingReclaim` 且 consumer name 稳定时**：提供 at-least-once，即每条消息至少被处理一次，可能重复；
- **`PendingReclaim` 默认关闭时**：消费者在 `XREADGROUP` 之后、`XACK` 之前崩溃/停机，消息会停留在 PEL 且**不会自动重投**，需要开启 reclaim 或人工 `XCLAIM`。

无论哪种配置，都不承诺 exactly-once，也不承诺全局顺序。

### 至少一次，可能重复

- `XACK` 语义：处理成功才 `XACK`；处理失败（含 panic、handler 超时）时，重试/死信任务写入成功后才 `XACK` 原消息（所有权转移），失败消息不会长期滞留 PEL（Ack 本身失败时除外）；写入失败则不 `XACK`，留给 reclaim 或人工处理。
- 以下场景会**重复投递**同一条消息：
  - handler 处理成功但进程在 `XACK` 前崩溃/被 kill；
  - 一批消息的处理总耗时超过 `PendingReclaim.MinIdle`，尚未轮到处理的消息被 reclaim 认领（同一实例或其他实例均可能，详见 [Pending 消息回收](#pending-消息回收)）；
  - handler 超时，本次处理被判为失败并进入重试，而 handler 已产生的副作用（如下游写库）已经发生；
  - 停机/取消时已 fetch 但未处理的批次留在 PEL，开启 reclaim 后会被重投；
  - 「先写重试/死信任务、再 `XACK`」两步之间崩溃，会同时存在重试任务与 PEL 原件（不漏消息，但可能重复）；
  - 重试任务执行成功后、删除任务前崩溃，下一轮会再次执行 handler。
- 因此 **handler 必须按 `Message.ID` 做幂等**（例如唯一索引、状态机 CAS、去重表）。`Message.ID` 在发布时生成（雪花 ID），可通过 `eventbus.WithMessageID` 显式指定，并在重试/死信任务中以 `RetryTask.MessageID` / `DeadLetterMessage.MessageID` 保留下来，便于审计与幂等键设计。
- 本版本提供了进程内可选的 [消费端幂等去重](#消费端幂等去重)（`WithDedup` + `DedupStore`），可对**已成功处理（Done）**的重复消息自动跳过；InProgress、窗口过期或存储故障时仍按 at-least-once 处理（可能重复执行 handler）。

### handler 超时与 drain

- **单次 handler 超时**：`ConsumerConfig.HandlerTimeout`（默认 0 = 不限）为每次 handler 调用包一层 `context.WithTimeout`。超时后 handler 返回错误，按普通失败进入重试/死信流程。**handler 必须尊重 ctx 才会被真正中断**：如果 handler 忽略 ctx，总线无法强杀 goroutine，只能等它返回（`HandlerTimeout` 只是设置 deadline），因此下游调用应传递并使用该 ctx。
- **drain 停机**：主动调用 `Stop` / `StopWithTimeout` 时，总线先停止拉取新消息，再等待在途 handler 处理完当前批次；等待超过超时时间返回 `ErrStopTimeout`。详见 [优雅停机](#优雅停机)。
- **parent ctx 取消**：直接取消在途 handler，不做 drain，用于进程退出/强制取消场景。

### 幂等去重窗口

- 去重是**两阶段**的：`Acquire` 占位 → handler 成功 `MarkDone`（标记已处理）/ 失败 `Release`（释放占位，仅对 Acquired 的占位）。只有**已 Done** 的重复消息才会被跳过并自动 `XACK`（触发 `DuplicateObserver`）。
- 占位存在但未完成（InProgress：其他实例处理中，或上次处理崩溃 / 释放失败残留）时，总线会**继续执行 handler**：跳过可能丢消息，因此 at-least-once 优先，代价是**可能重复处理**。
- TTL（默认 `DefaultDedupTTL` = 24h）只决定 **Done 标记**保留多久：窗口内命中 Done 会跳过，窗口过期后同一条消息会再次处理。
- 去重依赖非空 `Message.ID`；存储操作失败时 fail-open（继续处理并上报错误），退化为 at-least-once。详见 [消费端幂等去重](#消费端幂等去重)。

### 不保证顺序

- 同一 topic 使用单个 Redis stream，多个消费者/多实例并发处理时，同一订单/聚合根的事件可能乱序。
- 单个消费者的一个批次内按 stream 顺序串行处理，但跨实例、跨重启不保证。
- 对顺序敏感的场景，需要业务侧保证「单消费者单消费组」，或按聚合根在业务层做 partition（库不内置），详见 [顺序与 partition](#顺序与-partition)。

### 失败重试与死信流转

handler 返回 error、被 panic 中断或 handler 超时，按以下路径处理：

1. 写入重试队列：Redis ZSET（`<namespace>:retry`）保存 member/score 索引，HASH（`<namespace>:retry:data`）保存任务 JSON。member 对 `MessageID` 非空的任务稳定为 `mid:<group>:<messageID>`，因此同一消息重复调度会**原地覆盖**（刷新 `Attempt` / `AvailableAt` / `LastError`），不会堆积重复任务；`MessageID` 为空时保持旧语义（任务 JSON 作为 member，无法幂等）。
2. 重试/死信任务写入成功后，才 `XACK` 原 stream 消息——消息所有权转移给重试队列/死信队列，PEL 中不残留；写入失败则不 `XACK`，留给 reclaim 或人工处理。
3. 总线后台 retry worker 按 `WithRetryPollInterval`（默认 1s）轮询到期任务并重新执行 handler。
4. 失败后按 `RetryBaseDelay`（默认 5s）指数退避，名义延迟为 `base × 2^(attempt-1)`，受 `ConsumerConfig.RetryMaxDelay`（默认 `DefaultRetryMaxDelay` = 5m）上限约束，并叠加 **equal jitter**：实际延迟在 `[delay/2, delay]` 内随机，避免多实例同时重试造成惊群。
   默认配置下前几次重试实际等待约为：第 1 次 2.5–5s、第 2 次 5–10s、第 3 次 10–20s……依次翻倍直到 5m 上限。
5. `Attempt` 达到 `MaxRetries`（默认 3，可按 `ConsumerConfig` 覆盖）时，写入死信队列（Redis list，`<namespace>:dlq`）后移除重试任务，并触发 `Observer.OnDeadLettered`。死信记录由 Runtime 在写入时生成 `DeadLetterMessage.ID`。
6. 进入 DLQ 的消息可通过 [死信查询与重放](#死信查询与重放) 查询或重新投递。

**重试/死信写入失败时不会 `XACK` 原消息**，消息保留在 PEL 中，避免「失败消息凭空消失」：启用 `PendingReclaim` 时会在空闲超时后被回收重试；未启用则会一直停留在 PEL，需要人工处理（或后续开启回收）。写入失败的错误可通过 `ErrorObserver` 的 `handle_failure` / `schedule_retry` / `move_dead_letter` / `remove_retry` / `poll_retry` op 观测（见 [可观测性](#可观测性)）。

> 重试存储格式与 v0.2.0 兼容：v0.2 的旧 member（直接存任务 JSON）仍可被 `PollRetry` 读取；旧任务在重新调度时会原子迁移到新格式（`ScheduleRetry` 在同一事务里删除旧 member、写入新 member）。v0.2 之前（无 `MessageID`）的旧任务只能按旧语义追加/先写后删，无法幂等。`PollRetry` 会对无法反序列化的坏 member 主动清理（新格式同时清理 HASH payload），避免坏数据永久占据队首阻塞后续任务。

### panic 兜底

- 每次 handler 调用都会被 `recover()` 包裹。handler panic 不再导致进程退出。
- panic 会被转换为错误（`errors.Is(err, eventbus.ErrHandlerPanic)` 为 true），触发 `Observer.OnHandleFailed`（首次投递与 pending 认领路径；retry worker 中的失败不触发该回调，见已知限制），并走与普通 error 完全相同的重试/死信路径。
- panic 堆栈会被截断到 2KB 再写入 `LastError` / DLQ，避免异常长堆栈放大 Redis 内存占用。
- 建议在 handler 内部对可预期的错误返回 error，而不是依赖 panic；panic 兜底只是最后一道防线。

## Pending 消息回收

消费者在 `XREADGROUP` 之后、`XACK` 之前崩溃或重启，消息会停留在 Redis PEL 中。开启 `PendingReclaim` 后，总线会周期性认领空闲超时的 pending 消息，并走与正常消费完全相同的 handler / 重试 / 死信路径。

### 配置示例

```go
bus.Subscribe(eventbus.ConsumerConfig{
	Topic: "order.created",
	Group: "order-worker",
	Handler: handleOrderCreated,
	// 建议 PollBatchSize=1，减少批量预取导致的同实例重复处理。
	PollBatchSize: 1,
	PendingReclaim: eventbus.PendingReclaimConfig{
		// 消息在 PEL 中空闲超过该时长才会被认领。
		// 建议 ≥ 单条 handler p99 × 2；不能把批大小调为 1 时，应 ≥ PollBatchSize × handler p99 × 2。
		MinIdle: 30 * time.Second, // 零值默认 eventbus.DefaultReclaimMinIdle（30s）
		Batch:   10,               // 零值默认 10
		Interval: 10 * time.Second, // 零值默认 eventbus.DefaultReclaimInterval（10s）
	},
})
```

### 启用条件与默认值

- `PendingReclaim` 的**零值表示禁用**；`MinIdle` / `Batch` / `Interval` 任一字段非零即启用。想全部使用默认值又需要启用时，显式设置任一字段即可，例如 `PendingReclaimConfig{MinIdle: eventbus.DefaultReclaimMinIdle}`。
- 启用后未显式设置的字段使用默认值：
  - `MinIdle`：`eventbus.DefaultReclaimMinIdle` = 30s
  - `Batch`：10
  - `Interval`：`eventbus.DefaultReclaimInterval` = 10s
- 需要底层 Runtime 实现可选接口 `PendingReclaimer`：
  - `RedisRuntime` 已实现（内部使用 Redis `XAUTOCLAIM`，**要求 Redis 6.2+**；Redis 7 的 `XAUTOCLAIM` 会移除 PEL 孤儿，本实现解析 deleted IDs 并幂等 `XACK`，见 [StreamMaxLen 与 PEL 孤儿清理](#streammaxlen-与-pel-孤儿清理)）；
  - Runtime 未实现该接口时，`Subscribe` 返回 `eventbus.ErrPendingReclaimUnsupported`。这里的「不会静默降级」仅指接口能力检查；Redis 版本过低导致的运行时失效不在 `Subscribe` 的检查范围内。

### 使用注意

- **`MinIdle` 必须覆盖「一批消息的处理总耗时」**，而不是单条 handler 耗时。`XREADGROUP` 一次会预取一批消息，idle 从投递时刻开始计算；当前实例还没轮到处理的消息也可能被 reclaim goroutine 认领，导致**同一实例重复处理**。v0.2 实测：`PollBatchSize=10`、单条 handler 500ms、`MinIdle=1s` 时，10 条消息共触发 17 次 handler 调用（7 条被重复处理）。
- 推荐配置：**`PollBatchSize=1`，且 `MinIdle` ≥ 单条 handler p99 × 2**；如果不能改批大小，则按 `MinIdle ≥ PollBatchSize × handler p99 × 2` 设置。`MinIdle` 过小会造成重复处理，过大则故障恢复变慢。
- 吞吐：每轮以 `Batch` 为目标认领上限、间隔 `Interval`，默认 `Batch=10` / `Interval=10s`；实现会跨多次 `XAUTOCLAIM` 扫描 cursor 以凑满一批。PEL 积压多时调大 `Batch` / 缩小 `Interval`，但要同时考虑上面的批量耗时约束。
- 认领的消息同样受 `MaxRetries` / DLQ 约束，不会无限重试。
- pending 回收依赖消费者名称稳定：`RedisRuntime` 在构造时生成一次默认 consumer name（`eventbus-<pid>-<随机后缀>`），同一 Runtime 生命周期内不变；显式配置 `RedisRuntimeConfig.ConsumerName` 或 `ConsumerConfig.ConsumerName` 时以显式值为准。
- **错误不再静默**：消费拉取、`XAUTOCLAIM`、`XACK` 等内部错误会通过 `ErrorObserver`（`op=consume` / `reclaim` / `ack` 等）和可选 `WithLogger` 上报；不注入观测组件时仍不输出日志。Redis 版本过低时 reclaim 会持续失败，建议注入 `ErrorObserver` 或 `WithLogger` 以便尽早发现。
- Redis 版本：`XAUTOCLAIM` 需要 Redis 6.2+。生产环境建议先显式验证 reclaim 生效。

## 死信查询与重放

死信队列是 Redis list（默认 `<namespace>:dlq`）。当 Runtime 实现可选接口 `DeadLetterStore`（`RedisRuntime` 已实现）时，可以用根包方法查询与重放；否则调用返回 `eventbus.ErrDeadLetterStoreUnsupported`。

### 查询死信

```go
msgs, err := bus.DeadLetters(ctx, 0, 20) // offset, limit
if err != nil {
	if errors.Is(err, eventbus.ErrDeadLetterStoreUnsupported) {
		// Runtime 不支持死信存储（例如自定义 Runtime），需自行实现查询。
	}
	log.Fatal(err)
}
for _, msg := range msgs {
	log.Printf("dlq id=%s message_id=%s topic=%s attempt=%d error=%s",
		msg.ID, msg.MessageID, msg.Topic, msg.Attempt, msg.Error)
}
```

- `DeadLetters(ctx, offset, limit)` 只读分页，**index 0 为最新写入**（Runtime 用 `LPUSH` 写入）。`limit<=0` 时使用 `DefaultPollBatchSize`（10），单次上限 1000；`offset<0` 按 0 处理。
- 返回的 `DeadLetterMessage` 包含 `ID`（死信记录自身的 ID，由 Runtime 写入时生成）、原始 `MessageID`、`Topic` / `Group` / `Payload` / `Headers` / `Attempt` / `MaxRetries` / `Error` / `OccurredAt`。
- v0.2.0 写入的旧死信条目没有 `ID` 字段，解析后 `ID` 为空；坏数据（无法反序列化）会被跳过，不影响整页读取。

### 重放死信

```go
res, err := bus.ReplayDeadLetters(ctx, 100)
if err != nil {
	log.Fatal(err)
}
log.Printf("replayed=%d skipped=%d errors=%d", res.Replayed, res.Skipped, len(res.Errors))
```

- `ReplayDeadLetters(ctx, limit)` 从 DLQ **最旧记录**开始弹出（Redis `RPOP`），逐条重新写入重试队列（`Attempt=1`、`AvailableAt=now`），由后台 retry worker **异步执行 handler**；本方法只负责入队，不等待 handler 结果。`limit<=0` 时使用 `DefaultPollBatchSize`，单次上限 1000。
- **部分弹出失败不丢记录**：`PopDeadLetters` 是破坏性操作，可能在已弹出部分记录后返回错误；此时总线仍会处理已弹出的记录，并把 pop 错误追加到 `result.Errors`；只有一条都没弹出时才整体返回该错误。
- 重放写入的重试任务使用独立的 `RetryTask.Key`（`key:` 前缀 member），不会与 retry worker 正在清理的旧任务 member 冲突（否则旧任务的 `RemoveRetry` 可能误删刚入队的新任务）。
- 返回 `ReplayResult{Replayed, Skipped int; Errors []error}`：
  - `Replayed`：成功重新入队（进入重试队列）的数量；
  - `Skipped`：找不到对应订阅（当前 `Bus` 未 `Subscribe` 该 topic+group）的记录数；会**尝试**放回 DLQ，放回失败会额外记入 `Errors`，但仍计入 `Skipped`；
  - `Errors`：逐条重放失败的错误，均为带上下文的 wrapped error（包含 `topic` / `group` / `message_id` / `dlq_id`）；若 `PopDeadLetters` 中途失败，还会追加一条全局错误 `pop dead letters (already popped N records): ...`（无单条上下文）。单条失败不会中断整批。
- 找不到订阅的记录会通过 `MoveToDeadLetter` 放回 DLQ（重新生成/保留 ID，位于队列最新端），因此重放后顺序可能变化。

### 语义与限制

- **`Errors` 有两种形态**：`PopDeadLetters` 中途失败时追加全局错误 `pop dead letters (already popped N records): ...`（无单条上下文，已弹出的记录仍会继续处理）；逐条处理失败（重新入队、放回 DLQ 等）为带 `topic` / `group` / `message_id` / `dlq_id` 的 wrapped error。
- 重放依赖 `Bus` 已注册对应订阅；否则该条记录会 `Skipped`。
- 重放不是事务操作：`PopDeadLetters` 弹出记录后，进程崩溃在重新入队之前会丢消息；对可靠性要求高的场景请在业务侧记录重放审计或使用独立脚本。
- DLQ 目前没有 TTL、没有自动裁剪；Redis flush / evict 即丢失。生产环境建议同时通过 `Observer.OnDeadLettered` 落审计日志或转发到告警/工单系统。
- 并发调用 `ReplayDeadLetters` 没有全局互斥；建议由单实例/单任务触发。
- 重放的消息同正常重试一样受 `MaxRetries` 约束（`Attempt` 从 1 重新开始），并重新走一遍退避。

## 消费端幂等去重

可选能力：注入 `DedupStore` 后，总线在调用 handler 前先按去重键占位。去重接口是**两阶段**的（`Acquire` → `MarkDone` / `Release`）：

- `Acquire` 返回三种状态（根包导出 `eventbus.DedupStatus` 类型与 `eventbus.DedupAcquired` / `eventbus.DedupDone` / `eventbus.DedupInProgress` 常量，`String()` 分别返回 `acquired` / `done` / `in_progress`）：`DedupAcquired`（首次占位，需要执行 handler）、`DedupDone`（此前已成功处理，可跳过）、`DedupInProgress`（占位存在但未完成，可能是其他实例处理中或崩溃/释放失败残留）；
- `DedupAcquired` 与 `DedupInProgress` **都会执行 handler**（跳过 InProgress 可能丢消息，at-least-once 优先，代价是可能重复处理）；
- handler 成功后 `MarkDone`（Done 标记保留 TTL）；失败后**仅 `DedupAcquired` 时 `Release`**（InProgress 不释放，避免误删其他处理者的占位）；
- `MarkDone` / `Release` 使用不随停机取消的 context（`context.WithoutCancel`），`Stop` 超时或 parent ctx 取消时也能完成。

只有 `DedupDone` 会被跳过：消费路径自动 `XACK` 并跳过，retry 路径删除对应重试任务并跳过，两者都会触发可选接口 `DuplicateObserver.OnDuplicate`。

```go
dedup := eventbus.NewRedisDedupStore(client, eventbus.RedisDedupStoreConfig{
	Namespace: "demo", // key 前缀默认 "<Namespace>:dedup:"，Namespace 默认 "eventbus"
	// Prefix: "custom:dedup:", // 显式 Prefix 优先于 Namespace
})

bus := eventbus.NewBus(rt,
	eventbus.WithDedup(dedup, eventbus.DedupConfig{
		TTL: 24 * time.Hour, // 默认 eventbus.DefaultDedupTTL（24h）
		// KeyFunc 可选，默认 topic|group|messageID（使用 msg.ConsumerGroup）。
		// 自定义 KeyFunc 必须保证 key 非空且唯一：返回空字符串时该消息不做去重（fail-open）。
		KeyFunc: func(msg eventbus.Message) string {
			if k := msg.Headers["dedup_key"]; k != "" {
				return k
			}
			return msg.Topic + "|" + msg.ConsumerGroup + "|" + msg.ID
		},
	}),
)
```

### 去重前提

- 去重依赖**非空 `Message.ID`**：`Publish` 默认生成雪花 ID，也可用 `eventbus.WithMessageID` 显式指定；从 stream 外直接注入、或 v0.1.1 遗留的重试任务若没有 `Message.ID`，则**不做去重**（直接处理）。
- 默认去重键由 `topic|group|messageID` 组成（`group` 取 `msg.ConsumerGroup`）；发布端相同消息在不同消费组中不互相干扰。
- **`KeyFunc` 返回空字符串时该消息不做去重**（fail-open）：总线直接执行 handler，并通过 `ErrorObserver` 上报 `op=dedup_key` 的错误。自定义 `KeyFunc` 必须保证非空；需要业务级幂等（如按订单号）时可在 header 中携带 `dedup_key`，与 [契约建议（header）](#契约建议header) 配合使用。
- 自定义 `DedupStore` 必须实现两阶段语义：使用根包导出的 `eventbus.DedupStatus` 三态，占位存在但未完成时返回 `eventbus.DedupInProgress`（不要返回 Done），`MarkDone` / `Release` 应幂等。

### 处理流程与语义

- 消费（首次投递 / pending 认领）与 retry worker 路径都会先去重。
- `DedupAcquired`：首次占位成功，执行 handler。成功后 `MarkDone`（把键标记为 Done，TTL 从此刻重新计算）；失败（含 panic / handler 超时）时 `Release` 占位，允许重试继续处理。`Release` 只释放未完成占位，不会删除 `done` 标记（done 是终态，保留到 TTL 到期；并发失败方不会把其他实例已成功的标记清掉）。
- `DedupInProgress`：占位存在但未完成（其他实例处理中、上次处理崩溃 / 释放失败残留）。总线**继续执行 handler**：成功则 `MarkDone`；失败则**不 Release**（占位可能属于其他处理者，重试时仍是 InProgress，会再次处理）。因此可能重复执行 handler，但不会因为占位残留而跳过消息。
- `DedupDone`：去重命中，消费路径自动 `XACK` 并跳过，retry 路径删除对应重试任务并跳过，并触发 `DuplicateObserver.OnDuplicate`。
- **fail-open**：`Acquire` / `MarkDone` / `Release` 出错时不会阻塞消息处理——继续执行 handler，并通过 `ErrorObserver` 上报 `dedup_acquire` / `dedup_mark_done` / `dedup_release`（注入 `WithLogger` 时同时记日志）。此时等价于未启用去重。
- `RedisDedupStore`：`Acquire` 用 `SET key "pending" NX PX ttl` 占位；占位已存在时 `GET` 读取状态，值为 `"done"` 返回 Done，否则返回 InProgress（键在 `SETNX` 与 `GET` 之间过期会重试一次占位）；`MarkDone` 用 `SET key "done" PX ttl`；`Release` 用 Lua 脚本**仅删除值为 `"pending"` 的占位**（已完成 `done` 标记不会被并发失败方清除）。key 为 `<namespace>:dedup:<key>`，TTL<=0 时回退到 `DefaultDedupTTL`。client 生命周期由调用方管理。
- 去重实例是 Bus 级别的（`WithDedup` 为 `NewBus` 选项），对通过该 Bus 的所有订阅生效；键里包含 group，因此不会互相污染。

### 边界

- TTL 只决定 **Done 标记**保留多久：Done 未过期时重复消息被跳过；Done 过期后同一条消息会再次处理。InProgress 占位同样按 TTL 过期，过期后可重新占位。
- 去重**不承诺 exactly-once**：两个实例并发 `Acquire` 时只有一个拿到 Acquired，另一个是 InProgress 并同样执行 handler；崩溃残留的 InProgress 也会被再次处理——这些场景都会重复执行 handler（at-least-once）。
- 设计以「不丢消息」优先：崩溃或 `Release` 失败留下的占位**不会**被当作 Done 跳过；如果希望减少重复，可缩短 `TTL` 并保证 handler 幂等。
- 并发约束：`done` 是终态，不会被 `Release` 清除；若两实例并发处理同一条消息，晚到的失败方 `Release` 也只清理 `pending`，不会让已完成的消息重新变成可处理。
- 去重存储故障 / `KeyFunc` 返回空时按 fail-open 降级为不去重，不能依赖它做资金、库存等强一致去重。
- 自定义 `DedupStore` 若把「未完成占位」错误地报告为 Done，会导致消息被跳过而丢失——实现时必须遵守三态语义。

## 消费初始 offset（StartFrom）

`ConsumerConfig.StartFrom` 控制**新建消费组**的初始 offset。默认值保持 v0.2 行为：从头消费。

| 取值 | 含义 | 归一化 |
| --- | --- | --- |
| `""` / `"0"`（默认） | 从头消费该 stream 的现有消息 | `"0"` |
| `"$"` | 只消费订阅之后的新消息 | `"$"` |
| unix 毫秒时间戳，如 `"1700000000000"` | 从该时间点之后开始 | 自动补为 `"1700000000000-0"` |
| stream ID，如 `"1700000000000-0"` | 高级用法：直接指定 `XGROUP CREATE` 的 offset | 原样透传 |

常量：`eventbus.StartFromBeginning` = `"0"`、`eventbus.StartFromLatest` = `"$"`。

```go
if err := bus.Subscribe(eventbus.ConsumerConfig{
	Topic: "order.created",
	Group: "order-worker-v2",
	// 仅对「新建组」生效：只消费订阅之后的新消息。
	// 已存在的消费组不会被重置（Redis BUSYGROUP 被忽略）。
	StartFrom: eventbus.StartFromLatest,
	Handler:   handleOrderCreated,
}); err != nil {
	if errors.Is(err, eventbus.ErrStartFromUnsupported) {
		// Runtime 未实现 ConsumerGroupInitializer（RedisRuntime 已实现）。
	}
	log.Fatal(err)
}
```

- 规则：
  - 取值在 `Subscribe` 时校验；非法值返回 `eventbus.ErrInvalidStartFrom`（纯数字时间戳少于 10 位也视为非法）。
  - 非默认值（`"$"`、时间戳、stream ID）要求 Runtime 实现可选接口 `ConsumerGroupInitializer`；未实现时 `Subscribe` 返回 `eventbus.ErrStartFromUnsupported`，`Start` 时也会再次检查。
  - **只对新建组生效**：底层 `XGROUP CREATE` 只会在组不存在时应用 offset，已存在的组不受影响。要重置已有组的 offset，需要在 Redis 侧手动删除消费组（本库不提供该操作）。
  - 时间戳按 Redis stream ID 语义取「该毫秒之后」；用 `time.Time.UnixMilli()` 生成即可，例如 `strconv.FormatInt(time.Now().Add(-time.Hour).UnixMilli(), 10)`。
- `RedisRuntime` 已实现 `ConsumerGroupInitializer`（`EnsureConsumerGroupFrom`），无需额外配置。

## StreamMaxLen 与 PEL 孤儿清理

`RedisRuntimeConfig.StreamMaxLen > 0` 时，`Publish` 走 `XADD MAXLEN`，把 stream 长度限制在给定值，避免无限增长。裁剪会从 stream 中删除旧条目，但消费者 PEL 里可能仍保留这些条目的 ID：它们已经无法再被消费，称为 **PEL 孤儿**。

- **Redis 7+**：`XAUTOCLAIM` 自身会移除被 `XTRIM` / `StreamMaxLen` 裁剪掉的 PEL 孤儿，并在回复的第三个元素中返回这些 deleted IDs。`RedisRuntime.ClaimPending` 直接解析 raw 回复（go-redis 的 `XAutoClaimCmd` 会丢弃第三个元素），对这些 ID 再执行一次 `XACK` 作为**幂等防御**：正常情况下 Redis 已移除这些条目，`XACK` 是无害 no-op；若仍残留则补一次清理。清理错误会通过 `ErrorObserver` 的 `op=reclaim` 上报。
- **Redis 6.2**：`XAUTOCLAIM` 回复只有两个元素，没有 deleted IDs，总线**无法感知也无法清理**孤儿；这些 PEL 条目会一直存在（`XPENDING` 计数虚高，但不会再投递）。

### 使用建议

- 同时使用 `StreamMaxLen` 与 `PendingReclaim` 时，优先使用 **Redis 7+**，让 PEL 孤儿由 `XAUTOCLAIM` 自动移除。
- 若只能使用 Redis 6.2：避免对启用 reclaim 的 stream 做激进裁剪（把 `StreamMaxLen` 设得足够大，或先不裁剪），并定期用 `XPENDING` 结合 `XRANGE` 人工识别/`XACK` 孤儿；长期建议升级 Redis。
- `StreamMaxLen` 不能设置得过小：未 `XACK` 且仍在处理中的消息可能被裁剪，之后即使 handler 失败也无法重放（at-least-once 语义被破坏）。请保证 `StreamMaxLen` 覆盖业务高峰期的在途消息量，并与 `MinIdle` / 消费速度一起评估。
- 不设置 `StreamMaxLen` 时不会产生 trim 导致的孤儿，但 stream 会持续增长，需要业务侧自行规划裁剪/归档。

## 顺序与 partition

- 同一 topic 对应单个 Redis stream，多个消费者/多实例并发处理时**不保证顺序**；同一消费者的一批消息按 stream 顺序串行处理，但跨实例、跨重启不保证。
- 需要严格顺序时，最简单的方案是「**单消费者 + 单消费组**」：同一 `(topic, group)` 只由一个消费者实例处理（不扩容），或按业务键串行化。代价是吞吐受限、故障切换需要人工/外部协调。
- 需要更高吞吐又要求同键有序时，业务侧可以做 **partition**：按聚合根 ID 哈希到多个 topic/stream，每个 partition 使用独立消费组与单消费者。库不内置 partition 能力，需要业务层自行实现。

partition 模式示意（库不提供内置实现）：

```go
// 发布端：同一 aggregate_id 必须始终映射到同一个 partition topic。
func partitionTopic(aggregateID string, partitions int) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(aggregateID))
	return fmt.Sprintf("order.events.p%d", int(h.Sum32())%partitions)
}

// 消费端：每个 partition 一个订阅，每个 (topic, group) 只由一个消费者实例处理。
for i := 0; i < partitions; i++ {
	topic := fmt.Sprintf("order.events.p%d", i)
	if err := bus.Subscribe(eventbus.ConsumerConfig{
		Topic: topic,
		Group: "order-worker-" + strconv.Itoa(i),
		// 同一个 partition 内保持单消费者，才能保证同聚合根有序。
		Handler: handleOrderEvent,
	}); err != nil {
		log.Fatal(err)
	}
}
```

> partition 的扩容/再均衡、consumer 与 partition 的分配、以及「单消费者」约束都需要业务侧保证；新增消费者实例时如果分配到相同 partition，会破坏顺序。

## 契约建议（header）

`Message.Headers` 在发布、消费、重试、死信之间**原样透传**（`WithHeaders` 会复制 map，重试/死信任务保存 header）。库不解析 header 语义，也不会修改 payload；以下键属于「约定」，由平台层/业务层共同治理。

| 键 | 归属 | 说明 |
| --- | --- | --- |
| `message_id` | 库保留（消息标识） | 消息唯一标识，由库在 `Publish` 时生成（雪花 ID），通过 `Message.ID` / `RetryTask.MessageID` / `DeadLetterMessage.MessageID` 暴露。需要显式指定时用 `eventbus.WithMessageID`，不要在 header 里再定义一份。 |
| `traceparent` / `tracestate` / `baggage` | 基础设施（OpenTelemetry） | W3C 传播字段，由 `otel` 子模块注入/提取；业务不要手工写入或覆盖。 |
| `event_type` | 业务推荐 | 事件类型（如 `order.created`）。库不校验，建议与 topic 对应关系写入平台契约。 |
| `event_version` | 业务推荐 | 事件 schema 版本，便于消费端做多版本兼容。 |
| `aggregate_id` | 业务推荐 | 聚合根/业务实体 ID，用于幂等、审计与 partition 哈希。 |
| `dedup_key` | 业务推荐（可选） | 业务幂等键；可在自定义 `DedupConfig.KeyFunc` 中使用，见 [消费端幂等去重](#消费端幂等去重)。 |

- **库不解析、不修改 payload**：payload 只做编解码（默认 JSON；`WithCodec` 可替换）与字节透传，schema 演进、envelope 规范、schema registry、跨版本兼容策略都属于平台层/业务层，不在本库范围内。
- header 只支持 `map[string]string`；值会随消息写入 Redis stream，注意大小与敏感信息（不要把 token/密钥写进 header）。
- 发布示例：

```go
err := bus.Publish(ctx, "order.created", payload,
	eventbus.WithHeaders(map[string]string{
		"event_type":    "order.created",
		"event_version": "1",
		"aggregate_id":  orderID,
		"dedup_key":     orderID + "|order.created|v1",
	}),
)
```

### OpenTelemetry 子模块

`github.com/modern-magic-go/eventbus/otel`（独立 module）负责 traceparent 的注入/提取与 span 生成：

```go
import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	eventbusotel "github.com/modern-magic-go/eventbus/otel"
)

// 必须在进程启动时配置一次：OTel Go 的全局 propagator 默认是空实现，
// 不设置就不会注入/提取 traceparent。
otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
	propagation.TraceContext{}, propagation.Baggage{},
))
// Publish 使用全局 TracerProvider；如需自定义，调用 otel.SetTracerProvider(tp)。

// 发布：创建 producer span（SpanKindProducer），自动注入 traceparent，业务 header 原样保留。
err := eventbusotel.Publish(ctx, bus, "order.created", payload, map[string]string{
	"event_type":    "order.created",
	"event_version": "1",
	"aggregate_id":  orderID,
})

// 消费：从 msg.Headers 恢复上游 trace context，创建 consumer span 后再调用业务 handler。
bus.Subscribe(eventbus.ConsumerConfig{
	Topic: "order.created",
	Group: "order-worker",
	Handler: eventbusotel.WrapHandler(handleOrderCreated,
		eventbusotel.WithTracerProvider(tp),
		eventbusotel.WithPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{},
		)),
	),
})
```

- producer span 默认名 `eventbus.publish <topic>`，consumer span 默认名 `eventbus.consume <topic>`；属性包含 `messaging.system=redis`、`messaging.destination.name`，consumer 额外含 `messaging.consumer.group.name` / `messaging.message.id`；handler 返回错误会 `RecordError` 并置 span status 为 Error。
- Options：`WithTracerProvider`、`WithPropagator`、`WithSpanNameFormatter`。
- `Publish` 的 `headers` 参数优先于 `opts` 中的 `eventbus.WithHeaders`（内部在 opts 之后合并），业务 header 通过该参数传入。

## 与业务事务和 outbox

本库**不提供** `PublishInTx` 或 outbox 实现：`Publish` 即 `XADD`，不是数据库事务的一部分；如果业务事务回滚，已经发布的事件会变成**幽灵事件**（下游看到一条实际未发生的事件）。反过来，如果先提交事务再发布，发布失败会**丢事件**。生产环境建议使用 **transactional outbox** 模式，属于业务侧/平台侧实现。

推荐模式：

1. 业务事务内，把「要发布的事件」写入业务库的 **outbox 表**（与业务数据同库、同事务），事务提交即保证事件与业务数据一致；
2. 后台 relay 任务轮询未发送的 outbox 记录，调用 `bus.Publish`（或直接 `XADD` 到 Redis stream）；
3. 发布成功后标记 `sent_at`（失败的记录累加 `attempts`，按退避重试）；
4. 配合 `Message.ID` / `dedup_key` header 做幂等；at-least-once 下消费者仍需幂等，可直接使用本版本的 `WithDedup`。

outbox 表字段建议（以 PostgreSQL 为例，仅示意）：

```sql
CREATE TABLE event_outbox (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic       TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    headers     JSONB       NOT NULL DEFAULT '{}',
    message_id  TEXT        NOT NULL, -- 发布时通过 WithMessageID 指定，重发保持稳定
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at     TIMESTAMPTZ,
    attempts    INT         NOT NULL DEFAULT 0
);

-- relay 任务只取未发送记录；建议按 id 分批、加 SKIP LOCKED 支持多实例。
SELECT id, topic, payload, headers, message_id
FROM event_outbox
WHERE sent_at IS NULL AND attempts < 10
ORDER BY id
LIMIT 100
FOR UPDATE SKIP LOCKED;
```

relay 伪代码：

```go
for _, row := range fetchUnsent(ctx, 100) {
	err := bus.Publish(ctx, row.Topic, row.Payload,
		eventbus.WithMessageID(row.MessageID), // 建议 outbox 表持久化 message_id，重发时保持不变
		eventbus.WithHeaders(row.Headers),
	)
	if err != nil {
		markAttemptFailed(ctx, row.ID) // attempts+1，保留待重试
		continue
	}
	markSent(ctx, row.ID) // sent_at=now()
}
```

- outbox relay 自身是 at-least-once：发布成功但标记 `sent_at` 前崩溃会重发，用 `message_id` + `dedup_key` + 消费端 `WithDedup` 兜住重复。
- 清理策略由业务侧决定（例如按 `created_at` 定期归档/删除已发送记录）；本库不管理 outbox。
- 已知限制表中保留「outbox 需业务侧实现」一项。

## 可观测性

默认情况下 `NewBus` **不输出任何日志（零噪音）**，也不上报指标；内部错误只在注入观测组件后可见。生产环境建议至少注入 `NewSlogObserver` 或 `WithLogger` 之一，并接入 Prometheus。

### Observer

`Observer` 是事件总线的生命周期观测扩展点，实现该接口即可接入发布、消费、重试、死信全链路事件。

```go
type Observer interface {
    OnPublished(ctx context.Context, msg Message)
    OnPublishFailed(ctx context.Context, msg Message, cause error)
    OnHandled(ctx context.Context, msg Message)
    OnHandleFailed(ctx context.Context, msg Message, cause error)
    OnRetryScheduled(ctx context.Context, task RetryTask, cause error)
    OnDeadLettered(ctx context.Context, msg DeadLetterMessage)
}
```

### ErrorEvent 与 ErrorObserver

`ErrorObserver` 是**可选扩展接口**：实现后总线会把原本静默的内部错误回调给它。老的自定义 `Observer` **不需要实现该接口**，不受影响（非破坏性）。

```go
type ErrorEvent struct {
    Op        string // 出错操作名，见下表
    Topic     string
    Group     string
    MessageID string
    Err       error
}

type ErrorObserver interface {
    OnError(ctx context.Context, event ErrorEvent)
}
```

| Op | 触发场景 |
| --- | --- |
| `publish` | 发布失败（同时会触发 `OnPublishFailed`） |
| `consume` | `XREADGROUP` 拉取失败（200ms 后重试） |
| `reclaim` | `XAUTOCLAIM` 认领失败 / PEL 孤儿清理失败 |
| `ack` | `XACK` 失败 |
| `handle_failure` | 首次投递/认领路径中失败处理失败（写重试/死信或随后的 `XACK` 出错） |
| `schedule_retry` | retry worker 重新调度、死信重放入队失败 |
| `remove_retry` | 删除重试任务失败 |
| `move_dead_letter` | 写死信失败（含重放放回失败） |
| `poll_retry` | 轮询重试队列失败 |
| `dedup_key` | `KeyFunc` 返回空 key，该消息不做去重（fail-open，继续处理） |
| `dedup_acquire` | 去重存储占位失败（fail-open，继续处理） |
| `dedup_mark_done` | 去重 Done 标记写入失败（fail-open，继续处理） |
| `dedup_release` | 去重存储释放失败（fail-open，继续处理） |

`DuplicateObserver` 也是可选扩展：去重命中时回调 `OnDuplicate(ctx, msg)`。

### WithLogger 与 NewSlogObserver

两种接入方式（均为标准库 `log/slog`，零第三方依赖）：

```go
// 方式一：仅给总线内部错误配 logger（不增加 Observer 开销/事件）。
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
bus := eventbus.NewBus(rt, eventbus.WithLogger(logger))

// 方式二：用内置 slog Observer 记录生命周期事件 + 内部错误 + 去重命中。
bus = eventbus.NewBus(rt,
	eventbus.WithObserver(eventbus.NewSlogObserver(logger)),
)
```

- `WithLogger`：只输出总线内部错误（`consume` / `reclaim` / `ack` / 重试/死信存储错误等），不记录发布/消费成功事件；未注入时完全静默，错误仍可通过 `ErrorObserver` 观测。
- `NewSlogObserver(logger)`：返回实现 `Observer` + `ErrorObserver` + `DuplicateObserver` 的观测器；logger 为 nil 时使用 `slog.Default()`。日志级别约定：Debug（published/handled/duplicate）、Warn（handle failed/retry scheduled）、Error（publish failed/dead lettered/内部错误）。
- 两种方式同时使用会让内部错误记录两次（`OnError` 一次 + `WithLogger` 一次），按需二选一。

### Prometheus 子模块

`github.com/modern-magic-go/eventbus/prometheus`（独立 module，依赖 prometheus/client_golang）提供两部分能力：

```go
import (
	eventbusprom "github.com/modern-magic-go/eventbus/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

reg := prometheus.NewRegistry()

// Observer：实现 Observer / ErrorObserver / DuplicateObserver，把总线事件转成计数器。
obs := eventbusprom.NewObserver(reg,
	eventbusprom.WithConstLabels(prometheus.Labels{"service": "order"}),
)

bus := eventbus.NewBus(rt, eventbus.WithObserver(obs))

// Collector：实现 prometheus.Collector，从 Runtime 的 RuntimeStats 采集 gauge。
collector := eventbusprom.NewCollector(rt, []eventbusprom.Subscription{
	{Topic: "order.created", Group: "order-worker"},
}, eventbusprom.WithCollectTimeout(5*time.Second))
reg.MustRegister(collector)

http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
```

指标清单：

| 指标 | 类型 | Label | 说明 |
| --- | --- | --- | --- |
| `eventbus_published_total` | counter | `topic,status` | 发布结果（`success` / `failed`） |
| `eventbus_handled_total` | counter | `topic,status` | 处理结果（`success` / `failed`） |
| `eventbus_retry_scheduled_total` | counter | `topic` | 进入重试队列的次数 |
| `eventbus_dead_lettered_total` | counter | `topic` | 进入死信队列的次数 |
| `eventbus_errors_total` | counter | `op` | 总线内部错误次数（op 见 `ErrorEvent`） |
| `eventbus_duplicates_total` | counter | `topic` | 去重命中、被跳过的消息数 |
| `eventbus_stream_lag` | gauge | `topic,group` | 消费组待消费消息数 |
| `eventbus_pending_messages` | gauge | `topic,group` | 消费组 PEL 中的消息数 |
| `eventbus_retry_queue_length` | gauge | — | 重试队列任务数 |
| `eventbus_dead_letter_length` | gauge | — | 死信队列消息数 |
| `eventbus_collect_errors_total` | counter | `op` | Runtime 指标采集失败次数 |

- `Subscription` 的 `(Topic, Group)` 必须与 `Bus.Subscribe` 一致，否则采到的是不存在/已废弃消费组的数据（通常为 0 或报错）。
- 每次 `Collect` 使用带超时的 context 调用 `RuntimeStats`（默认 5s，`WithCollectTimeout` 可调）；单次 stats 调用失败时跳过对应指标并把 `eventbus_collect_errors_total{op}` 加一，不 panic。
- Runtime 未实现 `RuntimeStats`（或传入 nil）时，所有统计 gauge 输出 `UnavailableValue`（`-1`），并记录 `eventbus_collect_errors_total{op="runtime_stats"}`；未实现 `DeadLetterLen` 时死信长度输出 `-1`，不计为采集错误。
- 统计类 gauge 不可用值统一为 `-1`，便于区分「值为 0」与「采不到」。

### 依赖说明

`internal/core` 零第三方依赖（只用标准库）；`NewSlogObserver` / `WithLogger` 使用标准库 `log/slog`，观测能力本身也不引入第三方库。但**根包并非零依赖**：根 module 自 v0.2 起依赖 go-redis（`redis.go` 暴露 `RedisRuntime`），测试依赖 miniredis / testify，本次未新增。Prometheus 与 OpenTelemetry 是**可选嵌套 module**，只有显式导入才会引入各自依赖。

## 优雅停机

```go
// 默认 30s 超时
_ = bus.Stop()

// 自定义超时
err := bus.StopWithTimeout(10 * time.Second)
if err != nil {
	if errors.Is(err, eventbus.ErrStopTimeout) {
		// 在途 handler 未在超时内完成：总线已停止拉取，本次不再等待。
		// 注入的 Runtime 不会被关闭，由调用方管理（避免 handler 使用已关闭连接）。
		log.Printf("graceful stop timeout: %v", err)
	} else {
		log.Printf("stop error: %v", err)
	}
}
```

停机语义（v0.3 起）：

1. **先停止拉取新消息**（取消 poll ctx），但当前批次已取到的消息会继续处理完（drain）；
2. **等待所有 worker 退出**，包括 retry worker 正在执行的 handler；等待期间 handler ctx 仍然有效；
3. 全部退出后取消 handler ctx，若 `WithCloseRuntime(true)` 则关闭注入的 Runtime；
4. 等待超过 `timeout` 时**立即取消在途 handler** 并返回 `eventbus.ErrStopTimeout`（v0.2 返回 nil），且**不关闭 Runtime**：worker 可能仍在收尾，调用方应自行决定 Runtime 生命周期（建议等待相关 handler 返回后再关闭客户端）；
5. **parent ctx 取消**（传入 `Start` 的 ctx）会立即取消所有 worker 与在途 handler，不进入 drain；适用于进程强制退出；
6. 未调用 `Start` 或已停止时调用 `Stop` 返回 nil。

补充：

- 超时后 `Bus` 不再等待剩余 goroutine；如果 handler 忽略 ctx，它可能继续运行。请确保 handler 尊重 ctx，或在调用方用 `sync.WaitGroup` 等机制管理自己的长任务。
- 停机时已 fetch 但尚未处理的消息若没有 `XACK` 会留在 PEL；开启 `PendingReclaim` 可在重启后重投。

## Runtime 生命周期

默认 `Stop()` 不会关闭注入的 `Runtime`（如 Redis 客户端），以便调用方复用连接。
如需总线负责完整生命周期：

```go
bus := eventbus.NewBus(rt, eventbus.WithCloseRuntime(true))
```

- `CloseRuntime=true` 时，Runtime 只在 **worker 全部退出后**（正常停机路径）关闭；`StopWithTimeout` 超时返回 `ErrStopTimeout` 时不会关闭，避免在途 handler 使用已关闭连接。
- 即使开启 `CloseRuntime`，也建议调用方等待 `StopWithTimeout` 返回成功后再释放自己持有的 client；超时分支由调用方自行管理。

## 兼容性与升级

从 v0.2.x 升级到 v0.3.x 的要点（新增 API 均向后兼容，行为变化已标注）：

- **`StopWithTimeout` 超时返回值变化（行为变更）**：等待在途 handler 超时现在返回 `ErrStopTimeout`（v0.2 返回 nil），且超时后不再关闭 Runtime。忽略返回值的调用方功能上不受影响，但若原实现依赖「超时后 Runtime 已关闭」需调整。
- **停机更「慢」但更安全（行为变更）**：`Stop` 现在先停止拉取再 drain 在途 handler，可能比 v0.2 更晚返回（最长到超时时间）；v0.2 是取消后立即返回。parent ctx 取消仍立即生效。
- **退避加入 jitter 与上限（行为变更）**：名义延迟 `base × 2^(attempt-1)` 仍保留，但实际延迟为 `[delay/2, delay]` 随机且被 `RetryMaxDelay`（默认 5m）封顶，因此单次重试可能比 v0.2 更早发生，长期失败不会无限翻倍。
- **重试存储格式变化（自动兼容）**：重试任务从「ZSET member 直接存任务 JSON」变为「ZSET member 索引 + `<ns>:retry:data` HASH payload」，同一 `MessageID` 幂等覆盖。v0.2 旧 member 仍可读取，重新调度时自动原子迁移；v0.1.1 无 `message_id` 的旧任务仍走旧语义，处理完后即消失。新增配置 `RedisRuntimeConfig.RetryDataKey`。
- **`DeadLetterMessage.ID` 新增**：死信记录获得自身 ID；v0.2 旧 DLQ 条目读出来 `ID` 为空，重放/重新写入时由 Runtime 重新生成。
- **内部错误不再静默（行为增强）**：新增可选 `ErrorObserver`、`WithLogger`、`NewSlogObserver`；老 `Observer` 无需改动。
- **新增可选接口**：`ConsumerGroupInitializer`（`StartFrom`）、`DeadLetterStore`（死信查询/重放）、`RuntimeStats`（指标）；自定义 Runtime 不实现时仅在请求对应能力时报错（`ErrStartFromUnsupported` / `ErrDeadLetterStoreUnsupported`），其余功能不受影响。
- **去重改为两阶段（接口变化）**：`DedupStore` 由「占位 + 释放」变为「`Acquire` → `MarkDone` / `Release`」，`Acquire` 返回 `DedupAcquired` / `DedupDone` / `DedupInProgress` 三态；只有 Done 会跳过，InProgress 会继续执行 handler（at-least-once，避免崩溃/释放失败导致丢消息）。去重能力本身在 v0.2 不存在，因此对 v0.2 升级用户无破坏；自研 `DedupStore` 实现需按三态语义适配。该版本同时修复了「Release 失败或停机取消 ctx 导致重试被误判重复而丢消息」的问题（`MarkDone` / `Release` 使用 `context.WithoutCancel`）。
- **重放与重试任务变化**：`ReplayDeadLetters` 重放写入的重试任务使用独立 `RetryTask.Key`（`key:` 前缀 member），避免与 retry worker 正在清理的旧任务 member 冲突；`ReplayResult.Errors` 均为带 `topic` / `group` / `message_id` / `dlq_id` 上下文的 wrapped error；`PopDeadLetters` 部分弹出失败时仍处理已弹出记录，不再整批丢弃。
- **重试坏数据清理**：`PollRetry` 会主动清理无法反序列化的重试 member（新格式同时清理 HASH payload），避免坏数据永久占据队首阻塞合法任务。
- **新增嵌套 module**：`otel/`、`prometheus/` 各自独立 `go.mod`（tag 形如 `otel/v0.3.0`、`prometheus/v0.3.0`，require 根 module v0.3.0），根 module 依赖集合不变（本次未新增）；不导入即不引入 OTel/Prometheus 依赖。`examples/basic` 仍只依赖根 module 与 go-redis。
- **默认行为保持**：`StartFrom` 默认从头、`HandlerTimeout` 默认 0（不限）、去重默认关闭、`CloseRuntime` 默认 false，均与 v0.2 一致。
- Redis 基础能力要求与 v0.2 相同（reclaim 需 Redis 6.2+）；同时使用 `StreamMaxLen` 与 reclaim 时建议 Redis 7+（由 `XAUTOCLAIM` 自动移除 PEL 孤儿）。

## 核心边界

- 核心 API 不暴露 ORM 或具体业务模型
- 事务提交后发布这类语义建议在业务项目侧适配（outbox，见 [与业务事务和 outbox](#与业务事务和-outbox)）
- 运行时通过 `Runtime` 接口解耦，可替换 Redis 实现
- 不内置 outbox、partition、schema 治理；不承诺 exactly-once / 全局有序
- 去重、死信查询/重放、初始 offset、运行指标均为**可选能力**，依赖 Runtime 实现对应可选接口

## 已知限制与后续计划

以下限制对应 [issue #1](https://github.com/modern-magic-go/eventbus/issues/1)、[issue #3](https://github.com/modern-magic-go/eventbus/issues/3)、[issue #4](https://github.com/modern-magic-go/eventbus/issues/4) 中尚未解决或本版本明确不做的边界，不代表当前版本承诺：

| 限制 | 说明 | 对应 |
| --- | --- | --- |
| 未启用 PendingReclaim 时 PEL 不自动恢复 | 消费者崩溃/停机后 pending 消息会一直留在 PEL，不会自动重投；需开启 `PendingReclaim` 或人工 `XCLAIM` | issue #1 边界 · P0-1 |
| outbox 需业务侧实现 | 库不提供 `PublishInTx`/outbox；`Publish` 即 `XADD`，事务回滚会产生幽灵事件。需按 [与业务事务和 outbox](#与业务事务和-outbox) 在业务库实现 outbox + relay | issue #1 · P1-5 |
| 无 partition key / 全局顺序保证 | 单 topic 单 stream，多消费者并发处理时不保证同一聚合根的事件顺序；库不内置 partition，顺序敏感场景需单消费者单组或业务侧按聚合根哈希分 topic | issue #1 · P1-6 |
| 同实例慢 handler 可能重复处理 | `XREADGROUP` 批量预取，`MinIdle` 小于批处理总耗时时，未处理完的消息会被同一实例的 reclaim goroutine 认领；建议 `PollBatchSize=1` 或按批耗时设置 `MinIdle` | issue #1 · P1-2 边界 |
| 多服务共享 namespace 可能误删重试任务 | retry worker 对「本进程无订阅」的 `(topic, group)` 任务会执行 `RemoveRetry`（v0.2 起既有行为）；多个服务共用同一 namespace / 重试队列时，可能删掉其他服务的重试任务。建议一个 namespace 只由一个服务的 Bus 管理 | 本版本边界 |
| 重试任务非 exactly-once | 重试任务执行成功后、`RemoveRetry` 前崩溃会再次执行 handler；无 `MessageID` 的旧任务仍可能先写后删产生重复任务（handler 需幂等） | 本版本边界 |
| Redis 6.2 无法清理 PEL 孤儿 | 与 `StreamMaxLen` 组合时，已从 stream 裁剪的消息会留在 PEL；Redis 7 的 `XAUTOCLAIM` 会自动移除，本实现解析 deleted IDs 并幂等 `XACK`；Redis 6.2 不返回 deleted IDs、无法清理，需升级 Redis 7+ 或人工处理 | issue #4 · 4-2 |
| 去重 InProgress 会重复执行 handler | `Acquire` 得到 `DedupInProgress`（其他实例处理中 / 上次崩溃或释放失败残留）时总线会继续执行 handler；两实例并发 `Acquire` 也可能同时处理。去重只保证 Done 后跳过，不承诺 exactly-once；好处是崩溃/Release 失败不会丢消息 | 本版本边界 |
| 去重 fail-open 与 TTL 语义 | 去重存储故障或 `KeyFunc` 返回空 key 时降级为不去重并继续处理；TTL 只决定 Done 标记保留时长，过期后会再次处理 | 本版本边界 |
| 死信重放非事务 | `PopDeadLetters` 弹出后、重新入队前进程崩溃仍可能丢消息（部分弹出失败已保证已弹出记录会被处理）；并发重放无互斥；DLQ 无 TTL/裁剪，Redis flush/evict 即丢失 | 本版本边界 |
| Stop 超时后 Runtime 由调用方管理 | 等待在途 handler 超时返回 `ErrStopTimeout` 后，总线不再关闭 Runtime；worker 可能仍在收尾，调用方需自行管理连接与退出顺序 | 本版本边界 |
| Stop 超时后 handler 的 Ack 可能失败 | `StopWithTimeout` 超时取消 handler ctx 后，总线随后的 `XACK` 可能因 ctx 已取消而失败，消息留在 PEL，由 reclaim 兜底（预期副作用） | 本版本边界 |
| handler 不尊重 ctx 时 HandlerTimeout 无效 | `HandlerTimeout` 只设置 ctx deadline，不能强杀 goroutine；handler 必须把 ctx 传给下游并响应取消 | 本版本边界 |
| 重试路径不触发 OnHandleFailed | retry worker 中 handler 失败（含 panic）是基线行为：`OnHandleFailed` 只在首次投递与 reclaim 路径触发，重试路径通过 `OnRetryScheduled` / `OnDeadLettered` 观测 | issue #1 基线行为 |
| 老版本数据兼容限制 | v0.2 的 DLQ 条目无 `ID`（重放时重新生成）；v0.1.1 已入队且无 `message_id` 的 `RetryTask` 升级后 `Message.ID` 为空、无法参与去重/幂等覆盖，旧任务消费完后恢复 | 升级注记 |
| 无 schema / envelope 治理 | 库只做 header 透传，不解析 payload、不校验 schema；事件版本兼容、schema registry、envelope 规范由平台层负责 | issue #3 |

后续版本计划按 issue 的优先级推进；outbox、partition、schema 治理等不在当前版本承诺范围内。

## 主要类型与选项

### 核心

- `Bus`：事件总线入口（`Publish` / `Subscribe` / `Start` / `Stop` / `StopWithTimeout` / `DeadLetters` / `ReplayDeadLetters`）
- `Runtime`：底层运行时抽象
- `RedisRuntime` / `RedisRuntimeConfig`：默认 Redis 实现与配置（`Namespace`、`StreamMaxLen`、`RetryQueueKey`、`RetryDataKey`、`DeadLetterKey`、`ConsumerName` 等）
- `ConsumerConfig`：订阅/消费配置（`MaxRetries`、`PollBatchSize`、`PollBlock`、`RetryBaseDelay`、`RetryMaxDelay`、`HandlerTimeout`、`StartFrom`、`PendingReclaim`、`ConsumerName`）
- `PendingReclaimConfig`：pending 消息回收配置（`MinIdle` / `Batch` / `Interval`）
- `Message`：`ID` / `Topic` / `Payload` / `Headers` / `PublishedAt` / `ConsumerGroup` / `Attempt` / `MaxRetries` / `DeliveryTag`
- `Handler`：`func(ctx context.Context, msg Message) error`
- `Codec`：payload 编解码抽象（默认 JSON）
- `PublishOption`：`WithMessageID`、`WithHeaders`

### 可选接口（Runtime 能力）

- `PendingReclaimer`：认领空闲 pending 消息（RedisRuntime 已实现，Redis 6.2+）
- `ConsumerGroupInitializer`：按 `StartFrom` 创建消费组（RedisRuntime 已实现）
- `DeadLetterStore`：死信查询/弹出/长度（RedisRuntime 已实现）
- `RuntimeStats`：`StreamLag` / `PendingCount` / `RetryQueueLen`（RedisRuntime 已实现）

### 观测与去重

- `Observer`：发布、处理、重试、死信生命周期钩子
- `ErrorObserver` / `ErrorEvent`：内部错误（可选实现，非破坏）
- `DuplicateObserver`：去重命中回调（可选实现，非破坏）
- `NewSlogObserver`：内置 slog Observer（同时实现 ErrorObserver/DuplicateObserver）
- `DedupStatus`（根包导出）：去重占位三态（`eventbus.DedupAcquired` / `eventbus.DedupDone` / `eventbus.DedupInProgress`；`String()` 返回 `acquired` / `done` / `in_progress`），由 `DedupStore.Acquire` 返回
- `DedupConfig` / `DedupStore` / `WithDedup`：消费端两阶段幂等去重（`Acquire` / `MarkDone` / `Release`）
- `RedisDedupStore` / `RedisDedupStoreConfig` / `NewRedisDedupStore`：Redis 去重存储（占位值 `pending` / `done`）

### 重试与死信

- `RetryTask`：`Key`（可选 member 覆盖，DLQ 重放使用独立 key）/ `MessageID` / `Topic` / `Group` / `Payload` / `Headers` / `Attempt` / `MaxRetries` / `AvailableAt` / `LastError`
- `DeadLetterMessage`：`ID` / `MessageID` / `Topic` / `Group` / `Payload` / `Headers` / `Attempt` / `MaxRetries` / `Error` / `OccurredAt`
- `ReplayResult`：`Replayed` / `Skipped` / `Errors`（pop 中途失败为全局错误，逐条失败为带上下文的 wrapped error）

### 选项与默认值

- `NewBus(rt, opts...)` 选项：`WithCodec`、`WithObserver`、`WithRetryPollInterval`、`WithCloseRuntime`、`WithLogger`、`WithDedup`
- 默认值常量：`DefaultMaxRetries`（3）、`DefaultPollBatchSize`（10）、`DefaultPollBlock`（2s）、`DefaultRetryBaseDelay`（5s）、`DefaultRetryMaxDelay`（5m）、`DefaultReclaimMinIdle`（30s）、`DefaultReclaimInterval`（10s）、`DefaultDedupTTL`（24h）
- 初始 offset 常量：`StartFromBeginning`（`"0"`）、`StartFromLatest`（`"$"`）

### 错误

- `ErrInvalidConsumerConfig`：订阅配置非法
- `ErrDuplicateSubscription`：重复订阅同一 `(topic, group)`
- `ErrHandlerPanic`：handler panic（已 recover）
- `ErrPendingReclaimUnsupported`：Runtime 未实现 `PendingReclaimer` 但启用了回收
- `ErrInvalidStartFrom`：`StartFrom` 非法
- `ErrStartFromUnsupported`：Runtime 未实现 `ConsumerGroupInitializer` 但设置了非默认 `StartFrom`
- `ErrDeadLetterStoreUnsupported`：Runtime 未实现 `DeadLetterStore`
- `ErrStopTimeout`：优雅停机等待在途 handler 超时

以上错误均可用 `errors.Is` 判断。
