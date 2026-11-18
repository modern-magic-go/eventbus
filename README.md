# eventbus

可复用的 Go 事件总线模块，支持：

- 显式构造的 `Bus` 实例，无包级单例
- 发布 / 订阅 / 消费生命周期管理
- 失败重试与死信路由编排
- handler panic 兜底，不再导致进程退出
- pending 消息自动回收（可选，基于 Redis `XAUTOCLAIM`）
- 可选 `Observer` 扩展点
- 基于 Redis 的默认运行时实现

投递语义取决于配置：启用 pending 回收时为 at-least-once（至少一次，可能重复），详见 [投递语义](#投递语义)。

## 安装

```bash
go get github.com/modern-magic-go/eventbus
```

当前 module 路径为 `github.com/modern-magic-go/eventbus`，发布到对应仓库后，其他项目即可直接 `go get`。

## 目录结构

- 根目录：稳定的对外 API，保持导入路径为 `github.com/modern-magic-go/eventbus`
- `internal/core/`：事件总线核心实现
- `internal/redisruntime/`：Redis 运行时实现
- `internal/integration/`：Redis 集成测试与可选的真实 Redis e2e 测试

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

- `XACK` 语义：处理成功才 `XACK`；处理失败（含 panic）时，重试/死信任务写入成功后才 `XACK` 原消息（所有权转移），失败消息不会长期滞留 PEL（Ack 本身失败时除外）；写入失败则不 `XACK`，留给 reclaim 或人工处理。
- 以下场景会**重复投递**同一条消息：
  - handler 处理成功但进程在 `XACK` 前崩溃/被 kill；
  - 一批消息的处理总耗时超过 `PendingReclaim.MinIdle`，尚未轮到处理的消息被 reclaim 认领（同一实例或其他实例均可能，详见 [Pending 消息回收](#pending-消息回收)）；
  - 停机/取消时已 fetch 但未处理的批次留在 PEL，开启 reclaim 后会被重投；
  - 「先写重试/死信任务、再 `XACK`」两步之间崩溃，会同时存在重试任务与 PEL 原件（不漏消息，但可能重复）；
  - 重试任务在「写新任务/删旧任务」之间崩溃（见 [已知限制](#已知限制与后续计划)）。
- 因此 **handler 必须按 `Message.ID` 做幂等**（例如唯一索引、状态机 CAS、去重表）。`Message.ID` 在发布时生成（雪花 ID），可通过 `eventbus.WithMessageID` 显式指定，并在重试/死信任务中以 `RetryTask.MessageID` / `DeadLetterMessage.MessageID` 保留下来，便于审计与幂等键设计。

### 不保证顺序

- 同一 topic 使用单个 Redis stream，多个消费者/多实例并发处理时，同一订单/聚合根的事件可能乱序。
- 对顺序敏感的场景，需要业务侧保证「单消费者单消费组」，或按聚合根在业务层串行化；当前版本不提供 partition key（见 [已知限制](#已知限制与后续计划)）。

### 失败重试与死信流转

handler 返回 error 或被 panic 中断时，按以下路径处理：

1. 写入重试队列（Redis ZSET，`<namespace>:retry`），记录 `MessageID`、`Attempt`、`AvailableAt` 与 `LastError`；
2. 重试/死信任务写入成功后，才 `XACK` 原 stream 消息——消息所有权转移给重试队列/死信队列，PEL 中不残留；写入失败则不 `XACK`，留给 reclaim 或人工处理；
3. 总线后台 retry worker 按 `WithRetryPollInterval`（默认 1s）轮询到期任务并重新执行 handler；
4. 失败后按 `RetryBaseDelay`（默认 5s）指数退避（5s → 10s → 20s …）重新入队；
5. `Attempt` 达到 `MaxRetries`（默认 3，可按 `ConsumerConfig` 覆盖）时，写入死信队列（Redis list，`<namespace>:dlq`）后 `XACK` 原消息，并触发 `Observer.OnDeadLettered`。

**重试/死信写入失败时不会 `XACK` 原消息**，消息保留在 PEL 中，避免「失败消息凭空消失」：启用 `PendingReclaim` 时会在空闲超时后被回收重试；未启用则会一直停留在 PEL，需要人工处理（或后续开启回收）。

> 当前 DLQ 只是 Redis list，没有消费、查询或重放 API（见 [已知限制](#已知限制与后续计划)），生产环境建议同时通过 `Observer` 落审计日志或转发到外部告警/工单系统。

### panic 兜底

- 每次 handler 调用都会被 `recover()` 包裹。handler panic 不再导致进程退出。
- panic 会被转换为错误（`errors.Is(err, eventbus.ErrHandlerPanic)` 为 true），触发 `Observer.OnHandleFailed`（首次投递与 pending 认领路径；retry worker 中的失败不触发该回调，见已知限制），并走与普通 error 完全相同的重试/死信路径。
- 建议在 handler 内部对可预期的错误返回 error，而不是依赖 panic；panic 兜底只是最后一道防线。

## Pending 消息回收

消费者在 `XREADGROUP` 之后、`XACK` 之前崩溃或重启，消息会停留在 Redis PEL 中。开启 `PendingReclaim` 后，总线会周期性认领空闲超时的 pending 消息，并走与正常消费完全相同的 handler / 重试 / 死信路径。

### 配置示例

```go
bus.Subscribe(eventbus.ConsumerConfig{
	Topic: "order.created",
	Group: "order-worker",
	Handler: handleOrderCreated,
	PendingReclaim: eventbus.PendingReclaimConfig{
		// 消息在 PEL 中空闲超过该时长才会被认领。
		// 必须大于「一批消息的处理总耗时」，建议 ≥ PollBatchSize × handler p99 × 2。
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
  - `RedisRuntime` 已实现（内部使用 Redis `XAUTOCLAIM`，**要求 Redis 6.2+**；低版本下 reclaim 会在运行时静默失效，见下方使用注意）；
  - Runtime 未实现该接口时，`Subscribe` 返回 `eventbus.ErrPendingReclaimUnsupported`。这里的「不会静默降级」仅指接口能力检查；Redis 版本过低导致的运行时失效不在 `Subscribe` 的检查范围内。

### 使用注意

- **`MinIdle` 必须大于「一批消息的处理总耗时」**（约 `PollBatchSize × 单条 handler 最大耗时`，含下游调用超时、重试等待等），而不是单条 handler 耗时。`XREADGROUP` 一次会预取一批消息，idle 从投递时刻开始计算；当前实例还没轮到处理的消息也可能被 reclaim goroutine 认领，导致**同一实例重复处理**。实测：`PollBatchSize=10`、单条 handler 500ms、`MinIdle=1s` 时，10 条消息共触发 17 次 handler 调用（7 条被重复处理）。
- 缓解方式：建议 `PollBatchSize=1` 减少批次预取，或按 `MinIdle ≥ PollBatchSize × handler p99 × 2` 设置。`MinIdle` 过小会造成重复处理，过大则故障恢复变慢。
- 吞吐：每轮以 `Batch` 为目标认领上限、间隔 `Interval`，默认 `Batch=10` / `Interval=10s`；实现会跨多次 `XAUTOCLAIM` 扫描 cursor 以凑满一批。PEL 积压多时调大 `Batch` / 缩小 `Interval`，但要同时考虑上面的批量耗时约束。
- 认领的消息同样受 `MaxRetries` / DLQ 约束，不会无限重试。
- pending 回收依赖消费者名称稳定：`RedisRuntime` 在构造时生成一次默认 consumer name（`eventbus-<pid>-<随机后缀>`），同一 Runtime 生命周期内不变；显式配置 `RedisRuntimeConfig.ConsumerName` 或 `ConsumerConfig.ConsumerName` 时以显式值为准。
- Redis 版本：`XAUTOCLAIM` 需要 Redis 6.2+。版本过低时 reclaim 会在运行时**静默失效**（当前实现忽略认领错误，无日志、无 Observer 回调），请自行确认 Redis 版本；生产环境建议先显式验证 reclaim 生效。

## Observer 用法

`Observer` 是事件总线的观测扩展点，实现该接口即可接入发布、消费、重试、死信全链路事件。

### 接口定义

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

### 实现示例

```go
type loggingObserver struct{ logger *log.Logger }

func (o loggingObserver) OnPublished(_ context.Context, msg eventbus.Message) {
    o.logger.Printf("[published] topic=%s", msg.Topic)
}

func (o loggingObserver) OnPublishFailed(_ context.Context, msg eventbus.Message, cause error) {
    o.logger.Printf("[publish-failed] topic=%s err=%v", msg.Topic, cause)
}

func (o loggingObserver) OnHandled(_ context.Context, msg eventbus.Message) {
    o.logger.Printf("[handled] topic=%s", msg.Topic)
}

func (o loggingObserver) OnHandleFailed(_ context.Context, msg eventbus.Message, cause error) {
    o.logger.Printf("[handle-failed] topic=%s err=%v", msg.Topic, cause)
}

func (o loggingObserver) OnRetryScheduled(_ context.Context, task eventbus.RetryTask, cause error) {
    o.logger.Printf("[retry] topic=%s message_id=%s attempt=%d", task.Topic, task.MessageID, task.Attempt)
}

func (o loggingObserver) OnDeadLettered(_ context.Context, msg eventbus.DeadLetterMessage) {
    o.logger.Printf("[dlq] topic=%s message_id=%s err=%s", msg.Topic, msg.MessageID, msg.Error)
}
```

### 注入方式

```go
bus := eventbus.NewBus(rt,
    eventbus.WithObserver(loggingObserver{logger: log.Default()}),
)
```

不注入时默认使用 no-op 实现：接口调用不会 panic，但**不会输出任何日志或指标**。发布失败、消费失败、重试与死信等事件只能通过返回值、Redis 队列状态感知。生产环境建议至少注入一个日志 Observer，并优先接入指标采集。

### 典型用途

- **日志记录**：记录发布/消费成功与失败
- **指标采集**：对接 Prometheus，统计 QPS、延迟、重试队列长度
- **告警通知**：死信消息触发告警、发布失败通知
- **审计追踪**：利用 `RetryTask.MessageID` / `DeadLetterMessage.MessageID` 记录关键事件流转轨迹

## 优雅停机

```go
// 默认 30s 超时
bus.Stop()

// 自定义超时
bus.StopWithTimeout(10 * time.Second)
```

停机时已 fetch 但未处理完的批次会留在 PEL；若希望重启后自动重投，请启用 `PendingReclaim`，或在业务侧接受重复/人工处理。

## Runtime 生命周期

默认 `Stop()` 不会关闭注入的 `Runtime`（如 Redis 客户端），以便调用方复用连接。
如需总线负责完整生命周期：

```go
bus := eventbus.NewBus(rt, eventbus.WithCloseRuntime(true))
```

## 核心边界

- 核心 API 不暴露 ORM 或具体业务模型
- 事务提交后发布这类语义建议在业务项目侧适配
- 运行时通过 `Runtime` 接口解耦，可替换 Redis 实现
- 不提供幂等去重、outbox、DLQ 重放等上层能力，需业务侧自行实现或等待后续版本（见下节）

## 已知限制与后续计划

以下限制对应 [issue #1](https://github.com/modern-magic-go/eventbus/issues/1) 的 P1/P2 规划项或本次修复的边界，不代表当前版本承诺：

| 限制 | 说明 | 对应 |
| --- | --- | --- |
| 未启用 PendingReclaim 时 PEL 不自动恢复 | 消费者崩溃/停机后 pending 消息会一直留在 PEL，不会自动重投；需开启 `PendingReclaim` 或人工 `XCLAIM` | 本次修复边界 · P0-1 |
| 停机时已 fetch 未处理的消息留在 PEL | 即使优雅停机（`StopWithTimeout`），取消后已读取但尚未处理的批次也不会被 `XACK`；建议启用 `PendingReclaim`，或在业务侧接受重启后重投 | 本次修复边界 |
| Ack 失败不可观测 | `Ack` 失败当前没有 Observer 回调、没有日志，只能靠 reclaim 兜底重新投递；PEL 清理情况无法直接观测 | 本次修复边界 · 可观测性 |
| 无 DLQ 重放 / 管理 API | DLQ 仅为无上限的 Redis list，无消费、查询、重放 API，也没有 TTL/裁剪；Redis flush 或 evict 即丢失 | issue #1 · P1-4 |
| 无 outbox / 幂等去重 | `Publish` 即 `XADD`，业务事务回滚会产生幽灵事件；at-least-once 下的重复消费需业务侧按 `Message.ID` 自行去重 | issue #1 · P1-5 |
| 无 partition key / 顺序保证 | 单 topic 单 stream，多消费者并发处理时不保证同一聚合根的事件顺序；顺序敏感场景需单消费者单组或业务侧串行化 | issue #1 · P1-6 |
| 重试任务换位非原子 | `processRetry` 先写新任务再删旧任务，极端崩溃时可能产生重复重试任务（handler 需幂等） | issue #1 · P1-7 |
| 退避无 jitter / 上限 | 固定指数退避（5s → 10s → 20s …），渠道故障时多实例可能同时重试；建议业务侧配合抖动或限流 | issue #1 · P2 |
| handler 无独立超时 | handler 的 ctx 为总线生命周期 ctx，单条消息处理不会因超时被中断；请在 handler 内部用 `context.WithTimeout` 控制 | issue #1 · P2 |
| 默认 no-op Observer | 仅默认关闭观测，不影响功能；生产环境请注入日志/指标实现 | issue #1 · P2 |
| 重试/死信写入失败时无额外退避 | 写入失败会保留旧重试任务并在下个 `RetryPollInterval` 重新执行 handler（不丢消息），但没有额外退避与日志/指标，Redis 故障期间可能按轮询间隔反复执行 | 本 PR 边界 |
| 重试路径不触发 OnHandleFailed | retry worker 中 handler 失败（含 panic）是基线行为：`OnHandleFailed` 只在首次投递与 reclaim 路径触发，重试路径通过 `OnRetryScheduled` / `OnDeadLettered` 观测 | issue #1 基线行为 |
| 老版本重试任务无 message_id | v0.1.1 已入队的 `RetryTask` 不含 `message_id`，升级后这些旧任务重试时 `Message.ID` 为空、进入 DLQ 时 `MessageID` 为空；旧任务消费完后恢复 | 升级注记 |

后续版本计划按 issue #1 的优先级推进：先补齐 DLQ Store 抽象与重放、幂等去重中间件、partition key，再处理退避 jitter、handler 超时与默认可观测性。

## 主要类型

- `Bus`：事件总线入口
- `Runtime`：底层运行时抽象
- `RedisRuntime`：默认 Redis 实现
- `ConsumerConfig`：订阅/消费配置，含 `PendingReclaim`
- `PendingReclaimConfig`：pending 消息回收配置（`MinIdle` / `Batch` / `Interval`）
- `PendingReclaimer`：可选运行时能力，认领空闲 pending 消息
- `Observer`：发布、处理、重试、死信观测钩子
- `RetryTask` / `DeadLetterMessage`：重试与死信任务，均携带原始 `MessageID`
- `Codec`：payload 编解码抽象
- `ErrHandlerPanic` / `ErrPendingReclaimUnsupported`：可用 `errors.Is` 判断的错误
