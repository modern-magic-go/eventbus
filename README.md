# pkg/eventbus 使用说明

## 模块定位

[`pkg/eventbus/`](pkg/eventbus/) 提供可移植事件总线基础能力，职责仅限于：

- 显式构造的 Bus 实例（无包级单例）。
- 发布 / 订阅 / 消费生命周期管理。
- 失败重试与死信路由编排。
- 可选 observer 扩展点（日志、审计、指标由项目侧注入）。

模块自身不依赖 [`internal/`](internal/) 或项目业务模型。

## 核心边界

- portable 核心 API 不暴露 [`gorm.DB`](internal/svc/eventbus_after_commit.go:28) 或 ORM 类型。
- 事务提交后发布语义由项目层适配：见 [`internal/svc/eventbus_after_commit.go`](internal/svc/eventbus_after_commit.go)。
- Redis 运行时通过显式注入客户端与命名空间工作：见 [`NewRedisRuntime()`](pkg/eventbus/redis_runtime.go:34)。

## 快速组装（项目侧）

项目服务启动时，在 [`newEventBusBundle()`](internal/svc/eventbus.go:22) 组装：

1. 初始化 Redis 客户端。
2. 构造 [`RedisRuntime`](pkg/eventbus/redis_runtime.go:29)。
3. 构造 [`Bus`](pkg/eventbus/bus.go:18) 并注入 observer。
4. 在服务启动与停止阶段调用 [`Start()`](internal/svc/eventbus.go:61) / [`Stop()`](internal/svc/eventbus.go:68)。

## Observer 用法

核心通过 [`Observer`](pkg/eventbus/observer.go:6) 暴露 4 个节点：

- 发布成功 [`OnPublished()`](pkg/eventbus/observer.go:7)
- 处理成功 [`OnHandled()`](pkg/eventbus/observer.go:8)
- 安排重试 [`OnRetryScheduled()`](pkg/eventbus/observer.go:9)
- 进入死信 [`OnDeadLettered()`](pkg/eventbus/observer.go:10)

项目默认实现见 [`eventBusObserver`](internal/svc/eventbus_observer.go:10)。

## After-Commit 适配原则

- portable 核心仅提供立即发布 [`Publish()`](pkg/eventbus/bus.go:75)。
- 若业务要求“提交成功后再发布”，请使用项目层适配 [`PublishAfterCommit()`](internal/svc/eventbus_after_commit.go:28)。
- 不在 [`pkg/eventbus/`](pkg/eventbus/) 内引入事务回调/ORM 依赖。

