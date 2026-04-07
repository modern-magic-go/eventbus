# eventbus

可复用的 Go 事件总线模块，支持：

- 显式构造的 `Bus` 实例，无包级单例
- 发布 / 订阅 / 消费生命周期管理
- 失败重试与死信路由编排
- 可选 `Observer` 扩展点
- 基于 Redis 的默认运行时实现

## 安装

```bash
go get github.com/CATT-L/go-eventbus
```

当前 module 路径为 `github.com/CATT-L/go-eventbus`，发布到对应仓库后，其他项目即可直接 `go get`。

## 目录结构

- 根目录：稳定的对外 API，保持导入路径为 `github.com/CATT-L/go-eventbus`
- `internal/core/`：事件总线核心实现
- `internal/redisruntime/`：Redis 运行时实现

## 快速开始

```go
package main

import (
	"context"
	"log"

	redis "github.com/redis/go-redis/v9"

	"github.com/CATT-L/go-eventbus"
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
    o.logger.Printf("[retry] topic=%s attempt=%d", task.Topic, task.Attempt)
}

func (o loggingObserver) OnDeadLettered(_ context.Context, msg eventbus.DeadLetterMessage) {
    o.logger.Printf("[dlq] topic=%s err=%s", msg.Topic, msg.Error)
}
```

### 注入方式

```go
bus := eventbus.NewBus(rt,
    eventbus.WithObserver(loggingObserver{logger: log.Default()}),
)
```

不注入时默认使用 no-op 实现，不影响正常运行。

### 典型用途

- **日志记录**：记录发布/消费成功与失败
- **指标采集**：对接 Prometheus，统计 QPS、延迟、重试队列长度
- **告警通知**：死信消息触发告警、发布失败通知
- **审计追踪**：记录关键事件流转轨迹

## 优雅停机

```go
// 默认 30s 超时
bus.Stop()

// 自定义超时
bus.StopWithTimeout(10 * time.Second)
```

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

## 主要类型

- `Bus`：事件总线入口
- `Runtime`：底层运行时抽象
- `RedisRuntime`：默认 Redis 实现
- `Observer`：发布、处理、重试、死信观测钩子
- `Codec`：payload 编解码抽象
