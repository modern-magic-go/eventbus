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
