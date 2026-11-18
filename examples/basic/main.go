package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	redis "github.com/redis/go-redis/v9"

	"github.com/modern-magic-go/eventbus"
)

// loggingObserver 实现 eventbus.Observer；额外实现可选的 eventbus.ErrorObserver，
// 用于观测 consume / reclaim / ack / 重试 / 死信等内部错误（老 Observer 无需实现）。
type loggingObserver struct{ logger *log.Logger }

func (o loggingObserver) OnPublished(_ context.Context, msg eventbus.Message) {
	o.logger.Printf("[published] topic=%s id=%s", msg.Topic, msg.ID)
}

func (o loggingObserver) OnPublishFailed(_ context.Context, msg eventbus.Message, cause error) {
	o.logger.Printf("[publish-failed] topic=%s err=%v", msg.Topic, cause)
}

func (o loggingObserver) OnHandled(_ context.Context, msg eventbus.Message) {
	o.logger.Printf("[handled] topic=%s id=%s", msg.Topic, msg.ID)
}

func (o loggingObserver) OnHandleFailed(_ context.Context, msg eventbus.Message, cause error) {
	o.logger.Printf("[handle-failed] topic=%s err=%v", msg.Topic, cause)
}

func (o loggingObserver) OnRetryScheduled(_ context.Context, task eventbus.RetryTask, cause error) {
	o.logger.Printf("[retry-scheduled] topic=%s message_id=%s attempt=%d err=%v", task.Topic, task.MessageID, task.Attempt, cause)
}

func (o loggingObserver) OnDeadLettered(_ context.Context, msg eventbus.DeadLetterMessage) {
	o.logger.Printf("[dead-lettered] topic=%s message_id=%s attempt=%d err=%s", msg.Topic, msg.MessageID, msg.Attempt, msg.Error)
}

// OnError 是可选接口 eventbus.ErrorObserver 的方法，用于观测总线内部错误。
func (o loggingObserver) OnError(_ context.Context, event eventbus.ErrorEvent) {
	o.logger.Printf("[bus-error] op=%s topic=%s group=%s message_id=%s err=%v",
		event.Op, event.Topic, event.Group, event.MessageID, event.Err)
}

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()

	rt := eventbus.NewRedisRuntime(client, eventbus.RedisRuntimeConfig{
		Namespace:    "demo",
		ConsumerName: "worker-1",
	})

	// 可选：消费端两阶段幂等去重（Redis）。只有已成功处理（Done）的重复消息会跳过；
	// 处理中/崩溃残留（InProgress）会继续执行 handler，at-least-once 优先，可能重复处理。
	// 默认 key = topic|group|messageID。
	// 也可以改用 eventbus.NewSlogObserver(logger) 或 eventbus.WithLogger(logger) 观测内部错误。
	dedup := eventbus.NewRedisDedupStore(client, eventbus.RedisDedupStoreConfig{
		Namespace: "demo",
	})

	bus := eventbus.NewBus(rt,
		eventbus.WithObserver(loggingObserver{logger: log.Default()}),
		eventbus.WithDedup(dedup, eventbus.DedupConfig{
			TTL: 24 * time.Hour, // 默认 eventbus.DefaultDedupTTL（24h）
		}),
		eventbus.WithCloseRuntime(false),
	)

	if err := bus.Subscribe(eventbus.ConsumerConfig{
		Topic: "order.created",
		Group: "order-workers",
		// StartFrom 只对「新建消费组」生效：eventbus.StartFromLatest（"$"）表示
		// 只消费订阅之后的新消息；默认 "" / "0" 从头消费。已存在的组不会被重置，
		// 需要从头再消费应换新 group（或手动删除 Redis 消费组）。
		StartFrom: eventbus.StartFromLatest,
		Handler: func(ctx context.Context, msg eventbus.Message) error {
			// header 契约示例：库原样透传 headers，语义由业务/平台层约定。
			log.Printf("processing order: %s (message_id=%s event_type=%s aggregate_id=%s)",
				string(msg.Payload), msg.ID,
				msg.Headers["event_type"], msg.Headers["aggregate_id"])
			// 去重键默认是 topic|group|messageID；业务级幂等可通过
			// DedupConfig.KeyFunc 使用 header 里的 dedup_key。
			return nil
		},
		// PendingReclaim 用于回收「消费后未 XACK 就崩溃」而滞留在 Redis PEL 的消息。
		// 零值禁用；任一字段非零即启用，未设置字段使用默认值。
		PendingReclaim: eventbus.PendingReclaimConfig{
			// 建议 PollBatchSize=1，或让 MinIdle ≥ 一批消息的处理总耗时，
			// 避免同实例 reclaim 并发重复处理已预取但未处理的消息。
			MinIdle:  30 * time.Second, // 默认 eventbus.DefaultReclaimMinIdle = 30s
			Batch:    10,               // 默认 10
			Interval: 10 * time.Second, // 默认 eventbus.DefaultReclaimInterval = 10s
		},
	}); err != nil {
		// 若 Runtime 未实现 PendingReclaimer / ConsumerGroupInitializer，
		// 会返回 ErrPendingReclaimUnsupported / ErrStartFromUnsupported。
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := bus.Start(ctx); err != nil {
		log.Fatal(err)
	}

	go func() {
		time.Sleep(2 * time.Second)
		// header 契约示例：event_type / event_version / aggregate_id / dedup_key
		// 是业务推荐键；traceparent 由 otel 子模块负责，业务不要手写。
		if err := bus.Publish(ctx, "order.created",
			map[string]any{"id": 1, "item": "coffee"},
			eventbus.WithHeaders(map[string]string{
				"event_type":    "order.created",
				"event_version": "1",
				"aggregate_id":  "order-1",
				"dedup_key":     "order-1|order.created|v1",
			}),
		); err != nil {
			log.Printf("publish error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	// Stop 先停止拉取新消息，再 drain 在途 handler；等待超时返回 ErrStopTimeout，
	// 此时注入的 Runtime 不会被关闭（由调用方管理）。
	if err := bus.StopWithTimeout(10 * time.Second); err != nil {
		if errors.Is(err, eventbus.ErrStopTimeout) {
			log.Printf("graceful stop timeout, runtime left open for caller: %v", err)
		} else {
			log.Printf("stop error: %v", err)
		}
	}
}
