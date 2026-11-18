package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	redis "github.com/redis/go-redis/v9"

	"github.com/modern-magic-go/eventbus"
)

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

	bus := eventbus.NewBus(rt,
		eventbus.WithObserver(loggingObserver{logger: log.Default()}),
		eventbus.WithCloseRuntime(false),
	)

	if err := bus.Subscribe(eventbus.ConsumerConfig{
		Topic: "order.created",
		Group: "order-workers",
		Handler: func(ctx context.Context, msg eventbus.Message) error {
			// 注意：投递语义是 at-least-once，handler 需按 msg.ID 自行保证幂等。
			log.Printf("processing order: %s (message_id=%s)", string(msg.Payload), msg.ID)
			return nil
		},
		// PendingReclaim 用于回收「消费后未 XACK 就崩溃」而滞留在 Redis PEL 的消息。
		// 零值禁用；任一字段非零即启用，未设置字段使用默认值。
		PendingReclaim: eventbus.PendingReclaimConfig{
			// MinIdle 应大于单条 handler 的最大耗时（建议 ≥ 2 × p99），
			// 否则正在处理的消息可能被其他消费者提前认领。
			MinIdle:  30 * time.Second, // 默认 eventbus.DefaultReclaimMinIdle = 30s
			Batch:    10,               // 默认 10
			Interval: 10 * time.Second, // 默认 eventbus.DefaultReclaimInterval = 10s
		},
	}); err != nil {
		// 若 Runtime 未实现 PendingReclaimer（RedisRuntime 已实现），返回 ErrPendingReclaimUnsupported。
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := bus.Start(ctx); err != nil {
		log.Fatal(err)
	}

	go func() {
		time.Sleep(2 * time.Second)
		if err := bus.Publish(ctx, "order.created", map[string]any{"id": 1, "item": "coffee"}); err != nil {
			log.Printf("publish error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	if err := bus.StopWithTimeout(10 * time.Second); err != nil {
		log.Printf("stop error: %v", err)
	}
}
