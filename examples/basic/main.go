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
	o.logger.Printf("[retry-scheduled] topic=%s attempt=%d err=%v", task.Topic, task.Attempt, cause)
}

func (o loggingObserver) OnDeadLettered(_ context.Context, msg eventbus.DeadLetterMessage) {
	o.logger.Printf("[dead-lettered] topic=%s attempt=%d err=%s", msg.Topic, msg.Attempt, msg.Error)
}

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "192.168.4.111:6379"
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
			log.Printf("processing order: %s", string(msg.Payload))
			return nil
		},
	}); err != nil {
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
