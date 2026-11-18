package integration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/modern-magic-go/eventbus"
)

const (
	testNamespace = "itest"
	topic         = "order.created"
	group         = "order-worker"
	streamKey     = testNamespace + ":topic:" + topic
	retryKey      = testNamespace + ":retry"
	dlqKey        = testNamespace + ":dlq"
)

func newRedisHarness(t *testing.T) (*miniredis.Miniredis, *redis.Client, *eventbus.RedisRuntime) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rt := eventbus.NewRedisRuntime(client, eventbus.RedisRuntimeConfig{Namespace: testNamespace})

	t.Cleanup(func() {
		_ = client.Close()
		mr.Close()
	})
	return mr, client, rt
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// P0-2 端到端回归：handler 失败后原消息被 XACK，PEL 不泄漏，
// 消息最终只保留在 retry 队列 / DLQ 的其中一处。
func TestBusRedis_FailureAcksOriginalStreamMessage(t *testing.T) {
	_, client, rt := newRedisHarness(t)
	bus := eventbus.NewBus(rt, eventbus.WithRetryPollInterval(5*time.Millisecond))

	require.NoError(t, bus.Subscribe(eventbus.ConsumerConfig{
		Topic:          topic,
		Group:          group,
		MaxRetries:     1,
		RetryBaseDelay: 5 * time.Millisecond,
		PollBlock:      20 * time.Millisecond,
		Handler: func(context.Context, eventbus.Message) error {
			return errors.New("boom")
		},
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	require.NoError(t, bus.Publish(ctx, topic, map[string]int{"id": 1}, eventbus.WithMessageID("msg-fail")))

	// MoveToDeadLetter 与 RemoveRetry 是两步操作，等待两者都完成，避免读到中间状态。
	waitFor(t, 3*time.Second, func() bool {
		n, err := client.LLen(ctx, dlqKey).Result()
		if err != nil || n != 1 {
			return false
		}
		zcard, err := client.ZCard(ctx, retryKey).Result()
		return err == nil && zcard == 0
	})

	pending, err := client.XPending(ctx, streamKey, group).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count, "失败消息不应残留在 PEL")

	zcard, err := client.ZCard(ctx, retryKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), zcard, "进入 DLQ 后重试任务应被移除")

	raw, err := client.LIndex(ctx, dlqKey, 0).Result()
	require.NoError(t, err)
	var dlq eventbus.DeadLetterMessage
	require.NoError(t, json.Unmarshal([]byte(raw), &dlq))
	assert.Equal(t, "msg-fail", dlq.MessageID)
	assert.Contains(t, dlq.Error, "boom")
}

// P0-3 端到端回归：handler panic 不导致消费 goroutine 崩溃，走重试 / 死信兜底。
func TestBusRedis_PanicRecoveredAndDeadLettered(t *testing.T) {
	_, client, rt := newRedisHarness(t)
	bus := eventbus.NewBus(rt, eventbus.WithRetryPollInterval(5*time.Millisecond))

	require.NoError(t, bus.Subscribe(eventbus.ConsumerConfig{
		Topic:          topic,
		Group:          group,
		MaxRetries:     1,
		RetryBaseDelay: 5 * time.Millisecond,
		PollBlock:      20 * time.Millisecond,
		Handler: func(context.Context, eventbus.Message) error {
			panic("dirty payload")
		},
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	require.NoError(t, bus.Publish(ctx, topic, []byte("not-json"), eventbus.WithMessageID("msg-panic")))

	waitFor(t, 3*time.Second, func() bool {
		n, err := client.LLen(ctx, dlqKey).Result()
		return err == nil && n == 1
	})

	raw, err := client.LIndex(ctx, dlqKey, 0).Result()
	require.NoError(t, err)
	var dlq eventbus.DeadLetterMessage
	require.NoError(t, json.Unmarshal([]byte(raw), &dlq))
	assert.Equal(t, "msg-panic", dlq.MessageID)
	assert.Contains(t, dlq.Error, "handler panic")
	assert.Contains(t, dlq.Error, "dirty payload")

	pending, err := client.XPending(ctx, streamKey, group).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)
}

// P0-1 端到端回归：消费者在 XREADGROUP 之后、XACK 之前“崩溃”，
// 重启后启用 PendingReclaim 的消费者应通过 XAUTOCLAIM 接管消息并完成处理。
func TestBusRedis_PendingReclaimAfterRestart(t *testing.T) {
	mr, client, _ := newRedisHarness(t)
	ctx := context.Background()

	// 旧消费者的运行时：读到消息后不 Ack，模拟进程崩溃。
	crashed := eventbus.NewRedisRuntime(client, eventbus.RedisRuntimeConfig{
		Namespace:    testNamespace,
		ConsumerName: "crashed-worker",
	})
	require.NoError(t, crashed.Publish(ctx, eventbus.Message{
		ID:          "msg-orphan",
		Topic:       topic,
		Payload:     []byte(`{"id":2}`),
		PublishedAt: time.Now(),
	}))
	require.NoError(t, crashed.EnsureConsumerGroup(ctx, topic, group))

	orphans, err := crashed.Consume(ctx, eventbus.ConsumerConfig{
		Topic: topic, Group: group, PollBatchSize: 1, PollBlock: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Len(t, orphans, 1)

	pending, err := client.XPending(ctx, streamKey, group).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), pending.Count)

	// “重启”后的新进程：启用 pending 回收。
	restarted := eventbus.NewRedisRuntime(client, eventbus.RedisRuntimeConfig{
		Namespace:    testNamespace,
		ConsumerName: "restarted-worker",
	})
	bus := eventbus.NewBus(restarted)

	handled := make(chan string, 1)
	require.NoError(t, bus.Subscribe(eventbus.ConsumerConfig{
		Topic:     topic,
		Group:     group,
		PollBlock: 20 * time.Millisecond,
		PendingReclaim: eventbus.PendingReclaimConfig{
			MinIdle:  20 * time.Millisecond,
			Interval: 10 * time.Millisecond,
		},
		Handler: func(_ context.Context, msg eventbus.Message) error {
			handled <- msg.ID
			return nil
		},
	}))

	startCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(startCtx))
	defer bus.Stop()

	select {
	case id := <-handled:
		assert.Equal(t, "msg-orphan", id)
	case <-time.After(3 * time.Second):
		t.Fatal("pending 消息未被认领处理")
	}

	waitFor(t, 2*time.Second, func() bool {
		p, err := client.XPending(startCtx, streamKey, group).Result()
		return err == nil && p.Count == 0
	})

	// 不应被重复处理（认领后 Ack 掉，下一轮不会再认领）。
	select {
	case id := <-handled:
		t.Fatalf("消息被重复处理: %s", id)
	case <-time.After(100 * time.Millisecond):
	}

	_ = mr
}
