package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/modern-magic-go/eventbus"
)

// TestE2E_RealRedis 在真实 Redis（6.2+，XAUTOCLAIM 需要）上验证 P0 修复与 pending 回收的回归场景。
// 默认跳过，避免 CI 依赖外部 Redis；本地联调：
//
//	EVENTBUS_E2E_REDIS_ADDR=127.0.0.1:6379 go test ./internal/integration/ -run TestE2E_RealRedis -v
//
// 该测试会清空 EVENTBUS_E2E_REDIS_DB（默认 14）指定的库，请勿指向有数据的库。
func TestE2E_RealRedis(t *testing.T) {
	addr := os.Getenv("EVENTBUS_E2E_REDIS_ADDR")
	if addr == "" {
		t.Skip("set EVENTBUS_E2E_REDIS_ADDR to run real-Redis e2e tests")
	}

	db := 14
	if raw := os.Getenv("EVENTBUS_E2E_REDIS_DB"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		require.NoError(t, err)
		db = parsed
	}

	client := redis.NewClient(&redis.Options{Addr: addr, DB: db})
	ctx := context.Background()
	require.NoError(t, client.FlushDB(ctx).Err(), "e2e 需要独占 DB %d", db)
	t.Cleanup(func() {
		_ = client.FlushDB(context.Background()).Err()
		_ = client.Close()
	})

	t.Run("failure_acks_original", func(t *testing.T) { e2eFailureAcksOriginal(t, client) })
	t.Run("panic_recovered", func(t *testing.T) { e2ePanicRecovered(t, client) })
	t.Run("pending_reclaim_after_restart", func(t *testing.T) { e2ePendingReclaimAfterRestart(t, client) })
	t.Run("pending_reclaim_cursor_walk", func(t *testing.T) { e2ePendingReclaimCursorWalk(t, client) })
}

func newE2ERuntime(t *testing.T, client *redis.Client, namespace, consumerName string) *eventbus.RedisRuntime {
	t.Helper()
	return eventbus.NewRedisRuntime(client, eventbus.RedisRuntimeConfig{
		Namespace:    namespace,
		ConsumerName: consumerName,
	})
}

func e2eKey(namespace, suffix string) string {
	return namespace + ":" + suffix
}

func e2eFailureAcksOriginal(t *testing.T, client *redis.Client) {
	namespace := fmt.Sprintf("e2e-fail-%d", time.Now().UnixNano())
	rt := newE2ERuntime(t, client, namespace, "worker-fail")
	bus := eventbus.NewBus(rt, eventbus.WithRetryPollInterval(10*time.Millisecond))

	require.NoError(t, bus.Subscribe(eventbus.ConsumerConfig{
		Topic:          topic,
		Group:          group,
		MaxRetries:     1,
		RetryBaseDelay: 10 * time.Millisecond,
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
	dlqKey := e2eKey(namespace, "dlq")

	waitFor(t, 5*time.Second, func() bool {
		n, err := client.LLen(ctx, dlqKey).Result()
		return err == nil && n == 1
	})

	pending, err := client.XPending(ctx, e2eKey(namespace, "topic:"+topic), group).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)

	raw, err := client.LIndex(ctx, dlqKey, 0).Result()
	require.NoError(t, err)
	var dlq eventbus.DeadLetterMessage
	require.NoError(t, json.Unmarshal([]byte(raw), &dlq))
	assert.Equal(t, "msg-fail", dlq.MessageID)
	assert.Contains(t, dlq.Error, "boom")
}

func e2ePanicRecovered(t *testing.T, client *redis.Client) {
	namespace := fmt.Sprintf("e2e-panic-%d", time.Now().UnixNano())
	rt := newE2ERuntime(t, client, namespace, "worker-panic")
	bus := eventbus.NewBus(rt, eventbus.WithRetryPollInterval(10*time.Millisecond))

	require.NoError(t, bus.Subscribe(eventbus.ConsumerConfig{
		Topic:          topic,
		Group:          group,
		MaxRetries:     1,
		RetryBaseDelay: 10 * time.Millisecond,
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
	dlqKey := e2eKey(namespace, "dlq")

	waitFor(t, 5*time.Second, func() bool {
		n, err := client.LLen(ctx, dlqKey).Result()
		return err == nil && n == 1
	})

	raw, err := client.LIndex(ctx, dlqKey, 0).Result()
	require.NoError(t, err)
	var dlq eventbus.DeadLetterMessage
	require.NoError(t, json.Unmarshal([]byte(raw), &dlq))
	assert.Equal(t, "msg-panic", dlq.MessageID)
	assert.Contains(t, dlq.Error, "handler panic")

	pending, err := client.XPending(ctx, e2eKey(namespace, "topic:"+topic), group).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)
}

func e2ePendingReclaimAfterRestart(t *testing.T, client *redis.Client) {
	namespace := fmt.Sprintf("e2e-reclaim-%d", time.Now().UnixNano())
	ctx := context.Background()

	// 旧消费者读到消息后不 Ack，模拟进程在 XREADGROUP 与 XACK 之间崩溃。
	crashed := newE2ERuntime(t, client, namespace, "crashed-worker")
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

	pending, err := client.XPending(ctx, e2eKey(namespace, "topic:"+topic), group).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), pending.Count)

	// 新进程启用 pending 回收，通过 XAUTOCLAIM 接管并消费。
	restarted := newE2ERuntime(t, client, namespace, "restarted-worker")
	bus := eventbus.NewBus(restarted)

	handled := make(chan string, 1)
	require.NoError(t, bus.Subscribe(eventbus.ConsumerConfig{
		Topic:     topic,
		Group:     group,
		PollBlock: 20 * time.Millisecond,
		PendingReclaim: eventbus.PendingReclaimConfig{
			MinIdle:  50 * time.Millisecond,
			Interval: 20 * time.Millisecond,
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
	case <-time.After(5 * time.Second):
		t.Fatal("real redis: pending 消息未被认领处理")
	}

	waitFor(t, 2*time.Second, func() bool {
		p, err := client.XPending(startCtx, e2eKey(namespace, "topic:"+topic), group).Result()
		return err == nil && p.Count == 0
	})
}

// P1 回归（real Redis）：XAUTOCLAIM 单次调用只扫描 count*10 个 PEL 条目。
// 若 PEL 头部堆满「idle 未达 MinIdle」的条目，固定 Start:"0-0" 会永远看不到尾部；
// ClaimPending 必须沿 next cursor 推进，才能认领到尾部可处理消息。
func e2ePendingReclaimCursorWalk(t *testing.T, client *redis.Client) {
	namespace := fmt.Sprintf("e2e-cursor-%d", time.Now().UnixNano())
	rt := newE2ERuntime(t, client, namespace, "cursor-worker")
	ctx := context.Background()
	streamKey := e2eKey(namespace, "topic:"+topic)

	const (
		total   = 300
		headLen = 150
		minIdle = time.Second
		batch   = 10
	)

	for i := 0; i < total; i++ {
		require.NoError(t, rt.Publish(ctx, eventbus.Message{
			ID:          fmt.Sprintf("cursor-%03d", i),
			Topic:       topic,
			Payload:     []byte(`{"id":1}`),
			PublishedAt: time.Now(),
		}))
	}
	require.NoError(t, rt.EnsureConsumerGroup(ctx, topic, group))

	// 一次投递全部消息形成 PEL（不 XACK）。
	msgs, err := rt.Consume(ctx, eventbus.ConsumerConfig{
		Topic: topic, Group: group, PollBatchSize: total, PollBlock: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Len(t, msgs, total)

	// 让尾部消息 idle 超过 MinIdle，然后刷新头部 headLen 条的 idle，
	// 模拟外部工具/滚动发布用 XCLAIM 刷新过 PEL 头部。
	time.Sleep(minIdle + 200*time.Millisecond)
	headIDs := make([]string, 0, headLen)
	headSet := make(map[string]bool, headLen)
	for _, m := range msgs[:headLen] {
		headIDs = append(headIDs, m.DeliveryTag)
		headSet[m.ID] = true
	}
	_, err = client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   streamKey,
		Group:    group,
		Consumer: "external-tool",
		MinIdle:  0,
		Messages: headIDs,
	}).Result()
	require.NoError(t, err)

	claimed, err := rt.ClaimPending(ctx, eventbus.ConsumerConfig{
		Topic: topic,
		Group: group,
		PendingReclaim: eventbus.PendingReclaimConfig{
			MinIdle: minIdle,
			Batch:   batch,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, claimed, "PEL 尾部可认领消息不应因头部占满扫描预算而饿死")
	require.LessOrEqual(t, len(claimed), batch)

	for _, msg := range claimed {
		assert.False(t, headSet[msg.ID], "不应认领刚刷新过 idle 的头部消息: %s", msg.ID)
	}
}
