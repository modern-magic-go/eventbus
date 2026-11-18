package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/modern-magic-go/eventbus"
)

// TestE2E_RealRedis 在真实 Redis（6.2+，XAUTOCLAIM 需要）上验证 P0 回归与 v0.3 消费语义（StartFrom/去重/重放/drain/stats）。
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
	t.Run("start_from_latest", func(t *testing.T) { e2eStartFromLatest(t, client) })
	t.Run("dedup_skip", func(t *testing.T) { e2eDedupSkip(t, client) })
	t.Run("deadletter_replay", func(t *testing.T) { e2eDeadLetterReplay(t, client) })
	t.Run("graceful_drain", func(t *testing.T) { e2eGracefulDrain(t, client) })
	t.Run("runtime_stats_lag", func(t *testing.T) { e2eRuntimeStatsLag(t, client) })
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

// issue #4-1：真实 Redis 上 StartFrom="$" 不重放历史，只消费之后的新消息。
func e2eStartFromLatest(t *testing.T, client *redis.Client) {
	namespace := fmt.Sprintf("e2e-startfrom-%d", time.Now().UnixNano())
	rt := newE2ERuntime(t, client, namespace, "worker-startfrom")
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		require.NoError(t, rt.Publish(ctx, eventbus.Message{
			ID:          fmt.Sprintf("hist-%d", i),
			Topic:       topic,
			Payload:     []byte(`{"id":1}`),
			PublishedAt: time.Now(),
		}))
	}

	handled := make(chan string, 4)
	bus := eventbus.NewBus(rt)
	require.NoError(t, bus.Subscribe(eventbus.ConsumerConfig{
		Topic: topic, Group: group, StartFrom: eventbus.StartFromLatest,
		PollBlock: 20 * time.Millisecond,
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
		t.Fatalf("StartFrom=$ 不应重放历史消息: %s", id)
	case <-time.After(300 * time.Millisecond):
	}

	require.NoError(t, bus.Publish(ctx, topic, []byte(`{"id":2}`), eventbus.WithMessageID("new-1")))
	select {
	case id := <-handled:
		assert.Equal(t, "new-1", id)
	case <-time.After(5 * time.Second):
		t.Fatal("StartFrom=$ 后发布的新消息未被消费")
	}
}

// issue #1-5：真实 Redis 去重存储下，相同 MessageID 只处理一次。
func e2eDedupSkip(t *testing.T, client *redis.Client) {
	namespace := fmt.Sprintf("e2e-dedup-%d", time.Now().UnixNano())
	rt := newE2ERuntime(t, client, namespace, "worker-dedup")
	store := eventbus.NewRedisDedupStore(client, eventbus.RedisDedupStoreConfig{Namespace: namespace})

	var calls atomic.Int32
	bus := eventbus.NewBus(rt, eventbus.WithDedup(store, eventbus.DedupConfig{TTL: time.Minute}))
	require.NoError(t, bus.Subscribe(eventbus.ConsumerConfig{
		Topic: topic, Group: group, PollBlock: 20 * time.Millisecond,
		Handler: func(context.Context, eventbus.Message) error {
			calls.Add(1)
			return nil
		},
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	require.NoError(t, bus.Publish(ctx, topic, []byte(`{"id":1}`), eventbus.WithMessageID("dup-1")))
	require.NoError(t, bus.Publish(ctx, topic, []byte(`{"id":1}`), eventbus.WithMessageID("dup-1")))
	require.NoError(t, bus.Publish(ctx, topic, []byte(`{"id":2}`), eventbus.WithMessageID("unique-2")))

	waitFor(t, 5*time.Second, func() bool { return calls.Load() >= 2 })
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, int32(2), calls.Load())
}

// issue #1-4：真实 Redis 上死信可查询、可重放，重放后处理成功且 DLQ/PEL 清空。
func e2eDeadLetterReplay(t *testing.T, client *redis.Client) {
	namespace := fmt.Sprintf("e2e-dlq-%d", time.Now().UnixNano())
	rt := newE2ERuntime(t, client, namespace, "worker-dlq")
	bus := eventbus.NewBus(rt, eventbus.WithRetryPollInterval(10*time.Millisecond))

	var shouldFail atomic.Bool
	shouldFail.Store(true)
	handled := make(chan string, 2)
	require.NoError(t, bus.Subscribe(eventbus.ConsumerConfig{
		Topic: topic, Group: group, MaxRetries: 1, RetryBaseDelay: 10 * time.Millisecond,
		PollBlock: 20 * time.Millisecond,
		Handler: func(_ context.Context, msg eventbus.Message) error {
			if shouldFail.Load() {
				return errors.New("boom")
			}
			handled <- msg.ID
			return nil
		},
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	require.NoError(t, bus.Publish(ctx, topic, []byte(`{"id":1}`), eventbus.WithMessageID("msg-dlq")))

	// 等待 DLQ 写入与 retry worker 的 RemoveRetry 都完成，避免立刻重放时任务被旧清理步骤删除。
	waitFor(t, 5*time.Second, func() bool {
		deads, err := bus.DeadLetters(ctx, 0, 10)
		if err != nil || len(deads) != 1 {
			return false
		}
		zcard, err := client.ZCard(ctx, e2eKey(namespace, "retry")).Result()
		return err == nil && zcard == 0
	})
	deads, err := bus.DeadLetters(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, deads, 1)
	assert.NotEmpty(t, deads[0].ID)
	assert.Equal(t, "msg-dlq", deads[0].MessageID)

	shouldFail.Store(false)
	res, err := bus.ReplayDeadLetters(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Replayed)

	select {
	case id := <-handled:
		assert.Equal(t, "msg-dlq", id)
	case <-time.After(5 * time.Second):
		t.Fatal("重放消息未被处理")
	}
	waitFor(t, 3*time.Second, func() bool {
		deads, err := bus.DeadLetters(ctx, 0, 10)
		return err == nil && len(deads) == 0
	})
	pending, err := client.XPending(ctx, e2eKey(namespace, "topic:"+topic), group).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)
}

// issue #4-4：真实 Redis 上 Stop 的 drain 语义：在途 handler 完成并 XACK。
func e2eGracefulDrain(t *testing.T, client *redis.Client) {
	namespace := fmt.Sprintf("e2e-drain-%d", time.Now().UnixNano())
	rt := newE2ERuntime(t, client, namespace, "worker-drain")
	bus := eventbus.NewBus(rt)

	var once sync.Once
	started := make(chan struct{})
	release := make(chan struct{})
	handlerCtxErr := make(chan error, 1)
	require.NoError(t, bus.Subscribe(eventbus.ConsumerConfig{
		Topic: topic, Group: group, PollBlock: 20 * time.Millisecond,
		Handler: func(ctx context.Context, _ eventbus.Message) error {
			once.Do(func() { close(started) })
			<-release
			handlerCtxErr <- ctx.Err()
			return nil
		},
	}))

	require.NoError(t, bus.Start(context.Background()))
	require.NoError(t, bus.Publish(context.Background(), topic, []byte(`{"id":1}`), eventbus.WithMessageID("msg-drain")))

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler 未启动")
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- bus.StopWithTimeout(5 * time.Second) }()

	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-handlerCtxErr:
		t.Fatalf("drain 完成前 handler ctx 不应被取消: %v", err)
	default:
	}

	close(release)
	require.NoError(t, <-stopDone)
	require.NoError(t, <-handlerCtxErr)

	waitFor(t, 3*time.Second, func() bool {
		pending, err := client.XPending(context.Background(), e2eKey(namespace, "topic:"+topic), group).Result()
		return err == nil && pending.Count == 0
	})
}

// issue #4-3：真实 Redis 的 XINFO GROUPS lag / XPENDING / ZCARD 指标语义。
func e2eRuntimeStatsLag(t *testing.T, client *redis.Client) {
	namespace := fmt.Sprintf("e2e-stats-%d", time.Now().UnixNano())
	rt := newE2ERuntime(t, client, namespace, "worker-stats")
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		require.NoError(t, rt.Publish(ctx, eventbus.Message{
			ID: fmt.Sprintf("stat-%d", i), Topic: topic, Payload: []byte(`{"id":1}`), PublishedAt: time.Now(),
		}))
	}
	require.NoError(t, rt.EnsureConsumerGroup(ctx, topic, group))

	lag, err := rt.StreamLag(ctx, topic, group)
	require.NoError(t, err)
	assert.Equal(t, int64(3), lag, "组刚创建时应为 3 条积压")

	msgs, err := rt.Consume(ctx, eventbus.ConsumerConfig{
		Topic: topic, Group: group, PollBatchSize: 1, PollBlock: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	lag, err = rt.StreamLag(ctx, topic, group)
	require.NoError(t, err)
	assert.Equal(t, int64(2), lag)
	pending, err := rt.PendingCount(ctx, topic, group)
	require.NoError(t, err)
	assert.Equal(t, int64(1), pending)

	retryLen, err := rt.RetryQueueLen(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), retryLen)
}
