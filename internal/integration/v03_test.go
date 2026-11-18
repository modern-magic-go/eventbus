package integration

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/modern-magic-go/eventbus"
)

const retryDataKey = testNamespace + ":retry:data"

// issue #4-1：新消费组 StartFrom="$" 时不重放历史消息，只消费订阅之后的新消息。
func TestBusRedis_StartFromLatestDoesNotReplayHistory(t *testing.T) {
	_, _, rt := newRedisHarness(t)
	ctx := context.Background()

	// 历史消息：此时消费组尚未创建。
	for _, id := range []string{"hist-1", "hist-2"} {
		require.NoError(t, rt.Publish(ctx, eventbus.Message{
			ID: id, Topic: topic, Payload: []byte(`{"id":1}`), PublishedAt: time.Now(),
		}))
	}

	handled := make(chan string, 4)
	bus := eventbus.NewBus(rt)
	require.NoError(t, bus.Subscribe(eventbus.ConsumerConfig{
		Topic:     topic,
		Group:     group,
		StartFrom: eventbus.StartFromLatest,
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
		t.Fatalf("StartFrom=$ 不应重放历史消息，收到 %s", id)
	case <-time.After(150 * time.Millisecond):
	}

	require.NoError(t, bus.Publish(ctx, topic, []byte(`{"id":2}`), eventbus.WithMessageID("new-1")))
	select {
	case id := <-handled:
		assert.Equal(t, "new-1", id)
	case <-time.After(2 * time.Second):
		t.Fatal("StartFrom=$ 后发布的新消息未被消费")
	}
}

// issue #4-1：默认 StartFrom（"0"）保持兼容，消费组会重放历史消息。
func TestBusRedis_DefaultStartReplaysHistory(t *testing.T) {
	_, _, rt := newRedisHarness(t)
	ctx := context.Background()

	for _, id := range []string{"hist-1", "hist-2"} {
		require.NoError(t, rt.Publish(ctx, eventbus.Message{
			ID: id, Topic: topic, Payload: []byte(`{"id":1}`), PublishedAt: time.Now(),
		}))
	}

	handled := make(chan string, 4)
	bus := eventbus.NewBus(rt)
	require.NoError(t, bus.Subscribe(eventbus.ConsumerConfig{
		Topic:     topic,
		Group:     group,
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

	got := map[string]bool{}
	deadline := time.After(2 * time.Second)
	for len(got) < 2 {
		select {
		case id := <-handled:
			got[id] = true
		case <-deadline:
			t.Fatalf("默认 StartFrom 应重放历史消息，实际收到 %v", got)
		}
	}
	assert.True(t, got["hist-1"])
	assert.True(t, got["hist-2"])
}

// issue #1-4：死信可通过 Bus 查询，并通过 ReplayDeadLetters 重新入队、最终处理成功。
func TestBusRedis_DeadLetterQueryAndReplay(t *testing.T) {
	_, client, rt := newRedisHarness(t)
	bus := eventbus.NewBus(rt, eventbus.WithRetryPollInterval(10*time.Millisecond))

	var shouldFail atomic.Bool
	shouldFail.Store(true)
	handled := make(chan string, 2)

	require.NoError(t, bus.Subscribe(eventbus.ConsumerConfig{
		Topic:          topic,
		Group:          group,
		MaxRetries:     1,
		RetryBaseDelay: 10 * time.Millisecond,
		PollBlock:      20 * time.Millisecond,
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

	// 死信写入与 RemoveRetry 是两步：等两者都稳定后再重放，避免与 retry worker 的清理竞争。
	waitFor(t, 3*time.Second, func() bool {
		deads, err := bus.DeadLetters(ctx, 0, 10)
		if err != nil || len(deads) != 1 {
			return false
		}
		zcard, err := client.ZCard(ctx, retryKey).Result()
		return err == nil && zcard == 0
	})

	deads, err := bus.DeadLetters(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, deads, 1)
	assert.NotEmpty(t, deads[0].ID, "死信记录应有 Runtime 生成的 ID")
	assert.Equal(t, "msg-dlq", deads[0].MessageID)
	assert.Equal(t, topic, deads[0].Topic)
	assert.Contains(t, deads[0].Error, "boom")

	shouldFail.Store(false)
	res, err := bus.ReplayDeadLetters(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Replayed)
	assert.Equal(t, 0, res.Skipped)
	assert.Empty(t, res.Errors)

	select {
	case id := <-handled:
		assert.Equal(t, "msg-dlq", id)
	case <-time.After(3 * time.Second):
		t.Fatal("重放消息未被重试队列处理")
	}

	waitFor(t, 2*time.Second, func() bool {
		deads, err := bus.DeadLetters(ctx, 0, 10)
		return err == nil && len(deads) == 0
	})
	pending, err := client.XPending(ctx, streamKey, group).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)
}

// issue #1-5：开启去重后，相同 MessageID 的重复投递只会执行一次 handler。
func TestBusRedis_DedupSkipsDuplicate(t *testing.T) {
	_, client, rt := newRedisHarness(t)
	store := eventbus.NewRedisDedupStore(client, eventbus.RedisDedupStoreConfig{Namespace: testNamespace})

	var calls atomic.Int32
	bus := eventbus.NewBus(rt, eventbus.WithDedup(store, eventbus.DedupConfig{TTL: time.Minute}))

	require.NoError(t, bus.Subscribe(eventbus.ConsumerConfig{
		Topic:     topic,
		Group:     group,
		PollBlock: 20 * time.Millisecond,
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

	waitFor(t, 2*time.Second, func() bool { return calls.Load() >= 2 })
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(2), calls.Load(), "重复 MessageID 不应重复执行 handler")

	waitFor(t, 2*time.Second, func() bool {
		pending, err := client.XPending(ctx, streamKey, group).Result()
		return err == nil && pending.Count == 0
	})
}

// issue #4-4：StopWithTimeout 提供 drain 语义——先停止拉取，等待在途 handler 完成，
// handler ctx 在 drain 期间不被取消，完成后消息正常 XACK。
func TestBusRedis_GracefulDrain(t *testing.T) {
	_, client, rt := newRedisHarness(t)
	bus := eventbus.NewBus(rt)

	var once sync.Once
	started := make(chan struct{})
	release := make(chan struct{})
	handlerCtxErr := make(chan error, 1)

	require.NoError(t, bus.Subscribe(eventbus.ConsumerConfig{
		Topic:     topic,
		Group:     group,
		PollBlock: 20 * time.Millisecond,
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
	case <-time.After(2 * time.Second):
		t.Fatal("handler 未启动")
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- bus.StopWithTimeout(2 * time.Second) }()

	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-handlerCtxErr:
		t.Fatalf("drain 完成前 handler ctx 不应被取消: %v", err)
	default:
	}

	close(release)
	select {
	case err := <-stopDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("StopWithTimeout 未在超时前返回")
	}
	require.NoError(t, <-handlerCtxErr)

	waitFor(t, 2*time.Second, func() bool {
		pending, err := client.XPending(context.Background(), streamKey, group).Result()
		return err == nil && pending.Count == 0
	})
}

// issue #1-7：同一 MessageID 重复调度重试任务时，ZSET 与 payload HASH 都只保留一份。
func TestRedisRuntime_RetryStoreIsIdempotent(t *testing.T) {
	_, client, rt := newRedisHarness(t)
	ctx := context.Background()

	task := eventbus.RetryTask{
		MessageID:   "msg-idem",
		Topic:       topic,
		Group:       group,
		Payload:     []byte(`{"id":1}`),
		Attempt:     1,
		MaxRetries:  3,
		AvailableAt: time.Now().Add(-time.Second),
	}
	require.NoError(t, rt.ScheduleRetry(ctx, task))

	task.Attempt = 2
	task.AvailableAt = time.Now().Add(-time.Second)
	require.NoError(t, rt.ScheduleRetry(ctx, task))

	zcard, err := client.ZCard(ctx, retryKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), zcard)
	hlen, err := client.HLen(ctx, retryDataKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), hlen)

	tasks, err := rt.PollRetry(ctx, 10, time.Now())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, 2, tasks[0].Attempt)

	require.NoError(t, rt.RemoveRetry(ctx, tasks[0]))
	zcard, err = client.ZCard(ctx, retryKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), zcard)
	hlen, err = client.HLen(ctx, retryDataKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), hlen)
}

// issue #4-3 / #1 P2：RuntimeStats 提供 pending / retry 等基础指标。
func TestRedisRuntime_Stats(t *testing.T) {
	_, _, rt := newRedisHarness(t)
	ctx := context.Background()

	require.NoError(t, rt.Publish(ctx, eventbus.Message{
		ID: "stat-1", Topic: topic, Payload: []byte(`{"id":1}`), PublishedAt: time.Now(),
	}))
	require.NoError(t, rt.EnsureConsumerGroup(ctx, topic, group))

	msgs, err := rt.Consume(ctx, eventbus.ConsumerConfig{
		Topic: topic, Group: group, PollBatchSize: 1, PollBlock: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	pending, err := rt.PendingCount(ctx, topic, group)
	require.NoError(t, err)
	assert.Equal(t, int64(1), pending)

	retryLen, err := rt.RetryQueueLen(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), retryLen)

	require.NoError(t, rt.ScheduleRetry(ctx, eventbus.RetryTask{
		MessageID: "retry-stat", Topic: topic, Group: group, Payload: []byte(`1`),
		Attempt: 1, MaxRetries: 3, AvailableAt: time.Now(),
	}))
	retryLen, err = rt.RetryQueueLen(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), retryLen)
}
