package redisruntime

import (
	"context"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/modern-magic-go/eventbus/internal/core"
)

func TestRedisRuntime_PublishConsumeAckRetryDLQ(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rt := NewRedisRuntime(client, Config{Namespace: "testbus", ConsumerName: "c1"})
	ctx := context.Background()

	msg := core.Message{Topic: "order.created", Payload: []byte(`{"id":1}`), PublishedAt: time.Now()}
	require.NoError(t, rt.Publish(ctx, msg))

	require.NoError(t, rt.EnsureConsumerGroup(ctx, "order.created", "g1"))
	items, err := rt.Consume(ctx, core.ConsumerConfig{Topic: "order.created", Group: "g1", PollBatchSize: 1, PollBlock: 10 * time.Millisecond})
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.NoError(t, rt.Ack(ctx, items[0]))

	pending, err := client.XPending(ctx, "testbus:topic:order.created", "g1").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)

	task := core.RetryTask{
		Topic:       "order.created",
		Group:       "g1",
		Payload:     []byte(`{"id":1}`),
		Attempt:     1,
		MaxRetries:  3,
		AvailableAt: time.Now().Add(-time.Second),
	}
	require.NoError(t, rt.ScheduleRetry(ctx, task))

	retries, err := rt.PollRetry(ctx, 10, time.Now())
	require.NoError(t, err)
	require.Len(t, retries, 1)
	require.NoError(t, rt.RemoveRetry(ctx, retries[0]))

	dlq := core.DeadLetterMessage{
		Topic:      "order.created",
		Group:      "g1",
		Payload:    []byte(`{"id":1}`),
		Attempt:    3,
		MaxRetries: 3,
		Error:      "boom",
		OccurredAt: time.Now(),
	}
	require.NoError(t, rt.MoveToDeadLetter(ctx, dlq))

	length, err := client.LLen(ctx, "testbus:dlq").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), length)
}

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
		mr.Close()
	})
	return mr, client
}

// 默认 consumer name 必须在 Runtime 生命周期内保持稳定，
// 否则每次 Consume 都会新建“僵尸消费者”，PEL 归属也无法追踪。
func TestRedisRuntime_ConsumerNameIsStable(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "stable"})
	ctx := context.Background()

	require.NoError(t, rt.Publish(ctx, core.Message{Topic: "t", Payload: []byte(`1`), PublishedAt: time.Now()}))
	require.NoError(t, rt.Publish(ctx, core.Message{Topic: "t", Payload: []byte(`2`), PublishedAt: time.Now()}))
	require.NoError(t, rt.EnsureConsumerGroup(ctx, "t", "g"))

	cfg := core.ConsumerConfig{Topic: "t", Group: "g", PollBatchSize: 1, PollBlock: 10 * time.Millisecond}
	for i := 0; i < 2; i++ {
		msgs, err := rt.Consume(ctx, cfg)
		require.NoError(t, err)
		require.Len(t, msgs, 1)
		require.NoError(t, rt.Ack(ctx, msgs[0]))
	}

	consumers, err := client.XInfoConsumers(ctx, "stable:topic:t", "g").Result()
	require.NoError(t, err)
	require.Len(t, consumers, 1)
	assert.Contains(t, consumers[0].Name, "eventbus-")
}

// P0-1 回归：未 Ack 的消息在空闲超过 MinIdle 后可被 XAUTOCLAIM 认领。
func TestRedisRuntime_ClaimPendingRecoversUnacked(t *testing.T) {
	mr, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "reclaim"})
	ctx := context.Background()

	require.NoError(t, rt.Publish(ctx, core.Message{
		ID:          "msg-1",
		Topic:       "t",
		Payload:     []byte(`{"id":1}`),
		PublishedAt: time.Now(),
	}))
	require.NoError(t, rt.EnsureConsumerGroup(ctx, "t", "g"))

	items, err := rt.Consume(ctx, core.ConsumerConfig{
		Topic: "t", Group: "g", PollBatchSize: 1, PollBlock: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Len(t, items, 1)

	pending, err := client.XPending(ctx, "reclaim:topic:t", "g").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), pending.Count)

	reclaimCfg := core.ConsumerConfig{
		Topic:          "t",
		Group:          "g",
		PendingReclaim: core.PendingReclaimConfig{MinIdle: time.Minute, Batch: 1},
	}

	// 空闲时间未达到 MinIdle：不能认领，避免抢走正在处理的消息。
	claimed, err := rt.ClaimPending(ctx, reclaimCfg)
	require.NoError(t, err)
	assert.Empty(t, claimed)

	// 模拟消费者崩溃后的时间流逝（miniredis 用 SetTime 推进逻辑时钟）。
	mr.SetTime(time.Now().Add(2 * time.Minute))

	claimed, err = rt.ClaimPending(ctx, reclaimCfg)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	assert.Equal(t, "msg-1", claimed[0].ID)
	assert.NotEmpty(t, claimed[0].DeliveryTag)

	require.NoError(t, rt.Ack(ctx, claimed[0]))
	pending, err = client.XPending(ctx, "reclaim:topic:t", "g").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)
}

// 消费消息时应还原发布时间的秒级时间戳，便于审计 / 延迟观测。
func TestRedisRuntime_PublishedAtParsed(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "published"})
	ctx := context.Background()

	publishedAt := time.Unix(1700000000, 0)
	require.NoError(t, rt.Publish(ctx, core.Message{
		Topic:       "t",
		Payload:     []byte(`1`),
		PublishedAt: publishedAt,
	}))
	require.NoError(t, rt.EnsureConsumerGroup(ctx, "t", "g"))

	items, err := rt.Consume(ctx, core.ConsumerConfig{
		Topic: "t", Group: "g", PollBatchSize: 1, PollBlock: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, publishedAt.Unix(), items[0].PublishedAt.Unix())
}
