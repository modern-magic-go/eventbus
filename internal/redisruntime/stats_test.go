package redisruntime

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/modern-magic-go/eventbus/internal/core"
)

// RuntimeStats：PendingCount 反映 PEL 数量，RetryQueueLen 反映重试 ZSET 长度；
// stream/group 不存在时按 0 返回而不是报错。
func TestRedisRuntime_RuntimeStats(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "stats"})
	ctx := context.Background()

	lag, err := rt.StreamLag(ctx, "t", "g")
	require.NoError(t, err)
	assert.Equal(t, int64(0), lag, "stream/group 不存在时 Lag 按 0 处理")

	pending, err := rt.PendingCount(ctx, "t", "g")
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending, "stream/group 不存在时 Pending 按 0 处理")

	retryLen, err := rt.RetryQueueLen(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), retryLen)

	require.NoError(t, rt.Publish(ctx, core.Message{Topic: "t", Payload: []byte(`1`), PublishedAt: time.Now()}))
	require.NoError(t, rt.Publish(ctx, core.Message{Topic: "t", Payload: []byte(`2`), PublishedAt: time.Now()}))
	require.NoError(t, rt.EnsureConsumerGroup(ctx, "t", "g"))

	// miniredis 的 XINFO GROUPS lag 实现是「stream 总长度」，与真实 Redis 语义
	// 不同，因此这里只断言不报错；精确 lag 语义由真实 Redis e2e 测试覆盖。
	lag, err = rt.StreamLag(ctx, "t", "g")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, lag, int64(0))

	msgs, err := rt.Consume(ctx, core.ConsumerConfig{
		Topic: "t", Group: "g", PollBatchSize: 1, PollBlock: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	pending, err = rt.PendingCount(ctx, "t", "g")
	require.NoError(t, err)
	assert.Equal(t, int64(1), pending, "消费未 Ack 的消息应计入 Pending")

	require.NoError(t, rt.Ack(ctx, msgs[0]))
	pending, err = rt.PendingCount(ctx, "t", "g")
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending)

	// 重试队列长度。
	for _, id := range []string{"r1", "r2"} {
		require.NoError(t, rt.ScheduleRetry(ctx, core.RetryTask{
			MessageID:   id,
			Topic:       "t",
			Group:       "g",
			Payload:     []byte(`1`),
			Attempt:     1,
			MaxRetries:  3,
			AvailableAt: time.Now().Add(-time.Second),
		}))
	}
	retryLen, err = rt.RetryQueueLen(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), retryLen)

	tasks, err := rt.PollRetry(ctx, 1, time.Now())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.NoError(t, rt.RemoveRetry(ctx, tasks[0]))

	retryLen, err = rt.RetryQueueLen(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), retryLen)
}

// PendingCount 在 group 不存在（stream 存在）时也应返回 0。
func TestRedisRuntime_PendingCountMissingGroup(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "stats-missing"})
	ctx := context.Background()

	require.NoError(t, rt.Publish(ctx, core.Message{Topic: "t", Payload: []byte(`1`), PublishedAt: time.Now()}))

	pending, err := rt.PendingCount(ctx, "t", "missing-group")
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending)

	lag, err := rt.StreamLag(ctx, "t", "missing-group")
	require.NoError(t, err)
	assert.Equal(t, int64(0), lag)
}
