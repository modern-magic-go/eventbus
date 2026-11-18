package redisruntime

import (
	"context"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/modern-magic-go/eventbus/internal/core"
)

// EnsureConsumerGroupFrom 支持 "0"（从头）与 "$"（只看新消息）。
func TestRedisRuntime_EnsureConsumerGroupFrom(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "start"})
	ctx := context.Background()

	// start="$"：组创建前的历史消息不可见。
	require.NoError(t, rt.Publish(ctx, core.Message{ID: "old", Topic: "latest", Payload: []byte(`old`), PublishedAt: time.Now()}))
	require.NoError(t, rt.EnsureConsumerGroupFrom(ctx, "latest", "g", core.StartFromLatest))

	msgs, err := rt.Consume(ctx, core.ConsumerConfig{
		Topic: "latest", Group: "g", PollBatchSize: 1, PollBlock: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	assert.Empty(t, msgs, "start=\"$\" 不应消费组创建前的历史消息")

	// 创建后的新消息可见。
	require.NoError(t, rt.Publish(ctx, core.Message{ID: "new", Topic: "latest", Payload: []byte(`new`), PublishedAt: time.Now()}))
	msgs, err = rt.Consume(ctx, core.ConsumerConfig{
		Topic: "latest", Group: "g", PollBatchSize: 1, PollBlock: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "new", msgs[0].ID)

	// start="0"：历史消息可见（默认行为）。
	require.NoError(t, rt.Publish(ctx, core.Message{ID: "hist", Topic: "beginning", Payload: []byte(`hist`), PublishedAt: time.Now()}))
	require.NoError(t, rt.EnsureConsumerGroupFrom(ctx, "beginning", "g", core.StartFromBeginning))
	msgs, err = rt.Consume(ctx, core.ConsumerConfig{
		Topic: "beginning", Group: "g", PollBatchSize: 1, PollBlock: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "hist", msgs[0].ID)

	// BUSYGROUP 忽略：已存在的组不会被重建 / 改变 offset。
	require.NoError(t, rt.EnsureConsumerGroupFrom(ctx, "latest", "g", core.StartFromBeginning))
	assert.Empty(t, mustConsume(t, rt, "latest", "g"))
}

// start 支持 unix 毫秒时间戳 / stream ID，直接透传给 XGROUP CREATE。
func TestRedisRuntime_EnsureConsumerGroupFromTimestamp(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "start-ts"})
	ctx := context.Background()
	streamKey := "start-ts:topic:ts"

	// 用显式 ID 写入两条历史消息，保证时间戳断言确定。
	for _, id := range []string{"1700000000000-0", "1700000000001-0"} {
		require.NoError(t, client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			ID:     id,
			Values: map[string]any{"payload": []byte(id), "published_at": time.Now().Unix()},
		}).Err())
	}

	// 从第一个 ID 之后开始：只能消费到第二条。
	require.NoError(t, rt.EnsureConsumerGroupFrom(ctx, "ts", "g", "1700000000000-0"))
	msgs, err := rt.Consume(ctx, core.ConsumerConfig{
		Topic: "ts", Group: "g", PollBatchSize: 10, PollBlock: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "1700000000001-0", msgs[0].DeliveryTag)
}

func mustConsume(t *testing.T, rt *RedisRuntime, topic, group string) []core.Message {
	t.Helper()
	msgs, err := rt.Consume(context.Background(), core.ConsumerConfig{
		Topic: topic, Group: group, PollBatchSize: 10, PollBlock: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	return msgs
}
