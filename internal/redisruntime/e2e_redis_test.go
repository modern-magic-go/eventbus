package redisruntime

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/modern-magic-go/eventbus/internal/core"
)

// 真实 Redis e2e 测试（默认跳过）：
//
//	EVENTBUS_E2E_REDIS_ADDR=127.0.0.1:6379 go test ./internal/redisruntime/ -run TestRedisRuntime_E2E -v
//
// 与 internal/integration 的 e2e 约定一致：默认使用 DB 14，运行前后都 FLUSHDB，
// 因此必须指向独占（或允许清空）的库。
func newE2ERedis(t *testing.T) *redis.Client {
	t.Helper()
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
	require.NoError(t, client.Ping(ctx).Err())
	require.NoError(t, client.FlushDB(ctx).Err(), "e2e 需要独占 DB %d", db)
	t.Cleanup(func() {
		_ = client.FlushDB(context.Background()).Err()
		_ = client.Close()
	})
	return client
}

// 真实 Redis 是 Redis 7，XAUTOCLAIM 回复带第三个元素（deleted IDs）：
// 验证 raw 命令解析可用，且被 StreamMaxLen 裁剪掉、仍留在 PEL 的孤儿消息
// 会被 ClaimPending 清理（XPENDING 归零）。
func TestRedisRuntime_E2E(t *testing.T) {
	client := newE2ERedis(t)

	t.Run("runtime_stats", func(t *testing.T) {
		rt := NewRedisRuntime(client, Config{Namespace: "e2e-stats", ConsumerName: "worker-stats"})
		ctx := context.Background()

		require.NoError(t, rt.Publish(ctx, core.Message{ID: "s-1", Topic: "t", Payload: []byte(`1`), PublishedAt: time.Now()}))
		require.NoError(t, rt.Publish(ctx, core.Message{ID: "s-2", Topic: "t", Payload: []byte(`2`), PublishedAt: time.Now()}))
		require.NoError(t, rt.EnsureConsumerGroup(ctx, "t", "g"))

		lag, err := rt.StreamLag(ctx, "t", "g")
		require.NoError(t, err)
		assert.Equal(t, int64(2), lag, "组刚创建时两条都未投递")

		pending, err := rt.PendingCount(ctx, "t", "g")
		require.NoError(t, err)
		assert.Equal(t, int64(0), pending)

		// 只消费 1 条（不 Ack）：last-delivered-id 前进，lag 应变为 1。
		msgs, err := rt.Consume(ctx, core.ConsumerConfig{
			Topic: "t", Group: "g", PollBatchSize: 1, PollBlock: 10 * time.Millisecond,
		})
		require.NoError(t, err)
		require.Len(t, msgs, 1)

		lag, err = rt.StreamLag(ctx, "t", "g")
		require.NoError(t, err)
		assert.Equal(t, int64(1), lag)

		pending, err = rt.PendingCount(ctx, "t", "g")
		require.NoError(t, err)
		assert.Equal(t, int64(1), pending)

		require.NoError(t, rt.Ack(ctx, msgs[0]))
		pending, err = rt.PendingCount(ctx, "t", "g")
		require.NoError(t, err)
		assert.Equal(t, int64(0), pending)

		retryLen, err := rt.RetryQueueLen(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), retryLen)
	})

	t.Run("xautoclaim_cleans_trimmed_pel_orphan", func(t *testing.T) {
		rt := NewRedisRuntime(client, Config{Namespace: "e2e-orphan", ConsumerName: "worker-orphan"})
		ctx := context.Background()
		streamKey := "e2e-orphan:topic:orphan"

		require.NoError(t, rt.EnsureConsumerGroup(ctx, "orphan", "g"))
		require.NoError(t, rt.Publish(ctx, core.Message{
			ID: "orphan-1", Topic: "orphan", Payload: []byte(`{"n":1}`), PublishedAt: time.Now(),
		}))

		// 消费但不 Ack，制造 PEL 中的 pending 消息。
		msgs, err := rt.Consume(ctx, core.ConsumerConfig{
			Topic: "orphan", Group: "g", PollBatchSize: 1, PollBlock: 10 * time.Millisecond,
		})
		require.NoError(t, err)
		require.Len(t, msgs, 1)

		pending, err := client.XPending(ctx, streamKey, "g").Result()
		require.NoError(t, err)
		require.Equal(t, int64(1), pending.Count)

		// 用 MAXLEN 裁剪把仍 pending 的消息从 stream 中删掉（PEL 条目保留）。
		for i := 0; i < 5; i++ {
			require.NoError(t, client.XAdd(ctx, &redis.XAddArgs{
				Stream: streamKey,
				MaxLen: 1,
				Values: map[string]any{"payload": []byte("trimmed"), "published_at": time.Now().Unix()},
			}).Err())
		}
		require.Equal(t, int64(1), client.XLen(ctx, streamKey).Val(), "MAXLEN=1 应只保留最新一条")

		// 被裁剪的消息仍留在 PEL 中，等待 XAUTOCLAIM 处理。
		pending, err = client.XPending(ctx, streamKey, "g").Result()
		require.NoError(t, err)
		require.Equal(t, int64(1), pending.Count)

		// 等 idle 超过 MinIdle。
		time.Sleep(50 * time.Millisecond)

		claimed, err := rt.ClaimPending(ctx, core.ConsumerConfig{
			Topic: "orphan",
			Group: "g",
			PendingReclaim: core.PendingReclaimConfig{
				MinIdle: 10 * time.Millisecond,
				Batch:   10,
			},
		})
		require.NoError(t, err)
		assert.Empty(t, claimed, "孤儿消息已不在 stream 中，不应被认领")

		pending, err = client.XPending(ctx, streamKey, "g").Result()
		require.NoError(t, err)
		assert.Equal(t, int64(0), pending.Count, "PEL 孤儿应被清理，pending 归零")
	})

	t.Run("start_from_latest", func(t *testing.T) {
		rt := NewRedisRuntime(client, Config{Namespace: "e2e-start", ConsumerName: "worker-start"})
		ctx := context.Background()

		require.NoError(t, rt.Publish(ctx, core.Message{ID: "hist", Topic: "latest", Payload: []byte(`1`), PublishedAt: time.Now()}))
		require.NoError(t, rt.EnsureConsumerGroupFrom(ctx, "latest", "g", core.StartFromLatest))

		msgs, err := rt.Consume(ctx, core.ConsumerConfig{
			Topic: "latest", Group: "g", PollBatchSize: 1, PollBlock: 10 * time.Millisecond,
		})
		require.NoError(t, err)
		assert.Empty(t, msgs, "start=\"$\" 不应消费历史消息")

		require.NoError(t, rt.Publish(ctx, core.Message{ID: "new", Topic: "latest", Payload: []byte(`2`), PublishedAt: time.Now()}))
		msgs, err = rt.Consume(ctx, core.ConsumerConfig{
			Topic: "latest", Group: "g", PollBatchSize: 1, PollBlock: 10 * time.Millisecond,
		})
		require.NoError(t, err)
		require.Len(t, msgs, 1)
		assert.Equal(t, "new", msgs[0].ID)
	})
}
