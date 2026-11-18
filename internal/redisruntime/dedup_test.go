package redisruntime

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/modern-magic-go/eventbus/internal/core"
)

func TestDedupStore_TwoPhaseSemantics(t *testing.T) {
	_, client := newTestRedis(t)
	store := NewDedupStore(client, DedupStoreConfig{Namespace: "dedup"})
	ctx := context.Background()

	status, err := store.Acquire(ctx, "topic|group|msg-1", time.Second)
	require.NoError(t, err)
	assert.Equal(t, core.DedupAcquired, status, "首次 Acquire 应占位成功")

	// 占位存在但未完成：返回 InProgress（调用方应继续处理，避免崩溃残留导致丢消息）。
	status, err = store.Acquire(ctx, "topic|group|msg-1", time.Second)
	require.NoError(t, err)
	assert.Equal(t, core.DedupInProgress, status)

	// handler 失败后 Release 未完成占位：可再次占位。
	require.NoError(t, store.Release(ctx, "topic|group|msg-1"))
	status, err = store.Acquire(ctx, "topic|group|msg-1", time.Second)
	require.NoError(t, err)
	assert.Equal(t, core.DedupAcquired, status)

	// MarkDone 后返回 Done（可安全跳过）。
	require.NoError(t, store.MarkDone(ctx, "topic|group|msg-1", time.Minute))
	status, err = store.Acquire(ctx, "topic|group|msg-1", time.Second)
	require.NoError(t, err)
	assert.Equal(t, core.DedupDone, status)

	// key 前缀与 TTL 生效。
	key := "dedup:dedup:topic|group|msg-1"
	val, err := client.Get(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, dedupValueDone, val)
	pttl, err := client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	assert.Greater(t, pttl, time.Duration(0))

	// 并发场景：失败方的 Release 不得清除已完成标记（done 是终态，保留到 TTL）。
	require.NoError(t, store.Release(ctx, "topic|group|msg-1"))
	status, err = store.Acquire(ctx, "topic|group|msg-1", time.Second)
	require.NoError(t, err)
	assert.Equal(t, core.DedupDone, status, "Release 不能删除 done 标记")

	// 新 key 首次 Acquire 返回 Acquired。
	status, err = store.Acquire(ctx, "fresh-key", time.Second)
	require.NoError(t, err)
	require.Equal(t, core.DedupAcquired, status)
}

func TestDedupStore_TTLExpiry(t *testing.T) {
	mr, client := newTestRedis(t)
	store := NewDedupStore(client, DedupStoreConfig{Namespace: "dedup"})
	ctx := context.Background()

	status, err := store.Acquire(ctx, "expiring", time.Second)
	require.NoError(t, err)
	require.Equal(t, core.DedupAcquired, status)
	require.NoError(t, store.MarkDone(ctx, "expiring", time.Second))

	mr.FastForward(2 * time.Second)

	status, err = store.Acquire(ctx, "expiring", time.Second)
	require.NoError(t, err)
	assert.Equal(t, core.DedupAcquired, status, "TTL 到期后应可重新占位")
}

func TestDedupStore_ConfigDefaults(t *testing.T) {
	_, client := newTestRedis(t)
	ctx := context.Background()

	// 默认 namespace：eventbus:dedup:。
	store := NewDedupStore(client, DedupStoreConfig{})
	status, err := store.Acquire(ctx, "k", time.Minute)
	require.NoError(t, err)
	require.Equal(t, core.DedupAcquired, status)
	exists, err := client.Exists(ctx, "eventbus:dedup:k").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists)

	// 自定义 Prefix 优先于 Namespace。
	custom := NewDedupStore(client, DedupStoreConfig{Namespace: "ignored", Prefix: "custom:"})
	status, err = custom.Acquire(ctx, "k2", time.Minute)
	require.NoError(t, err)
	require.Equal(t, core.DedupAcquired, status)
	exists, err = client.Exists(ctx, "custom:k2").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists)

	// ttl <= 0 时使用默认 TTL（不退化为永不过期）。
	status, err = store.Acquire(ctx, "no-ttl", 0)
	require.NoError(t, err)
	require.Equal(t, core.DedupAcquired, status)
	pttl, err := client.PTTL(ctx, "eventbus:dedup:no-ttl").Result()
	require.NoError(t, err)
	assert.Greater(t, pttl, time.Duration(0))
}
