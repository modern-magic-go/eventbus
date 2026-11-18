package redisruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/modern-magic-go/eventbus/internal/core"
)

// 同一 MessageID 重复调度必须幂等覆盖：ZSET 只保留一个 member，
// HASH 只保留一份 payload，PollRetry 返回最新的 attempt。
func TestRedisRuntime_ScheduleRetryIsIdempotentByMessageID(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "retry"})
	ctx := context.Background()

	now := time.Now()
	first := core.RetryTask{
		MessageID:   "m-1",
		Topic:       "t",
		Group:       "g",
		Payload:     []byte(`{"id":1}`),
		Attempt:     1,
		MaxRetries:  3,
		AvailableAt: now.Add(-time.Second),
		LastError:   "boom-1",
	}
	require.NoError(t, rt.ScheduleRetry(ctx, first))

	second := first
	second.Attempt = 2
	second.LastError = "boom-2"
	second.AvailableAt = now.Add(time.Minute)
	require.NoError(t, rt.ScheduleRetry(ctx, second))

	assert.Equal(t, "retry:retry:data", rt.cfg.RetryDataKey)
	member := "mid:g:m-1"

	zcard, err := client.ZCard(ctx, "retry:retry").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), zcard, "同一 MessageID 重复调度不能追加第二条 ZSET member")

	hlen, err := client.HLen(ctx, "retry:retry:data").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), hlen)

	score, err := client.ZScore(ctx, "retry:retry", member).Result()
	require.NoError(t, err)
	assert.InDelta(t, float64(second.AvailableAt.Unix()), score, 0, "重复调度应刷新 score")

	raw, err := client.HGet(ctx, "retry:retry:data", member).Result()
	require.NoError(t, err)
	var stored core.RetryTask
	require.NoError(t, json.Unmarshal([]byte(raw), &stored))
	assert.Equal(t, 2, stored.Attempt, "HASH 中应是最后一次调度的 payload")
	assert.Equal(t, "boom-2", stored.LastError)

	tasks, err := rt.PollRetry(ctx, 10, now.Add(2*time.Minute))
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "m-1", tasks[0].MessageID)
	assert.Equal(t, 2, tasks[0].Attempt)
	assert.Equal(t, member, core.RawMember(tasks[0]))

	// RemoveRetry 必须同时清理 ZSET 与 HASH。
	require.NoError(t, rt.RemoveRetry(ctx, tasks[0]))
	zcard, err = client.ZCard(ctx, "retry:retry").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), zcard)
	hlen, err = client.HLen(ctx, "retry:retry:data").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), hlen)
}

// RemoveRetry 在任务没有 RawMember 时按 MessageID 重新计算 member。
func TestRedisRuntime_RemoveRetryWithoutRawMember(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "retry-remove"})
	ctx := context.Background()

	task := core.RetryTask{
		MessageID:   "m-2",
		Topic:       "t",
		Group:       "g",
		Payload:     []byte(`1`),
		Attempt:     1,
		MaxRetries:  3,
		AvailableAt: time.Now().Add(-time.Second),
	}
	require.NoError(t, rt.ScheduleRetry(ctx, task))

	require.NoError(t, rt.RemoveRetry(ctx, task))
	zcard, err := client.ZCard(ctx, "retry-remove:retry").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), zcard)
	hlen, err := client.HLen(ctx, "retry-remove:retry:data").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), hlen)
}

// v0.2 数据兼容：member 就是任务 JSON，PollRetry 应能读出并带上 RawMember，
// RemoveRetry 能精确删除（HDEL 对旧 member 是 no-op）。
func TestRedisRuntime_RetryLegacyJSONMemberCompatible(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "legacy"})
	ctx := context.Background()

	legacy := core.RetryTask{
		MessageID:   "old-msg", // v0.2 也会写 MessageID，但仍然用 JSON 作为 member
		Topic:       "t",
		Group:       "g",
		Payload:     []byte(`{"legacy":true}`),
		Attempt:     1,
		MaxRetries:  3,
		AvailableAt: time.Now().Add(-time.Second),
	}
	blob, err := json.Marshal(legacy)
	require.NoError(t, err)
	legacyMember := string(blob)
	require.NoError(t, client.ZAdd(ctx, "legacy:retry", redis.Z{Score: float64(legacy.AvailableAt.Unix()), Member: legacyMember}).Err())

	tasks, err := rt.PollRetry(ctx, 10, time.Now())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "old-msg", tasks[0].MessageID)
	assert.Equal(t, "{\"legacy\":true}", string(tasks[0].Payload))
	assert.Equal(t, legacyMember, core.RawMember(tasks[0]))

	require.NoError(t, rt.RemoveRetry(ctx, tasks[0]))
	zcard, err := client.ZCard(ctx, "legacy:retry").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), zcard)
}

// 无 MessageID 的任务退化为 JSON member（v0.2 行为），不写稳定 member。
func TestRedisRuntime_RetryWithoutMessageIDUsesJSONMember(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "noid"})
	ctx := context.Background()

	task := core.RetryTask{
		Topic:       "t",
		Group:       "g",
		Payload:     []byte(`1`),
		Attempt:     1,
		MaxRetries:  3,
		AvailableAt: time.Now().Add(-time.Second),
	}
	require.NoError(t, rt.ScheduleRetry(ctx, task))

	members, err := client.ZRange(ctx, "noid:retry", 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, members, 1)
	assert.False(t, strings.HasPrefix(members[0], retryMemberPrefix))
	var decoded core.RetryTask
	require.NoError(t, json.Unmarshal([]byte(members[0]), &decoded))
	assert.Equal(t, "t", decoded.Topic)

	tasks, err := rt.PollRetry(ctx, 10, time.Now())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.NoError(t, rt.RemoveRetry(ctx, tasks[0]))
}

// HASH payload 丢失（脏数据）时应清理 ZSET 索引并跳过，不能永久卡住重试轮询。
func TestRedisRuntime_PollRetryCleansOrphanMember(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "orphan"})
	ctx := context.Background()

	member := "mid:g:ghost"
	require.NoError(t, client.ZAdd(ctx, "orphan:retry", redis.Z{Score: float64(time.Now().Add(-time.Second).Unix()), Member: member}).Err())

	tasks, err := rt.PollRetry(ctx, 10, time.Now())
	require.NoError(t, err)
	assert.Empty(t, tasks)

	zcard, err := client.ZCard(ctx, "orphan:retry").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), zcard, "缺失 payload 的 member 应被清理")
}

// RetryDataKey / RetryQueueKey 可自定义；未指定时按 Namespace 推导默认值。
func TestRedisRuntime_RetryKeysConfig(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{
		Namespace:     "custom",
		RetryQueueKey: "custom:queue",
		RetryDataKey:  "custom:data",
	})
	ctx := context.Background()

	task := core.RetryTask{
		MessageID:   "m-3",
		Topic:       "t",
		Group:       "g",
		Payload:     []byte(`1`),
		Attempt:     1,
		MaxRetries:  3,
		AvailableAt: time.Now().Add(-time.Second),
	}
	require.NoError(t, rt.ScheduleRetry(ctx, task))

	zcard, err := client.ZCard(ctx, "custom:queue").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), zcard)
	hlen, err := client.HLen(ctx, "custom:data").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), hlen)
}

// v0.2 遗留数据（member 是任务 JSON，但 JSON 里带 MessageID）被 PollRetry 取出后
// 重新调度时，必须迁移为新的稳定 "mid:" member，不能新旧 member 并存导致重复重试。
func TestRedisRuntime_ScheduleRetryMigratesLegacyMember(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "migrate"})
	ctx := context.Background()

	legacy := core.RetryTask{
		MessageID:   "m-9",
		Topic:       "t",
		Group:       "g",
		Payload:     []byte(`{"legacy":true}`),
		Attempt:     1,
		MaxRetries:  3,
		AvailableAt: time.Now().Add(-time.Second),
	}
	blob, err := json.Marshal(legacy)
	require.NoError(t, err)
	require.NoError(t, client.ZAdd(ctx, "migrate:retry", redis.Z{
		Score:  float64(legacy.AvailableAt.Unix()),
		Member: string(blob),
	}).Err())

	tasks, err := rt.PollRetry(ctx, 10, time.Now())
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	next := tasks[0]
	next.Attempt = 2
	next.LastError = "boom-again"
	next.AvailableAt = time.Now().Add(-time.Second)
	require.NoError(t, rt.ScheduleRetry(ctx, next))

	members, err := client.ZRange(ctx, "migrate:retry", 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, members, 1, "旧 JSON member 应被迁移删除，只保留稳定 member")
	assert.Equal(t, "mid:g:m-9", members[0])

	hlen, err := client.HLen(ctx, "migrate:retry:data").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), hlen)

	polled, err := rt.PollRetry(ctx, 10, time.Now())
	require.NoError(t, err)
	require.Len(t, polled, 1)
	assert.Equal(t, 2, polled[0].Attempt)
}

// 显式 RetryTask.Key 使用独立 member，可与同 MessageID 的稳定 member 任务共存
// （DLQ 重放场景，避免与正在清理的旧任务互相覆盖）。
func TestRedisRuntime_RetryKeyOverridesMember(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "retrykey"})
	ctx := context.Background()
	now := time.Now()

	stable := core.RetryTask{
		MessageID: "m-replay", Topic: "t", Group: "g", Payload: []byte(`1`),
		Attempt: 1, MaxRetries: 3, AvailableAt: now.Add(-time.Second),
	}
	replayed := stable
	replayed.Key = "replay-1"
	require.NoError(t, rt.ScheduleRetry(ctx, stable))
	require.NoError(t, rt.ScheduleRetry(ctx, replayed))

	zcard, err := client.ZCard(ctx, "retrykey:retry").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), zcard, "Key 覆盖值应与稳定 member 共存")

	members, err := client.ZRange(ctx, "retrykey:retry", 0, -1).Result()
	require.NoError(t, err)
	assert.Contains(t, members, "mid:g:m-replay")
	assert.Contains(t, members, "key:replay-1")

	tasks, err := rt.PollRetry(ctx, 10, now)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	for _, task := range tasks {
		require.NoError(t, rt.RemoveRetry(ctx, task))
	}
	zcard, err = client.ZCard(ctx, "retrykey:retry").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), zcard)
	hlen, err := client.HLen(ctx, "retrykey:retry:data").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), hlen)
}

// 无法反序列化的 member 必须被清理，否则会永久占据 ZSET 头部、把合法任务挤出 limit 窗口。
func TestRedisRuntime_PollRetryCleansBadLegacyMember(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "retrybad"})
	ctx := context.Background()
	now := time.Now()

	for i := 0; i < 10; i++ {
		require.NoError(t, client.ZAdd(ctx, "retrybad:retry", redis.Z{
			Score:  float64(now.Add(-time.Minute).Unix()),
			Member: fmt.Sprintf("not-json-%d", i),
		}).Err())
	}
	legit := core.RetryTask{
		Topic: "t", Group: "g", Payload: []byte(`1`),
		Attempt: 1, MaxRetries: 3, AvailableAt: now.Add(-time.Second),
	}
	raw, err := json.Marshal(legit)
	require.NoError(t, err)
	require.NoError(t, client.ZAdd(ctx, "retrybad:retry", redis.Z{
		Score: float64(now.Add(-time.Minute).Unix()), Member: string(raw),
	}).Err())

	// 第一轮把 10 条坏 member 清理掉（返回 0 条合法任务）。
	tasks, err := rt.PollRetry(ctx, 10, now)
	require.NoError(t, err)
	assert.Empty(t, tasks)

	// 第二轮能取到合法任务，说明坏数据没有永久阻塞队列。
	tasks, err = rt.PollRetry(ctx, 10, now)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "t", tasks[0].Topic)
}

// XAUTOCLAIM raw 回复解析：Redis 7 的 deleted IDs 与 Redis 6.2 的两元素兼容。
func TestParseXAutoClaimReply(t *testing.T) {
	redis7 := []any{
		"1-2",
		[]any{
			[]any{"1-1", []any{"payload", "x", "message_id", "m1"}},
		},
		[]any{"0-9", "0-8"},
	}
	reply, err := parseXAutoClaimReply(redis7)
	require.NoError(t, err)
	assert.Equal(t, "1-2", reply.next)
	require.Len(t, reply.messages, 1)
	assert.Equal(t, "1-1", reply.messages[0].ID)
	assert.Equal(t, []string{"0-9", "0-8"}, reply.deleted)

	redis62 := []any{"0-0", []any{}}
	reply, err = parseXAutoClaimReply(redis62)
	require.NoError(t, err)
	assert.Empty(t, reply.deleted)
	assert.Empty(t, reply.messages)

	_, err = parseXAutoClaimReply("unexpected")
	require.Error(t, err)
}
