package redisruntime

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/modern-magic-go/eventbus/internal/core"
)

// MoveToDeadLetter 写入时生成记录 ID；ListDeadLetters 按「最新在前」分页；
// PopDeadLetters 从最旧开始弹出并移除；DeadLetterLen 反映剩余数量。
func TestRedisRuntime_DeadLetterStoreOrdering(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "dlq"})
	ctx := context.Background()
	now := time.Now()

	for i, errText := range []string{"e1", "e2", "e3"} {
		require.NoError(t, rt.MoveToDeadLetter(ctx, core.DeadLetterMessage{
			MessageID:  "msg-" + errText,
			Topic:      "t",
			Group:      "g",
			Payload:    []byte(`{"n":` + string(rune('0'+i)) + `}`),
			Attempt:    3,
			MaxRetries: 3,
			Error:      errText,
			OccurredAt: now.Add(time.Duration(i) * time.Second),
		}))
	}

	list, err := rt.ListDeadLetters(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, list, 3)
	// LPUSH：index 0 是最新写入的记录。
	assert.Equal(t, "e3", list[0].Error)
	assert.Equal(t, "e2", list[1].Error)
	assert.Equal(t, "e1", list[2].Error)
	ids := map[string]struct{}{}
	for _, msg := range list {
		require.NotEmpty(t, msg.ID, "Runtime 必须为死信记录生成 ID")
		_, duplicated := ids[msg.ID]
		assert.False(t, duplicated, "死信 ID 不能重复")
		ids[msg.ID] = struct{}{}
		assert.Equal(t, "msg-"+msg.Error, msg.MessageID)
		assert.Equal(t, 3, msg.Attempt)
		assert.False(t, msg.OccurredAt.IsZero())
	}

	page, err := rt.ListDeadLetters(ctx, 1, 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	assert.Equal(t, "e2", page[0].Error)

	length, err := rt.DeadLetterLen(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(3), length)

	popped, err := rt.PopDeadLetters(ctx, 2)
	require.NoError(t, err)
	require.Len(t, popped, 2)
	assert.Equal(t, "e1", popped[0].Error, "Pop 应从最旧的一条开始")
	assert.Equal(t, "e2", popped[1].Error)

	length, err = rt.DeadLetterLen(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), length)

	list, err = rt.ListDeadLetters(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "e3", list[0].Error)

	popped, err = rt.PopDeadLetters(ctx, 10)
	require.NoError(t, err)
	require.Len(t, popped, 1)
	assert.Equal(t, "e3", popped[0].Error)

	length, err = rt.DeadLetterLen(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), length)

	empty, err := rt.PopDeadLetters(ctx, 5)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

// 调用方显式提供 ID 时不应被覆盖。
func TestRedisRuntime_DeadLetterPreservesProvidedID(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "dlq-id"})
	ctx := context.Background()

	require.NoError(t, rt.MoveToDeadLetter(ctx, core.DeadLetterMessage{
		ID:         "custom-id",
		Topic:      "t",
		Group:      "g",
		Payload:    []byte(`1`),
		Error:      "boom",
		OccurredAt: time.Now(),
	}))

	list, err := rt.ListDeadLetters(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "custom-id", list[0].ID)
}

// v0.2 旧条目没有 ID，应能正常读出（ID 为空不报错）；坏数据跳过。
func TestRedisRuntime_DeadLetterLegacyAndMalformedEntries(t *testing.T) {
	_, client := newTestRedis(t)
	rt := NewRedisRuntime(client, Config{Namespace: "dlq-legacy"})
	ctx := context.Background()
	key := "dlq-legacy:dlq"

	// 旧格式（v0.2）：没有 id 字段。
	require.NoError(t, client.LPush(ctx, key, `{"message_id":"m-old","topic":"legacy","group":"g","payload":"MQ==","error":"old","occurred_at":"2024-01-01T00:00:00Z"}`).Err())
	// 坏数据：应被 List/Pop 跳过。
	require.NoError(t, client.LPush(ctx, key, `{not-json`).Err())
	// 新格式，显式 ID。
	require.NoError(t, rt.MoveToDeadLetter(ctx, core.DeadLetterMessage{
		ID:         "new-id",
		MessageID:  "m-new",
		Topic:      "t",
		Group:      "g",
		Payload:    []byte(`2`),
		Error:      "new",
		OccurredAt: time.Now(),
	}))

	list, err := rt.ListDeadLetters(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, list, 2, "坏数据应被跳过")
	assert.Equal(t, "new-id", list[0].ID)
	assert.Equal(t, "legacy", list[1].Topic)
	assert.Empty(t, list[1].ID, "旧条目没有 ID，解析后应为空而不是报错")
	assert.Equal(t, "m-old", list[1].MessageID)

	// Pop 会弹出坏数据并丢弃，最终返回两条有效记录（最旧优先）。
	popped, err := rt.PopDeadLetters(ctx, 10)
	require.NoError(t, err)
	require.Len(t, popped, 2)
	assert.Equal(t, "legacy", popped[0].Topic)
	assert.Equal(t, "new-id", popped[1].ID)

	length, err := rt.DeadLetterLen(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), length)
}
