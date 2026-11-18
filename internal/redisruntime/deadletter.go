package redisruntime

import (
	"context"
	"encoding/json"
	"errors"

	redis "github.com/redis/go-redis/v9"

	"github.com/modern-magic-go/eventbus/internal/core"
)

// ListDeadLetters 按 LRANGE 语义分页返回死信记录（index 0 为最新写入）。
//
// 坏数据（无法反序列化的条目）会被跳过而不是让整页失败；v0.2.0 写入的旧条目
// 没有 ID 字段，解析后 ID 为空，调用方重放时由 Runtime 重新生成，不属于错误。
func (r *RedisRuntime) ListDeadLetters(ctx context.Context, offset, limit int) ([]core.DeadLetterMessage, error) {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = core.DefaultPollBatchSize
	}

	raws, err := r.client.LRange(ctx, r.cfg.DeadLetterKey, int64(offset), int64(offset+limit-1)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	messages := make([]core.DeadLetterMessage, 0, len(raws))
	for _, raw := range raws {
		var msg core.DeadLetterMessage
		if unmarshalErr := json.Unmarshal([]byte(raw), &msg); unmarshalErr != nil {
			continue
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

// PopDeadLetters 从死信队列尾部（最旧优先）弹出最多 limit 条记录。
//
// 弹出即从队列移除；坏数据也已被弹出，直接丢弃，避免阻塞后续重放。
func (r *RedisRuntime) PopDeadLetters(ctx context.Context, limit int) ([]core.DeadLetterMessage, error) {
	if limit <= 0 {
		limit = core.DefaultPollBatchSize
	}

	messages := make([]core.DeadLetterMessage, 0, limit)
	for i := 0; i < limit; i++ {
		raw, err := r.client.RPop(ctx, r.cfg.DeadLetterKey).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				break
			}
			return messages, err
		}
		var msg core.DeadLetterMessage
		if unmarshalErr := json.Unmarshal([]byte(raw), &msg); unmarshalErr != nil {
			continue
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

// DeadLetterLen 返回死信队列当前长度。
func (r *RedisRuntime) DeadLetterLen(ctx context.Context) (int64, error) {
	return r.client.LLen(ctx, r.cfg.DeadLetterKey).Result()
}
