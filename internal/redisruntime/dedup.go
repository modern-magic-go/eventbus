package redisruntime

import (
	"context"
	"errors"
	"time"

	redis "github.com/redis/go-redis/v9"

	"github.com/modern-magic-go/eventbus/internal/core"
)

// DedupStoreConfig 配置 Redis 消费端幂等去重存储。
type DedupStoreConfig struct {
	// Namespace 是默认 key 前缀的命名空间，默认 "eventbus"。
	Namespace string
	// Prefix 是完整 key 前缀，优先级高于 Namespace，默认 "<Namespace>:dedup:"。
	Prefix string
}

var _ core.DedupStore = (*DedupStore)(nil)

const (
	// dedupValueInProgress 表示占位已被某个处理者取得，但尚未确认处理成功。
	dedupValueInProgress = "pending"
	// dedupValueDone 表示消息已成功处理过。
	dedupValueDone = "done"
)

// DedupStore 是基于 Redis 的 core.DedupStore 两阶段实现（占位 → 完成）。
//
// 去重语义：
//   - Acquire 用 SET NX 占位；占位已存在时读取状态：
//     值为 "done" 返回 DedupDone（可安全跳过），否则返回 DedupInProgress
//     （处理中或崩溃残留，调用方应继续处理以免丢消息）；
//   - MarkDone 在 handler 成功后把键标记为 done 并保留 TTL；
//   - Release 删除占位，使失败重试可以再次处理同一条消息。
type DedupStore struct {
	client *redis.Client
	prefix string
}

// releasePendingScript 仅删除值为 "pending" 的占位：
// 并发场景下，处理失败的一方不能在另一方已 MarkDone 之后把完成标记删掉，
// 否则已成功处理的消息会在后续投递时被再次执行（重复副作用）。
var releasePendingScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
`)

// NewDedupStore 创建 Redis 去重存储。client 由调用方管理生命周期。
func NewDedupStore(client *redis.Client, cfg DedupStoreConfig) *DedupStore {
	if cfg.Namespace == "" {
		cfg.Namespace = "eventbus"
	}
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = cfg.Namespace + ":dedup:"
	}
	return &DedupStore{client: client, prefix: prefix}
}

// Acquire 尝试占位：SET key "pending" NX PX ttl；已存在时读取状态。
// ttl <= 0 时使用 core.DefaultDedupTTL，避免写入永不过期的去重键。
func (s *DedupStore) Acquire(ctx context.Context, key string, ttl time.Duration) (core.DedupStatus, error) {
	if ttl <= 0 {
		ttl = core.DefaultDedupTTL
	}
	fullKey := s.prefix + key

	acquired, err := s.client.SetNX(ctx, fullKey, dedupValueInProgress, ttl).Result()
	if err != nil {
		return core.DedupInProgress, err
	}
	if acquired {
		return core.DedupAcquired, nil
	}

	value, err := s.client.Get(ctx, fullKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// 键恰好在 SETNX 与 GET 之间过期/被删除：重试一次占位。
			acquired, err = s.client.SetNX(ctx, fullKey, dedupValueInProgress, ttl).Result()
			if err != nil {
				return core.DedupInProgress, err
			}
			if acquired {
				return core.DedupAcquired, nil
			}
			return core.DedupInProgress, nil
		}
		return core.DedupInProgress, err
	}
	if value == dedupValueDone {
		return core.DedupDone, nil
	}
	return core.DedupInProgress, nil
}

// MarkDone 在 handler 成功后把占位标记为已完成，并保留去重 TTL。
func (s *DedupStore) MarkDone(ctx context.Context, key string, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = core.DefaultDedupTTL
	}
	return s.client.Set(ctx, s.prefix+key, dedupValueDone, ttl).Err()
}

// Release 释放未完成的占位（仅当值仍为 "pending" 时 DEL）。
//
// 只清理 pending 状态：若并发处理中已有实例 MarkDone，Release 不会删除 done 标记，
// 避免把「已成功处理」重新变回可处理；供 handler 失败后允许重试再次处理。
func (s *DedupStore) Release(ctx context.Context, key string) error {
	return releasePendingScript.Run(ctx, s.client, []string{s.prefix + key}, dedupValueInProgress).Err()
}
