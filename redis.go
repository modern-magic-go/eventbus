package eventbus

import (
	redis "github.com/redis/go-redis/v9"

	"github.com/modern-magic-go/eventbus/internal/redisruntime"
)

type RedisRuntime = redisruntime.RedisRuntime
type RedisRuntimeConfig = redisruntime.Config

func NewRedisRuntime(client *redis.Client, cfg RedisRuntimeConfig) *RedisRuntime {
	return redisruntime.NewRedisRuntime(client, cfg)
}

// RedisDedupStore 是基于 Redis 的消费端幂等去重存储（实现 DedupStore）。
type RedisDedupStore = redisruntime.DedupStore
type RedisDedupStoreConfig = redisruntime.DedupStoreConfig

// NewRedisDedupStore 创建 Redis 去重存储，client 生命周期由调用方管理。
func NewRedisDedupStore(client *redis.Client, cfg RedisDedupStoreConfig) *RedisDedupStore {
	return redisruntime.NewDedupStore(client, cfg)
}
