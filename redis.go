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
