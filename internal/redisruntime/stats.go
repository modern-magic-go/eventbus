package redisruntime

import (
	"context"
	"strings"
)

// StreamLag 返回消费组尚未投递的消息数（XINFO GROUPS 的 lag 字段）。
//
// 语义选择：stream 或 group 不存在时返回 0（视为无积压），不返回错误。
// 这样可观测性组件（如 Prometheus collector）在订阅尚未启动 / 刚被清理时
// 不会反复上报虚假错误；其他 Redis 错误原样返回。
// lag 无法确定时 Redis 返回 nil，go-redis 解析为 -1，这里原样透传。
func (r *RedisRuntime) StreamLag(ctx context.Context, topic, group string) (int64, error) {
	groups, err := r.client.XInfoGroups(ctx, r.topicKey(topic)).Result()
	if err != nil {
		if isMissingStreamOrGroup(err) {
			return 0, nil
		}
		return 0, err
	}
	for _, g := range groups {
		if g.Name == group {
			return g.Lag, nil
		}
	}
	return 0, nil
}

// PendingCount 返回消费组 PEL（pending entries list）中的消息数。
//
// 与 StreamLag 一致：stream/group 不存在时返回 0，其他错误原样返回。
func (r *RedisRuntime) PendingCount(ctx context.Context, topic, group string) (int64, error) {
	pending, err := r.client.XPending(ctx, r.topicKey(topic), group).Result()
	if err != nil {
		if isMissingStreamOrGroup(err) {
			return 0, nil
		}
		return 0, err
	}
	return pending.Count, nil
}

// RetryQueueLen 返回重试队列（ZSET）中的任务数。
func (r *RedisRuntime) RetryQueueLen(ctx context.Context) (int64, error) {
	return r.client.ZCard(ctx, r.cfg.RetryQueueKey).Result()
}

// isMissingStreamOrGroup 判断 Redis 错误是否属于「stream / group 不存在」，
// 这类错误在指标采集中按 0 处理，不算故障。
func isMissingStreamOrGroup(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such key") ||
		strings.Contains(msg, "nogroup") ||
		strings.Contains(msg, "no such consumer group")
}
