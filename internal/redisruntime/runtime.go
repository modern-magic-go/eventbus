package redisruntime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	redis "github.com/redis/go-redis/v9"

	"github.com/modern-magic-go/eventbus/internal/core"
	"github.com/modern-magic-go/eventbus/internal/snowflake"
)

// maxClaimScanIterations 限制单次 ClaimPending 的 XAUTOCLAIM 调用次数，
// 避免异常 PEL 形态导致单轮扫描时间过长。每轮最多扫描
// maxClaimScanIterations * Batch * 10 个 PEL 条目。
const maxClaimScanIterations = 1000

// retryMemberPrefix 是「按 MessageID 稳定派生」的重试队列 member 前缀；
// v0.2 的旧数据没有该前缀（member 直接是任务 JSON），PollRetry 据此分流。
const retryMemberPrefix = "mid:"

// retryKeyPrefix 是显式指定 RetryTask.Key（例如 DLQ 重放）时的 member 前缀，
// 与 mid: 一样存放于 payload HASH。
const retryKeyPrefix = "key:"

var (
	_ core.Runtime                  = (*RedisRuntime)(nil)
	_ core.PendingReclaimer         = (*RedisRuntime)(nil)
	_ core.ConsumerGroupInitializer = (*RedisRuntime)(nil)
	_ core.DeadLetterStore          = (*RedisRuntime)(nil)
	_ core.RuntimeStats             = (*RedisRuntime)(nil)
)

type Config struct {
	Namespace       string
	StreamMaxLen    int64
	RetryBatchSize  int
	ConsumerName    string
	RetryQueueKey   string
	DeadLetterKey   string
	TopicPrefix     string
	RetryInterval   time.Duration
	DefaultBlock    time.Duration
	DefaultReadSize int64
	// RetryDataKey 是重试任务 payload 的 HASH key，默认 "<Namespace>:retry:data"。
	// ZSET（RetryQueueKey）只存 member/score 索引，payload 存在这里，
	// 使按 MessageID 的稳定 member 可以原地覆盖（幂等替换）。
	RetryDataKey string
}

type RedisRuntime struct {
	client *redis.Client
	cfg    Config
}

func NewRedisRuntime(client *redis.Client, cfg Config) *RedisRuntime {
	if cfg.Namespace == "" {
		cfg.Namespace = "eventbus"
	}
	if cfg.RetryBatchSize <= 0 {
		cfg.RetryBatchSize = core.DefaultPollBatchSize
	}
	if cfg.DefaultBlock <= 0 {
		cfg.DefaultBlock = core.DefaultPollBlock
	}
	if cfg.DefaultReadSize <= 0 {
		cfg.DefaultReadSize = int64(core.DefaultPollBatchSize)
	}
	if cfg.TopicPrefix == "" {
		cfg.TopicPrefix = "topic"
	}
	if cfg.RetryQueueKey == "" {
		cfg.RetryQueueKey = cfg.Namespace + ":retry"
	}
	if cfg.DeadLetterKey == "" {
		cfg.DeadLetterKey = cfg.Namespace + ":dlq"
	}
	if cfg.RetryDataKey == "" {
		cfg.RetryDataKey = cfg.Namespace + ":retry:data"
	}
	// consumer name 必须稳定：每次 Consume 换随机名会产生大量“僵尸消费者”，
	// 并让 PEL 里的 pending 消息归属难以追踪。这里在构造时生成一次。
	if cfg.ConsumerName == "" {
		cfg.ConsumerName = defaultConsumerName()
	}

	return &RedisRuntime{client: client, cfg: cfg}
}

func defaultConsumerName() string {
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		return fmt.Sprintf("eventbus-%d", os.Getpid())
	}
	return fmt.Sprintf("eventbus-%d-%s", os.Getpid(), hex.EncodeToString(suffix))
}

func (r *RedisRuntime) consumerName(cfg core.ConsumerConfig) string {
	if cfg.ConsumerName != "" {
		return cfg.ConsumerName
	}
	return r.cfg.ConsumerName
}

func (r *RedisRuntime) Publish(ctx context.Context, msg core.Message) error {
	values := map[string]any{
		"topic":        msg.Topic,
		"payload":      msg.Payload,
		"published_at": msg.PublishedAt.Unix(),
	}
	if msg.ID != "" {
		values["message_id"] = msg.ID
	}
	if len(msg.Headers) > 0 {
		h, err := json.Marshal(msg.Headers)
		if err != nil {
			return err
		}
		values["headers"] = h
	}

	args := &redis.XAddArgs{
		Stream: r.topicKey(msg.Topic),
		Values: values,
	}
	if r.cfg.StreamMaxLen > 0 {
		args.MaxLen = r.cfg.StreamMaxLen
	}
	return r.client.XAdd(ctx, args).Err()
}

func (r *RedisRuntime) EnsureConsumerGroup(ctx context.Context, topic, group string) error {
	return r.EnsureConsumerGroupFrom(ctx, topic, group, core.StartFromBeginning)
}

// EnsureConsumerGroupFrom 按指定 start offset 创建消费组，实现
// core.ConsumerGroupInitializer；组已存在时忽略 BUSYGROUP（与旧行为一致）。
//
// start 直接透传给 Redis XGROUP CREATE，支持 "0"（从头）、"$"（最新）、
// unix 毫秒时间戳或 stream ID（如 "1700000000000-0"）。空串按 "0" 处理。
// 注意：Redis 只会对新建组应用 start；已存在的组不会被重置。
func (r *RedisRuntime) EnsureConsumerGroupFrom(ctx context.Context, topic, group, start string) error {
	if strings.TrimSpace(start) == "" {
		start = core.StartFromBeginning
	}
	err := r.client.XGroupCreateMkStream(ctx, r.topicKey(topic), group, start).Err()
	if err != nil && !strings.Contains(strings.ToUpper(err.Error()), "BUSYGROUP") {
		return err
	}
	return nil
}

func (r *RedisRuntime) Consume(ctx context.Context, cfg core.ConsumerConfig) ([]core.Message, error) {
	consumer := r.consumerName(cfg)

	batchSize := int64(cfg.PollBatchSize)
	if batchSize <= 0 {
		batchSize = r.cfg.DefaultReadSize
	}
	block := cfg.PollBlock
	if block <= 0 {
		block = r.cfg.DefaultBlock
	}

	res, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    cfg.Group,
		Consumer: consumer,
		Streams:  []string{r.topicKey(cfg.Topic), ">"},
		Count:    batchSize,
		Block:    block,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	messages := make([]core.Message, 0, len(res))
	for _, stream := range res {
		for _, item := range stream.Messages {
			msg, convErr := r.fromRedisMessage(cfg, item)
			if convErr != nil {
				continue
			}
			messages = append(messages, msg)
		}
	}
	return messages, nil
}

// ClaimPending 认领消费组 PEL 中空闲超过 MinIdle 的消息（XAUTOCLAIM，Redis 6.2+）。
//
// 从 "0-0" 开始按 XAUTOCLAIM 返回的 next cursor 连续扫描，直到：
//   - 本批已认领满 Batch 条；或
//   - 扫描到 PEL 末尾（next cursor 回到 "0-0"）。
//
// 必须使用 cursor 推进而不是每次固定 "0-0"：Redis 单次 XAUTOCLAIM 最多扫描
// count*10 个 PEL 条目，若 PEL 头部存在大量 idle 未达 MinIdle 的条目，
// 固定 "0-0" 会反复扫描头部、永远看不到尾部可认领消息。
//
// 刚被认领的消息 idle 会重置，因此本轮内不会被重复认领；已 Ack 的消息会从 PEL 删除。
//
// 这里直接走 raw 命令而不是 go-redis 的 XAutoClaimCmd，因为后者会丢弃 Redis 7
// XAUTOCLAIM 回复的第三个元素（deleted IDs）：那些是被 StreamMaxLen/XTRIM 从
// stream 裁剪掉、PEL 中无法再认领的「孤儿」消息 ID。Redis 6.2 的回复只有两个
// 元素，此时无法感知孤儿，跳过清理。
func (r *RedisRuntime) ClaimPending(ctx context.Context, cfg core.ConsumerConfig) ([]core.Message, error) {
	minIdle := cfg.PendingReclaim.MinIdle
	if minIdle <= 0 {
		minIdle = core.DefaultReclaimMinIdle
	}
	batchSize := int64(cfg.PendingReclaim.Batch)
	if batchSize <= 0 {
		batchSize = core.DefaultPollBatchSize
	}

	streamKey := r.topicKey(cfg.Topic)
	consumer := r.consumerName(cfg)
	minIdleArg := strconv.FormatInt(minIdle.Milliseconds(), 10)
	countArg := strconv.FormatInt(batchSize, 10)

	messages := make([]core.Message, 0, batchSize)
	cursor := "0-0"
	claimed := int64(0)

	for iteration := 0; iteration < maxClaimScanIterations; iteration++ {
		raw, err := r.client.Do(ctx,
			"xautoclaim", streamKey, cfg.Group, consumer, minIdleArg, cursor,
			"count", countArg,
		).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				break
			}
			return messages, err
		}

		reply, err := parseXAutoClaimReply(raw)
		if err != nil {
			return messages, err
		}

		// Redis 7 起 deleted IDs 是「已从 stream 删除、PEL 中不再有对应消息」的 ID。
		// 对它们执行 XACK 清理 PEL 残留（Redis 7 的 XAUTOCLAIM 通常已移除，
		// XACK 对不存在的 pending 条目是无害 no-op）。
		for _, id := range reply.deleted {
			if ackErr := r.client.XAck(ctx, streamKey, cfg.Group, id).Err(); ackErr != nil {
				return messages, ackErr
			}
		}

		claimed += int64(len(reply.messages))
		for _, item := range reply.messages {
			msg, convErr := r.fromRedisMessage(cfg, item)
			if convErr != nil {
				continue
			}
			messages = append(messages, msg)
		}

		// PEL 扫描完 / 本批已认领满 / cursor 不再前进（防御异常实现），结束本轮。
		if reply.next == "0-0" || reply.next == cursor || claimed >= batchSize {
			break
		}
		cursor = reply.next
	}
	return messages, nil
}

// xautoclaimReply 是 XAUTOCLAIM 原始回复的解析结果。
type xautoclaimReply struct {
	// next 是下一次调用的 cursor，扫描完成时为 "0-0"。
	next string
	// messages 是本次认领到（所有权已转移给当前消费者）的消息。
	messages []redis.XMessage
	// deleted 是 Redis 7 返回的、已从 stream 删除且无法再认领的 PEL 孤儿 ID。
	// Redis 6.2 的回复没有该元素，此时为 nil。
	deleted []string
}

// parseXAutoClaimReply 解析 XAUTOCLAIM 的 raw 回复：
//
//	Redis 6.2: [next-cursor, messages]
//	Redis 7+:  [next-cursor, messages, deleted-ids]
//
// 每条 message 是 [id, [field, value, ...]]，与 redis.XMessage 的结构对齐。
func parseXAutoClaimReply(raw any) (xautoclaimReply, error) {
	var reply xautoclaimReply

	items, ok := raw.([]any)
	if !ok {
		return reply, fmt.Errorf("redisruntime: unexpected XAUTOCLAIM reply type %T", raw)
	}
	if len(items) != 2 && len(items) != 3 {
		return reply, fmt.Errorf("redisruntime: unexpected XAUTOCLAIM reply length %d, want 2 or 3", len(items))
	}

	reply.next = anyToString(items[0])

	rawMessages, ok := items[1].([]any)
	if !ok {
		return reply, fmt.Errorf("redisruntime: unexpected XAUTOCLAIM messages type %T", items[1])
	}
	reply.messages = make([]redis.XMessage, 0, len(rawMessages))
	for _, rawMessage := range rawMessages {
		pair, ok := rawMessage.([]any)
		if !ok || len(pair) != 2 {
			continue
		}
		reply.messages = append(reply.messages, redis.XMessage{
			ID:     anyToString(pair[0]),
			Values: streamMessageValues(pair[1]),
		})
	}

	if len(items) == 3 && items[2] != nil {
		if rawDeleted, ok := items[2].([]any); ok {
			reply.deleted = make([]string, 0, len(rawDeleted))
			for _, id := range rawDeleted {
				reply.deleted = append(reply.deleted, anyToString(id))
			}
		}
	}
	return reply, nil
}

// streamMessageValues 把 raw 回复里的 [field, value, ...] 转成 map，
// 供 fromRedisMessage 复用（raw 回复不经过 go-redis 的 XMessage 解析）。
func streamMessageValues(raw any) map[string]any {
	values, ok := raw.([]any)
	if !ok {
		return nil
	}
	res := make(map[string]any, len(values)/2)
	for i := 0; i+1 < len(values); i += 2 {
		res[anyToString(values[i])] = values[i+1]
	}
	return res
}

// anyToString 兼容 raw 回复里可能出现的 string / []byte 表示。
func anyToString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case nil:
		return ""
	default:
		return fmt.Sprint(t)
	}
}

func (r *RedisRuntime) Ack(ctx context.Context, msg core.Message) error {
	if msg.DeliveryTag == "" || msg.ConsumerGroup == "" {
		return nil
	}
	return r.client.XAck(ctx, r.topicKey(msg.Topic), msg.ConsumerGroup, msg.DeliveryTag).Err()
}

// ScheduleRetry 把重试任务写入重试队列。
//
// 存储模型：
//   - ZSET（RetryQueueKey）：member=任务标识，score=AvailableAt.Unix()，用于按时间轮询；
//   - HASH（RetryDataKey）：field=member，value=任务 JSON，存放 payload。
//
// 契约：同一 MessageID 重复调度必须覆盖而非追加。MessageID 非空时 member 由
// "mid:" + Group + ":" + MessageID 稳定派生，因此同一消息的重复调度（例如重试
// 再次失败后重新入队）会原地替换旧任务及其 score，不会在 ZSET 中堆积多条。
// MessageID 为空时退化为 v0.2 行为：整个任务 JSON 作为 member（无法幂等去重）。
//
// HSET 与 ZADD 在同一个 TxPipeline（MULTI/EXEC）中提交，保证原子性：
// 不会出现 ZSET 索引指向不存在 payload 的中间态。
//
// 兼容 v0.2 遗留数据：旧 member 是任务 JSON。调用方（Bus）对 MessageID 非空的
// 任务不再调用 RemoveRetry，因此这里额外把 task 自带的 RawMember（来自 PollRetry）
// 与稳定 member 不同时，在同一事务里删除旧 member，完成「旧格式 → 新格式」迁移；
// 否则 ZSET 会同时残留旧 JSON member 与新 "mid:" member，导致同一消息被重复重试。
func (r *RedisRuntime) ScheduleRetry(ctx context.Context, task core.RetryTask) error {
	b, err := json.Marshal(task)
	if err != nil {
		return err
	}
	member, err := retryTaskMember(task)
	if err != nil {
		return err
	}
	legacyMember := core.RawMember(task)
	migrate := legacyMember != "" && legacyMember != member
	_, err = r.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		if migrate {
			pipe.ZRem(ctx, r.cfg.RetryQueueKey, legacyMember)
			pipe.HDel(ctx, r.cfg.RetryDataKey, legacyMember)
		}
		pipe.HSet(ctx, r.cfg.RetryDataKey, member, string(b))
		pipe.ZAdd(ctx, r.cfg.RetryQueueKey, redis.Z{
			Score:  float64(task.AvailableAt.Unix()),
			Member: member,
		})
		return nil
	})
	return err
}

// PollRetry 取回到期（score <= now）的重试任务，最多 limit 条。
//
// 新格式 member（前缀 "mid:"）从 HASH 读取 payload；旧 v0.2 数据仍以任务 JSON
// 作为 member，直接反序列化，保持向前兼容。返回的任务都会带上 RawMember，
// 供 RemoveRetry 精确定位 ZSET/HASH 条目。
func (r *RedisRuntime) PollRetry(ctx context.Context, limit int, now time.Time) ([]core.RetryTask, error) {
	if limit <= 0 {
		limit = r.cfg.RetryBatchSize
	}
	items, err := r.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     r.cfg.RetryQueueKey,
		Start:   "-inf",
		Stop:    fmt.Sprintf("%d", now.Unix()),
		ByScore: true,
		Offset:  0,
		Count:   int64(limit),
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	tasks := make([]core.RetryTask, 0, len(items))
	for _, member := range items {
		var task core.RetryTask
		if strings.HasPrefix(member, retryMemberPrefix) || strings.HasPrefix(member, retryKeyPrefix) {
			raw, hgetErr := r.client.HGet(ctx, r.cfg.RetryDataKey, member).Result()
			if hgetErr != nil {
				if errors.Is(hgetErr, redis.Nil) {
					// payload 丢失（历史脏数据 / 人工误删）：清掉 ZSET 索引，
					// 避免该 member 永久占位、每轮轮询都失败。
					if zremErr := r.client.ZRem(ctx, r.cfg.RetryQueueKey, member).Err(); zremErr != nil {
						return nil, zremErr
					}
					continue
				}
				return nil, hgetErr
			}
			if unmarshalErr := json.Unmarshal([]byte(raw), &task); unmarshalErr != nil {
				// 坏 payload 不能只跳过：同一 member 会永久占据领先位置，
				// 当坏数据达到 limit 条后合法任务将永远无法被 poll 到。清理后继续。
				_ = r.client.ZRem(ctx, r.cfg.RetryQueueKey, member).Err()
				_ = r.client.HDel(ctx, r.cfg.RetryDataKey, member).Err()
				continue
			}
		} else {
			// v0.2 兼容：member 就是任务 JSON。
			if unmarshalErr := json.Unmarshal([]byte(member), &task); unmarshalErr != nil {
				// 无法解析的旧 member 会永久占据 ZSET 头部，必须清理。
				_ = r.client.ZRem(ctx, r.cfg.RetryQueueKey, member).Err()
				continue
			}
		}
		task = core.WithRawMember(task, member)
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// RemoveRetry 删除一条重试任务。
//
// 优先使用 PollRetry 带回的 RawMember（可精确命中新旧两种 member 格式）；
// 没有时按与 ScheduleRetry 相同的规则重新计算。ZSET 与 HASH 的删除在同一
// TxPipeline 中提交；对旧 JSON member 而言 HDEL 是无害 no-op。
func (r *RedisRuntime) RemoveRetry(ctx context.Context, task core.RetryTask) error {
	member := core.RawMember(task)
	if member == "" {
		var err error
		member, err = retryTaskMember(task)
		if err != nil {
			return err
		}
	}
	_, err := r.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.ZRem(ctx, r.cfg.RetryQueueKey, member)
		pipe.HDel(ctx, r.cfg.RetryDataKey, member)
		return nil
	})
	return err
}

// retryTaskMember 计算重试任务在重试队列中的 member。
//
// 契约：同一 MessageID + Group 必须映射到同一 member（幂等覆盖）；
// 无 MessageID 时退化为任务 JSON（v0.2 兼容行为，无法去重）。
func retryTaskMember(task core.RetryTask) (string, error) {
	if task.Key != "" {
		return retryKeyPrefix + task.Key, nil
	}
	if task.MessageID != "" {
		return retryMemberPrefix + task.Group + ":" + task.MessageID, nil
	}
	b, err := json.Marshal(task)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// MoveToDeadLetter 把消息写入死信队列。
//
// 记录 ID 由 Runtime 在写入时生成（调用方可以不填），用于查询与重放；
// 存储格式保持 v0.2 不变：LPUSH 单条 JSON 到 DeadLetterKey。
func (r *RedisRuntime) MoveToDeadLetter(ctx context.Context, msg core.DeadLetterMessage) error {
	if msg.ID == "" {
		msg.ID = snowflake.Generate()
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return r.client.LPush(ctx, r.cfg.DeadLetterKey, string(b)).Err()
}

func (r *RedisRuntime) Close() error {
	return r.client.Close()
}

func (r *RedisRuntime) topicKey(topic string) string {
	return fmt.Sprintf("%s:%s:%s", r.cfg.Namespace, r.cfg.TopicPrefix, topic)
}

func (r *RedisRuntime) fromRedisMessage(cfg core.ConsumerConfig, x redis.XMessage) (core.Message, error) {
	payload := extractBytes(x.Values["payload"])
	if payload == nil {
		return core.Message{}, errors.New("payload is required")
	}

	publishedAt := time.Now()
	if raw := extractString(x.Values["published_at"]); raw != "" {
		if ts, err := strconv.ParseInt(raw, 10, 64); err == nil {
			publishedAt = time.Unix(ts, 0)
		}
	}

	msg := core.Message{
		ID:            extractString(x.Values["message_id"]),
		Topic:         cfg.Topic,
		Payload:       payload,
		Headers:       extractHeaders(x.Values["headers"]),
		PublishedAt:   publishedAt,
		ConsumerGroup: cfg.Group,
		Attempt:       0,
		MaxRetries:    cfg.MaxRetries,
		DeliveryTag:   x.ID,
	}
	return msg, nil
}

func extractString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return ""
	}
}

func extractBytes(v any) []byte {
	switch t := v.(type) {
	case []byte:
		return append([]byte(nil), t...)
	case string:
		return []byte(t)
	default:
		return nil
	}
}

func extractHeaders(v any) map[string]string {
	raw := extractBytes(v)
	if len(raw) == 0 {
		return nil
	}
	res := map[string]string{}
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil
	}
	return res
}
