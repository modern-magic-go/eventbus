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
)

// maxClaimScanIterations 限制单次 ClaimPending 的 XAUTOCLAIM 调用次数，
// 避免异常 PEL 形态导致单轮扫描时间过长。每轮最多扫描
// maxClaimScanIterations * Batch * 10 个 PEL 条目。
const maxClaimScanIterations = 1000

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
	err := r.client.XGroupCreateMkStream(ctx, r.topicKey(topic), group, "0").Err()
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
func (r *RedisRuntime) ClaimPending(ctx context.Context, cfg core.ConsumerConfig) ([]core.Message, error) {
	minIdle := cfg.PendingReclaim.MinIdle
	if minIdle <= 0 {
		minIdle = core.DefaultReclaimMinIdle
	}
	batchSize := int64(cfg.PendingReclaim.Batch)
	if batchSize <= 0 {
		batchSize = core.DefaultPollBatchSize
	}

	messages := make([]core.Message, 0, batchSize)
	cursor := "0-0"
	claimed := int64(0)

	for iteration := 0; iteration < maxClaimScanIterations; iteration++ {
		res, next, err := r.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   r.topicKey(cfg.Topic),
			Group:    cfg.Group,
			Consumer: r.consumerName(cfg),
			MinIdle:  minIdle,
			Start:    cursor,
			Count:    batchSize,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				break
			}
			return messages, err
		}

		claimed += int64(len(res))
		for _, item := range res {
			msg, convErr := r.fromRedisMessage(cfg, item)
			if convErr != nil {
				continue
			}
			messages = append(messages, msg)
		}

		// PEL 扫描完 / 本批已认领满 / cursor 不再前进（防御异常实现），结束本轮。
		if next == "0-0" || next == cursor || claimed >= batchSize {
			break
		}
		cursor = next
	}
	return messages, nil
}

func (r *RedisRuntime) Ack(ctx context.Context, msg core.Message) error {
	if msg.DeliveryTag == "" || msg.ConsumerGroup == "" {
		return nil
	}
	return r.client.XAck(ctx, r.topicKey(msg.Topic), msg.ConsumerGroup, msg.DeliveryTag).Err()
}

func (r *RedisRuntime) ScheduleRetry(ctx context.Context, task core.RetryTask) error {
	b, err := json.Marshal(task)
	if err != nil {
		return err
	}
	member := string(b)
	return r.client.ZAdd(ctx, r.cfg.RetryQueueKey, redis.Z{Score: float64(task.AvailableAt.Unix()), Member: member}).Err()
}

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
	for _, item := range items {
		var task core.RetryTask
		if unmarshalErr := json.Unmarshal([]byte(item), &task); unmarshalErr != nil {
			continue
		}
		task = core.WithRawMember(task, item)
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func (r *RedisRuntime) RemoveRetry(ctx context.Context, task core.RetryTask) error {
	member := core.RawMember(task)
	if member == "" {
		b, err := json.Marshal(task)
		if err != nil {
			return err
		}
		member = string(b)
	}
	return r.client.ZRem(ctx, r.cfg.RetryQueueKey, member).Err()
}

func (r *RedisRuntime) MoveToDeadLetter(ctx context.Context, msg core.DeadLetterMessage) error {
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
