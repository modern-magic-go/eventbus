package redisruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	redis "github.com/redis/go-redis/v9"

	"github.com/CATT-L/go-eventbus/internal/core"
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

	return &RedisRuntime{client: client, cfg: cfg}
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
	consumer := cfg.ConsumerName
	if consumer == "" {
		consumer = r.cfg.ConsumerName
	}
	if consumer == "" {
		consumer = fmt.Sprintf("%s-%d", cfg.Group, time.Now().UnixNano())
	}

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

	msg := core.Message{
		ID:            extractString(x.Values["message_id"]),
		Topic:         cfg.Topic,
		Payload:       payload,
		Headers:       extractHeaders(x.Values["headers"]),
		PublishedAt:   time.Now(),
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
