package core

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/modern-magic-go/eventbus/internal/snowflake"
)

type Config struct {
	RetryPollInterval time.Duration
	CloseRuntime      bool
}

type Bus struct {
	runtime  Runtime
	codec    Codec
	observer Observer
	config   Config

	mu       sync.RWMutex
	registry map[string]ConsumerConfig

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func NewBus(rt Runtime, opts ...Option) *Bus {
	b := &Bus{
		runtime:  rt,
		codec:    jsonCodec{},
		observer: noopObserver{},
		config:   Config{RetryPollInterval: time.Second},
		registry: make(map[string]ConsumerConfig),
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.config.RetryPollInterval <= 0 {
		b.config.RetryPollInterval = time.Second
	}
	if b.codec == nil {
		b.codec = jsonCodec{}
	}
	if b.observer == nil {
		b.observer = noopObserver{}
	}
	return b
}

func (b *Bus) Subscribe(cfg ConsumerConfig) error {
	if strings.TrimSpace(cfg.Topic) == "" || strings.TrimSpace(cfg.Group) == "" || cfg.Handler == nil {
		return ErrInvalidConsumerConfig
	}
	cfg = normalizeConsumerConfig(cfg)
	key := subscriptionKey(cfg.Topic, cfg.Group)

	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.registry[key]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateSubscription, key)
	}
	b.registry[key] = cfg
	return nil
}

func (b *Bus) Publish(ctx context.Context, topic string, payload any, opts ...PublishOption) error {
	if strings.TrimSpace(topic) == "" {
		return errors.New("topic is required")
	}

	po := PublishOptions{}
	for _, opt := range opts {
		opt(&po)
	}

	msgID := po.MessageID
	if msgID == "" {
		msgID = generateMessageID()
	}

	var raw []byte
	switch v := payload.(type) {
	case []byte:
		raw = append([]byte(nil), v...)
	default:
		encoded, err := b.codec.Marshal(payload)
		if err != nil {
			return err
		}
		raw = encoded
	}

	msg := Message{
		ID:          msgID,
		Topic:       topic,
		Payload:     raw,
		Headers:     cloneHeaders(po.Headers),
		PublishedAt: time.Now(),
	}
	if err := b.runtime.Publish(ctx, msg); err != nil {
		b.observer.OnPublishFailed(ctx, msg, err)
		return err
	}
	b.observer.OnPublished(ctx, msg)
	return nil
}

func (b *Bus) Start(ctx context.Context) error {
	b.mu.Lock()
	if b.cancel != nil {
		b.mu.Unlock()
		return nil
	}
	workerCtx, cancel := context.WithCancel(ctx)
	b.cancel = cancel
	snapshot := make([]ConsumerConfig, 0, len(b.registry))
	for _, cfg := range b.registry {
		snapshot = append(snapshot, cfg)
	}
	b.mu.Unlock()

	for _, cfg := range snapshot {
		if err := b.runtime.EnsureConsumerGroup(workerCtx, cfg.Topic, cfg.Group); err != nil {
			return err
		}
		b.wg.Add(1)
		go b.runConsumer(workerCtx, cfg)
	}

	b.wg.Add(1)
	go b.runRetryWorker(workerCtx)
	return nil
}

func (b *Bus) Stop() error {
	return b.StopWithTimeout(30 * time.Second)
}

func (b *Bus) StopWithTimeout(timeout time.Duration) error {
	b.mu.Lock()
	if b.cancel != nil {
		b.cancel()
		b.cancel = nil
	}
	b.mu.Unlock()

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
	}

	if b.config.CloseRuntime {
		return b.runtime.Close()
	}
	return nil
}

func (b *Bus) runConsumer(ctx context.Context, cfg ConsumerConfig) {
	defer b.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		messages, err := b.runtime.Consume(ctx, cfg)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		for _, msg := range messages {
			handleErr := cfg.Handler(ctx, msg)
			if handleErr != nil {
				b.observer.OnHandleFailed(ctx, msg, handleErr)
				_ = b.handleFailure(ctx, cfg, msg, handleErr)
			} else {
				_ = b.runtime.Ack(ctx, msg)
				b.observer.OnHandled(ctx, msg)
			}
		}
	}
}

func (b *Bus) runRetryWorker(ctx context.Context) {
	defer b.wg.Done()
	ticker := time.NewTicker(b.config.RetryPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = b.processRetry(ctx)
		}
	}
}

func (b *Bus) processRetry(ctx context.Context) error {
	tasks, err := b.runtime.PollRetry(ctx, DefaultPollBatchSize, time.Now())
	if err != nil {
		return err
	}

	for _, task := range tasks {
		cfg, ok := b.findConsumer(task.Topic, task.Group)
		if !ok {
			_ = b.runtime.RemoveRetry(ctx, task)
			continue
		}

		msg := Message{
			Topic:         task.Topic,
			Payload:       task.Payload,
			Headers:       cloneHeaders(task.Headers),
			PublishedAt:   time.Now(),
			ConsumerGroup: task.Group,
			Attempt:       task.Attempt,
			MaxRetries:    task.MaxRetries,
		}

		handleErr := cfg.Handler(ctx, msg)
		if handleErr == nil {
			_ = b.runtime.RemoveRetry(ctx, task)
			b.observer.OnHandled(ctx, msg)
			continue
		}

		if task.Attempt >= task.MaxRetries {
			dlq := DeadLetterMessage{
				Topic:      task.Topic,
				Group:      task.Group,
				Payload:    task.Payload,
				Headers:    cloneHeaders(task.Headers),
				Attempt:    task.Attempt,
				MaxRetries: task.MaxRetries,
				Error:      handleErr.Error(),
				OccurredAt: time.Now(),
			}
			_ = b.runtime.MoveToDeadLetter(ctx, dlq)
			b.observer.OnDeadLettered(ctx, dlq)
			_ = b.runtime.RemoveRetry(ctx, task)
			continue
		}

		next := task
		next.Attempt = task.Attempt + 1
		next.LastError = handleErr.Error()
		next.AvailableAt = time.Now().Add(backoffDelay(cfg.RetryBaseDelay, next.Attempt))

		_ = b.runtime.ScheduleRetry(ctx, next)
		b.observer.OnRetryScheduled(ctx, next, handleErr)
		_ = b.runtime.RemoveRetry(ctx, task)
	}
	return nil
}

func (b *Bus) handleFailure(ctx context.Context, cfg ConsumerConfig, msg Message, cause error) error {
	attempt := msg.Attempt + 1
	if msg.Attempt <= 0 {
		attempt = 1
	}

	if attempt > cfg.MaxRetries {
		dlq := DeadLetterMessage{
			Topic:      cfg.Topic,
			Group:      cfg.Group,
			Payload:    msg.Payload,
			Headers:    cloneHeaders(msg.Headers),
			Attempt:    attempt,
			MaxRetries: cfg.MaxRetries,
			Error:      cause.Error(),
			OccurredAt: time.Now(),
		}
		if err := b.runtime.MoveToDeadLetter(ctx, dlq); err != nil {
			return err
		}
		b.observer.OnDeadLettered(ctx, dlq)
		return nil
	}

	task := RetryTask{
		Topic:       cfg.Topic,
		Group:       cfg.Group,
		Payload:     msg.Payload,
		Headers:     cloneHeaders(msg.Headers),
		Attempt:     attempt,
		MaxRetries:  cfg.MaxRetries,
		AvailableAt: time.Now().Add(backoffDelay(cfg.RetryBaseDelay, attempt)),
		LastError:   cause.Error(),
	}
	if err := b.runtime.ScheduleRetry(ctx, task); err != nil {
		return err
	}
	b.observer.OnRetryScheduled(ctx, task, cause)
	return nil
}

func (b *Bus) findConsumer(topic, group string) (ConsumerConfig, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	cfg, ok := b.registry[subscriptionKey(topic, group)]
	return cfg, ok
}

func subscriptionKey(topic, group string) string {
	return topic + "|" + group
}

func cloneHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(headers))
	maps.Copy(cloned, headers)
	return cloned
}

func backoffDelay(base time.Duration, attempt int) time.Duration {
	if attempt <= 1 {
		return base
	}
	delay := base
	for i := 1; i < attempt; i++ {
		delay *= 2
	}
	return delay
}

func generateMessageID() string {
	return snowflake.Generate()
}
