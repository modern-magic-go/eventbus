package core

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/modern-magic-go/eventbus/internal/snowflake"
)

const consumeErrorRetryDelay = 200 * time.Millisecond

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

	if cfg.PendingReclaim.enabled() {
		if _, ok := b.runtime.(PendingReclaimer); !ok {
			return fmt.Errorf("%w: %s", ErrPendingReclaimUnsupported, key)
		}
	}

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
			cancel()
			b.mu.Lock()
			b.cancel = nil
			b.mu.Unlock()
			return err
		}
	}

	for _, cfg := range snapshot {
		b.wg.Add(1)
		go b.runConsumer(workerCtx, cfg)
		if cfg.PendingReclaim.enabled() {
			b.wg.Add(1)
			go b.runPendingReclaimer(workerCtx, cfg)
		}
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
		if ctx.Err() != nil {
			return
		}

		messages, err := b.runtime.Consume(ctx, cfg)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(consumeErrorRetryDelay):
			}
			continue
		}

		for _, msg := range messages {
			if ctx.Err() != nil {
				return
			}
			b.handleMessage(ctx, cfg, msg)
		}
	}
}

// runPendingReclaimer 周期性认领 PEL 中空闲超时的消息，走与正常消费相同的处理路径。
// 它修复的是：消费者在 XREADGROUP 之后、XACK 之前崩溃时，消息永久停留在 PEL 的问题。
func (b *Bus) runPendingReclaimer(ctx context.Context, cfg ConsumerConfig) {
	defer b.wg.Done()

	claimer, ok := b.runtime.(PendingReclaimer)
	if !ok {
		return
	}
	interval := cfg.PendingReclaim.Interval
	if interval <= 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		b.reclaimPending(ctx, claimer, cfg)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (b *Bus) reclaimPending(ctx context.Context, claimer PendingReclaimer, cfg ConsumerConfig) {
	messages, err := claimer.ClaimPending(ctx, cfg)
	// 即使认领中途出错，也把本批已认领到的消息处理完，避免它们再多等一个 MinIdle 周期。
	for _, msg := range messages {
		if ctx.Err() != nil {
			return
		}
		b.handleMessage(ctx, cfg, msg)
	}
	// 认领失败只影响本轮回补，下个周期继续。
	_ = err
}

// handleMessage 统一处理正常消费与 pending 回收的消息：成功则 Ack，失败则进入重试 / 死信。
func (b *Bus) handleMessage(ctx context.Context, cfg ConsumerConfig, msg Message) {
	if err := b.invokeHandler(ctx, cfg.Handler, msg); err != nil {
		b.observer.OnHandleFailed(ctx, msg, err)
		_ = b.handleFailure(ctx, cfg, msg, err)
		return
	}
	_ = b.runtime.Ack(ctx, msg)
	b.observer.OnHandled(ctx, msg)
}

// invokeHandler 调用 handler，并把 panic 兜底转换为 error，避免消费 goroutine 直接崩溃。
// 返回的错误会被 OnHandleFailed / 重试 / 死信流程按普通失败处理。
func (b *Bus) invokeHandler(ctx context.Context, handler Handler, msg Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v\n%s", ErrHandlerPanic, r, capStack(debug.Stack()))
		}
	}()
	return handler(ctx, msg)
}

// maxPanicStackSize 限制 panic 堆栈长度：panic 错误会写入 RetryTask.LastError（ZSET member）
// 和 DLQ 条目，完整堆栈（数 KB）会显著放大 Redis 内存占用。
const maxPanicStackSize = 2 * 1024

func capStack(stack []byte) []byte {
	if len(stack) <= maxPanicStackSize {
		return stack
	}
	return append(stack[:maxPanicStackSize], []byte("\n... (stack truncated)")...)
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
			ID:            task.MessageID,
			Topic:         task.Topic,
			Payload:       task.Payload,
			Headers:       cloneHeaders(task.Headers),
			PublishedAt:   time.Now(),
			ConsumerGroup: task.Group,
			Attempt:       task.Attempt,
			MaxRetries:    task.MaxRetries,
		}

		handleErr := b.invokeHandler(ctx, cfg.Handler, msg)
		if handleErr == nil {
			_ = b.runtime.RemoveRetry(ctx, task)
			b.observer.OnHandled(ctx, msg)
			continue
		}

		if task.Attempt >= task.MaxRetries {
			dlq := DeadLetterMessage{
				MessageID:  task.MessageID,
				Topic:      task.Topic,
				Group:      task.Group,
				Payload:    task.Payload,
				Headers:    cloneHeaders(task.Headers),
				Attempt:    task.Attempt,
				MaxRetries: task.MaxRetries,
				Error:      handleErr.Error(),
				OccurredAt: time.Now(),
			}
			if err := b.runtime.MoveToDeadLetter(ctx, dlq); err != nil {
				// 死信写入失败时保留旧重试任务，下一轮继续尝试，避免消息丢失。
				continue
			}
			b.observer.OnDeadLettered(ctx, dlq)
			_ = b.runtime.RemoveRetry(ctx, task)
			continue
		}

		next := task
		next.Attempt = task.Attempt + 1
		next.LastError = handleErr.Error()
		next.AvailableAt = time.Now().Add(backoffDelay(cfg.RetryBaseDelay, next.Attempt))

		if err := b.runtime.ScheduleRetry(ctx, next); err != nil {
			// 新任务写入失败时保留旧任务，下一轮继续尝试，避免重试任务丢失。
			continue
		}
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
			MessageID:  msg.ID,
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
		// 死信已接管该消息，XACK 原 stream 消息，避免 PEL 泄漏。
		return b.runtime.Ack(ctx, msg)
	}

	task := RetryTask{
		MessageID:   msg.ID,
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
	// 重试任务已接管该消息（所有权转移），XACK 原 stream 消息，避免 PEL 泄漏。
	return b.runtime.Ack(ctx, msg)
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
