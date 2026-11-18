package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/modern-magic-go/eventbus/internal/snowflake"
)

const (
	consumeErrorRetryDelay = 200 * time.Millisecond
	maxPanicStackSize      = 2 * 1024
	// maxDeadLetterPageSize 限制单次死信查询 / 重放的批次上限。
	maxDeadLetterPageSize = 1000
	// dedupWriteTimeout 是停机场景下去重状态写入（Release/MarkDone）的超时上限。
	// 这些写入使用 context.WithoutCancel 保证停机时也能执行，但必须避免无限阻塞。
	dedupWriteTimeout = 5 * time.Second
)

type Config struct {
	RetryPollInterval time.Duration
	CloseRuntime      bool
}

type Bus struct {
	runtime  Runtime
	codec    Codec
	observer Observer
	logger   *slog.Logger
	config   Config

	dedupStore  DedupStore
	dedupConfig DedupConfig

	mu       sync.RWMutex
	registry map[string]ConsumerConfig

	wg            sync.WaitGroup
	pollCancel    context.CancelFunc
	handlerCancel context.CancelFunc
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
	if b.dedupStore != nil {
		if b.dedupConfig.TTL <= 0 {
			b.dedupConfig.TTL = DefaultDedupTTL
		}
		if b.dedupConfig.KeyFunc == nil {
			b.dedupConfig.KeyFunc = defaultDedupKey
		}
	}
	return b
}

func defaultDedupKey(msg Message) string {
	return msg.Topic + "|" + msg.ConsumerGroup + "|" + msg.ID
}

func (b *Bus) Subscribe(cfg ConsumerConfig) error {
	if strings.TrimSpace(cfg.Topic) == "" || strings.TrimSpace(cfg.Group) == "" || cfg.Handler == nil {
		return ErrInvalidConsumerConfig
	}
	cfg = normalizeConsumerConfig(cfg)
	key := subscriptionKey(cfg.Topic, cfg.Group)

	if cfg.StartFrom != "" {
		start, err := normalizeStartFrom(cfg.StartFrom)
		if err != nil {
			return fmt.Errorf("%w: %q", ErrInvalidStartFrom, cfg.StartFrom)
		}
		cfg.StartFrom = start
		if start != StartFromBeginning {
			if _, ok := b.runtime.(ConsumerGroupInitializer); !ok {
				return fmt.Errorf("%w: %s", ErrStartFromUnsupported, key)
			}
		}
	}

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
		b.reportError(ctx, "publish", topic, "", msgID, err)
		return err
	}
	b.observer.OnPublished(ctx, msg)
	return nil
}

// Start 启动消费。parent ctx 取消会立即取消所有 worker 与在途 handler；
// 主动调用 Stop/StopWithTimeout 则进入 drain：先停止拉取新消息，再等待在途
// handler 完成（受超时限制）。
func (b *Bus) Start(ctx context.Context) error {
	b.mu.Lock()
	if b.pollCancel != nil {
		b.mu.Unlock()
		return nil
	}
	pollCtx, pollCancel := context.WithCancel(ctx)
	handlerCtx, handlerCancel := context.WithCancel(ctx)
	b.pollCancel = pollCancel
	b.handlerCancel = handlerCancel
	snapshot := make([]ConsumerConfig, 0, len(b.registry))
	for _, cfg := range b.registry {
		snapshot = append(snapshot, cfg)
	}
	b.mu.Unlock()

	for _, cfg := range snapshot {
		var err error
		if cfg.StartFrom != "" && cfg.StartFrom != StartFromBeginning {
			initializer, ok := b.runtime.(ConsumerGroupInitializer)
			if !ok {
				err = fmt.Errorf("%w: %s", ErrStartFromUnsupported, subscriptionKey(cfg.Topic, cfg.Group))
			} else {
				err = initializer.EnsureConsumerGroupFrom(pollCtx, cfg.Topic, cfg.Group, cfg.StartFrom)
			}
		} else {
			err = b.runtime.EnsureConsumerGroup(pollCtx, cfg.Topic, cfg.Group)
		}
		if err != nil {
			pollCancel()
			handlerCancel()
			b.mu.Lock()
			b.pollCancel = nil
			b.handlerCancel = nil
			b.mu.Unlock()
			return err
		}
	}

	for _, cfg := range snapshot {
		b.wg.Add(1)
		go b.runConsumer(pollCtx, handlerCtx, cfg)
		if cfg.PendingReclaim.enabled() {
			b.wg.Add(1)
			go b.runPendingReclaimer(pollCtx, handlerCtx, cfg)
		}
	}

	b.wg.Add(1)
	go b.runRetryWorker(pollCtx, handlerCtx)
	return nil
}

func (b *Bus) Stop() error {
	return b.StopWithTimeout(30 * time.Second)
}

// StopWithTimeout 停止拉取新消息并等待在途 handler 完成；等待超时返回
// ErrStopTimeout。超时后不会再关闭注入的 Runtime（避免 handler 使用已关闭连接），
// 需要调用方自行处理。
func (b *Bus) StopWithTimeout(timeout time.Duration) error {
	b.mu.Lock()
	pollCancel := b.pollCancel
	handlerCancel := b.handlerCancel
	b.pollCancel = nil
	b.handlerCancel = nil
	b.mu.Unlock()

	if pollCancel == nil {
		return nil
	}

	// 先停止拉取新消息，再 drain 在途 handler。
	pollCancel()

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		handlerCancel()
		if b.config.CloseRuntime {
			return b.runtime.Close()
		}
		return nil
	case <-time.After(timeout):
		handlerCancel()
		return ErrStopTimeout
	}
}

func (b *Bus) runConsumer(pollCtx, handlerCtx context.Context, cfg ConsumerConfig) {
	defer b.wg.Done()
	for {
		if pollCtx.Err() != nil {
			return
		}

		messages, err := b.runtime.Consume(pollCtx, cfg)
		if err != nil {
			if pollCtx.Err() != nil {
				return
			}
			b.reportError(pollCtx, "consume", cfg.Topic, cfg.Group, "", err)
			select {
			case <-pollCtx.Done():
				return
			case <-time.After(consumeErrorRetryDelay):
			}
			continue
		}

		// drain：即使已经停止拉取，也要把本批已取到的消息处理完再退出。
		for _, msg := range messages {
			if handlerCtx.Err() != nil {
				return
			}
			b.handleMessage(handlerCtx, cfg, msg)
		}
	}
}

// runPendingReclaimer 周期性认领 PEL 中空闲超时的消息，走与正常消费相同的处理路径。
func (b *Bus) runPendingReclaimer(pollCtx, handlerCtx context.Context, cfg ConsumerConfig) {
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
		b.reclaimPending(pollCtx, handlerCtx, claimer, cfg)
		select {
		case <-pollCtx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (b *Bus) reclaimPending(pollCtx, handlerCtx context.Context, claimer PendingReclaimer, cfg ConsumerConfig) {
	messages, err := claimer.ClaimPending(pollCtx, cfg)
	if err != nil && pollCtx.Err() == nil {
		b.reportError(pollCtx, "reclaim", cfg.Topic, cfg.Group, "", err)
	}
	// 即使认领中途出错，也把本批已认领到的消息处理完，避免它们再多等一个 MinIdle 周期。
	for _, msg := range messages {
		if handlerCtx.Err() != nil {
			return
		}
		b.handleMessage(handlerCtx, cfg, msg)
	}
}

// handleMessage 统一处理正常消费与 pending 回收的消息：成功则 Ack，失败则进入重试 / 死信。
func (b *Bus) handleMessage(ctx context.Context, cfg ConsumerConfig, msg Message) {
	dedup := b.acquireDedup(ctx, cfg, msg)
	if dedup.active && dedup.status == DedupDone {
		b.notifyDuplicate(ctx, msg)
		if err := b.runtime.Ack(ctx, msg); err != nil {
			b.reportError(ctx, "ack", cfg.Topic, cfg.Group, msg.ID, err)
		}
		return
	}

	// DedupAcquired / DedupInProgress 都执行 handler：InProgress 可能是其他实例处理中、
	// 或上次处理崩溃 / 释放失败留下的残留，跳过会丢消息；at-least-once 优先于去重。
	err := b.invokeHandler(ctx, cfg, msg)
	if err != nil {
		if dedup.active && dedup.status == DedupAcquired {
			b.releaseDedup(ctx, cfg, msg, dedup.key)
		}
		b.observer.OnHandleFailed(ctx, msg, err)
		if herr := b.handleFailure(ctx, cfg, msg, err); herr != nil {
			b.reportError(ctx, "handle_failure", cfg.Topic, cfg.Group, msg.ID, herr)
		}
		return
	}
	if dedup.active {
		b.markDedupDone(ctx, cfg, msg, dedup.key)
	}
	if ackErr := b.runtime.Ack(ctx, msg); ackErr != nil {
		b.reportError(ctx, "ack", cfg.Topic, cfg.Group, msg.ID, ackErr)
	}
	b.observer.OnHandled(ctx, msg)
}

// dedupOutcome 描述一次去重占位结果。active=false 表示本消息未参与去重
// （未注入 DedupStore、无 MessageID、KeyFunc 返回空 key、或存储错误 fail-open）。
type dedupOutcome struct {
	key    string
	status DedupStatus
	active bool
}

// acquireDedup 尝试为消息占位。存储错误时 fail-open（继续处理并上报错误），
// 避免去重组件故障导致消息积压。
func (b *Bus) acquireDedup(ctx context.Context, cfg ConsumerConfig, msg Message) dedupOutcome {
	if b.dedupStore == nil || msg.ID == "" {
		return dedupOutcome{}
	}
	key := b.dedupConfig.KeyFunc(msg)
	if key == "" {
		b.reportError(ctx, "dedup_key", cfg.Topic, cfg.Group, msg.ID,
			errors.New("dedup KeyFunc returned empty key, dedup disabled for this message"))
		return dedupOutcome{}
	}
	status, err := b.dedupStore.Acquire(ctx, key, b.dedupConfig.TTL)
	if err != nil {
		b.reportError(ctx, "dedup_acquire", cfg.Topic, cfg.Group, msg.ID, err)
		return dedupOutcome{}
	}
	return dedupOutcome{key: key, status: status, active: true}
}

func (b *Bus) releaseDedup(ctx context.Context, cfg ConsumerConfig, msg Message, key string) {
	if key == "" {
		return
	}
	// 必须使用不随停机取消的 context：Stop 超时 / 父 ctx 取消时若用已取消的 ctx，
	// DEL 会失败并残留占位，后续重试会被误判为重复而丢消息。
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dedupWriteTimeout)
	defer cancel()
	if err := b.dedupStore.Release(writeCtx, key); err != nil {
		b.reportError(ctx, "dedup_release", cfg.Topic, cfg.Group, msg.ID, err)
	}
}

func (b *Bus) markDedupDone(ctx context.Context, cfg ConsumerConfig, msg Message, key string) {
	if key == "" {
		return
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dedupWriteTimeout)
	defer cancel()
	if err := b.dedupStore.MarkDone(writeCtx, key, b.dedupConfig.TTL); err != nil {
		b.reportError(ctx, "dedup_mark_done", cfg.Topic, cfg.Group, msg.ID, err)
	}
}

// invokeHandler 调用 handler：先按 HandlerTimeout 设置超时，再兜底 recover，
// 避免 handler 超时不可控或 panic 使消费 goroutine 直接崩溃。
func (b *Bus) invokeHandler(ctx context.Context, cfg ConsumerConfig, msg Message) (err error) {
	if cfg.HandlerTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.HandlerTimeout)
		defer cancel()
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v\n%s", ErrHandlerPanic, r, capStack(debug.Stack()))
		}
	}()
	return cfg.Handler(ctx, msg)
}

// capStack 限制 panic 堆栈长度：panic 错误会写入 RetryTask.LastError（ZSET member）
// 和 DLQ 条目，完整堆栈（数 KB）会显著放大 Redis 内存占用。
func capStack(stack []byte) []byte {
	if len(stack) <= maxPanicStackSize {
		return stack
	}
	return append(stack[:maxPanicStackSize], []byte("\n... (stack truncated)")...)
}

func (b *Bus) runRetryWorker(pollCtx, handlerCtx context.Context) {
	defer b.wg.Done()
	ticker := time.NewTicker(b.config.RetryPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pollCtx.Done():
			return
		case <-ticker.C:
			// 存储操作与 handler 使用 handlerCtx，保证 Stop 的 drain 窗口内可靠。
			_ = b.processRetry(handlerCtx)
		}
	}
}

func (b *Bus) processRetry(ctx context.Context) error {
	tasks, err := b.runtime.PollRetry(ctx, DefaultPollBatchSize, time.Now())
	if err != nil {
		b.reportError(ctx, "poll_retry", "", "", "", err)
		return err
	}

	for _, task := range tasks {
		cfg, ok := b.findConsumer(task.Topic, task.Group)
		if !ok {
			if rerr := b.runtime.RemoveRetry(ctx, task); rerr != nil {
				b.reportError(ctx, "remove_retry", task.Topic, task.Group, task.MessageID, rerr)
			}
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

		dedup := b.acquireDedup(ctx, cfg, msg)
		if dedup.active && dedup.status == DedupDone {
			b.notifyDuplicate(ctx, msg)
			if rerr := b.runtime.RemoveRetry(ctx, task); rerr != nil {
				b.reportError(ctx, "remove_retry", task.Topic, task.Group, msg.ID, rerr)
			}
			continue
		}

		handleErr := b.invokeHandler(ctx, cfg, msg)
		if handleErr == nil {
			if dedup.active {
				b.markDedupDone(ctx, cfg, msg, dedup.key)
			}
			if rerr := b.runtime.RemoveRetry(ctx, task); rerr != nil {
				b.reportError(ctx, "remove_retry", task.Topic, task.Group, msg.ID, rerr)
			}
			b.observer.OnHandled(ctx, msg)
			continue
		}
		if dedup.active && dedup.status == DedupAcquired {
			b.releaseDedup(ctx, cfg, msg, dedup.key)
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
				b.reportError(ctx, "move_dead_letter", task.Topic, task.Group, msg.ID, err)
				// 死信写入失败时保留旧重试任务，下一轮继续尝试，避免消息丢失。
				continue
			}
			b.observer.OnDeadLettered(ctx, dlq)
			if rerr := b.runtime.RemoveRetry(ctx, task); rerr != nil {
				b.reportError(ctx, "remove_retry", task.Topic, task.Group, msg.ID, rerr)
			}
			continue
		}

		next := task
		next.Attempt = task.Attempt + 1
		next.LastError = handleErr.Error()
		next.AvailableAt = time.Now().Add(backoffDelay(cfg.RetryBaseDelay, cfg.RetryMaxDelay, next.Attempt))

		if err := b.runtime.ScheduleRetry(ctx, next); err != nil {
			b.reportError(ctx, "schedule_retry", task.Topic, task.Group, msg.ID, err)
			// 新任务写入失败时保留旧任务，下一轮继续尝试，避免重试任务丢失。
			continue
		}
		b.observer.OnRetryScheduled(ctx, next, handleErr)
		// Runtime 契约：MessageID 非空时 ScheduleRetry 按消息 ID 幂等覆盖，无需再删旧任务；
		// 老数据（无 MessageID）仍按旧语义先写后删。
		if task.MessageID == "" {
			if rerr := b.runtime.RemoveRetry(ctx, task); rerr != nil {
				b.reportError(ctx, "remove_retry", task.Topic, task.Group, msg.ID, rerr)
			}
		}
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
		AvailableAt: time.Now().Add(backoffDelay(cfg.RetryBaseDelay, cfg.RetryMaxDelay, attempt)),
		LastError:   cause.Error(),
	}
	if err := b.runtime.ScheduleRetry(ctx, task); err != nil {
		return err
	}
	b.observer.OnRetryScheduled(ctx, task, cause)
	// 重试任务已接管该消息（所有权转移），XACK 原 stream 消息，避免 PEL 泄漏。
	return b.runtime.Ack(ctx, msg)
}

// DeadLetters 返回死信队列中的消息（只读查询，不消费）。
// 需要 Runtime 实现 DeadLetterStore，否则返回 ErrDeadLetterStoreUnsupported。
func (b *Bus) DeadLetters(ctx context.Context, offset, limit int) ([]DeadLetterMessage, error) {
	store, ok := b.runtime.(DeadLetterStore)
	if !ok {
		return nil, ErrDeadLetterStoreUnsupported
	}
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = DefaultPollBatchSize
	}
	if limit > maxDeadLetterPageSize {
		limit = maxDeadLetterPageSize
	}
	return store.ListDeadLetters(ctx, offset, limit)
}

// ReplayDeadLetters 从死信队列弹出一批消息并重新进入重试队列（异步重放）。
//
// 找不到对应订阅的消息会被放回死信队列并计入 Skipped；逐条失败记录在 Errors 中。
// 需要 Runtime 实现 DeadLetterStore，否则返回 ErrDeadLetterStoreUnsupported。
func (b *Bus) ReplayDeadLetters(ctx context.Context, limit int) (ReplayResult, error) {
	result := ReplayResult{}
	store, ok := b.runtime.(DeadLetterStore)
	if !ok {
		return result, ErrDeadLetterStoreUnsupported
	}
	if limit <= 0 {
		limit = DefaultPollBatchSize
	}
	if limit > maxDeadLetterPageSize {
		limit = maxDeadLetterPageSize
	}

	deads, popErr := store.PopDeadLetters(ctx, limit)
	if popErr != nil {
		// PopDeadLetters 是破坏性操作：可能已经弹出部分记录后失败。
		// 即使出错也要处理已弹出的记录，否则这些消息会从 DLQ 消失且没有重新入队（丢消息）。
		result.Errors = append(result.Errors, fmt.Errorf("pop dead letters (already popped %d records): %w", len(deads), popErr))
		if len(deads) == 0 {
			return result, popErr
		}
	}

	replayErr := func(dlq DeadLetterMessage, err error) error {
		return fmt.Errorf("replay dead letter topic=%s group=%s message_id=%s dlq_id=%s: %w",
			dlq.Topic, dlq.Group, dlq.MessageID, dlq.ID, err)
	}

	for _, dlq := range deads {
		cfg, found := b.findConsumer(dlq.Topic, dlq.Group)
		if !found {
			if rerr := b.runtime.MoveToDeadLetter(ctx, dlq); rerr != nil {
				wrapped := replayErr(dlq, rerr)
				b.reportError(ctx, "move_dead_letter", dlq.Topic, dlq.Group, dlq.MessageID, wrapped)
				result.Errors = append(result.Errors, wrapped)
			}
			result.Skipped++
			continue
		}

		task := RetryTask{
			// 使用独立的 Key：DLQ 重放可能紧跟在 retry worker 的 DLQ 写入之后，
			// 若沿用 MessageID 稳定 member，旧任务的 RemoveRetry 可能把刚入队的新任务删掉。
			Key:         "replay:" + generateMessageID(),
			MessageID:   dlq.MessageID,
			Topic:       dlq.Topic,
			Group:       dlq.Group,
			Payload:     dlq.Payload,
			Headers:     cloneHeaders(dlq.Headers),
			Attempt:     1,
			MaxRetries:  cfg.MaxRetries,
			AvailableAt: time.Now(),
			LastError:   "replayed from dead letter: " + dlq.Error,
		}
		if serr := b.runtime.ScheduleRetry(ctx, task); serr != nil {
			wrapped := replayErr(dlq, serr)
			b.reportError(ctx, "schedule_retry", dlq.Topic, dlq.Group, dlq.MessageID, wrapped)
			result.Errors = append(result.Errors, wrapped)
			// 重新入队失败：尽量把消息放回死信队列。
			if rerr := b.runtime.MoveToDeadLetter(ctx, dlq); rerr != nil {
				wrappedBack := replayErr(dlq, rerr)
				b.reportError(ctx, "move_dead_letter", dlq.Topic, dlq.Group, dlq.MessageID, wrappedBack)
				result.Errors = append(result.Errors, wrappedBack)
			}
			continue
		}
		b.observer.OnRetryScheduled(ctx, task, nil)
		result.Replayed++
	}
	return result, nil
}

func (b *Bus) findConsumer(topic, group string) (ConsumerConfig, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	cfg, ok := b.registry[subscriptionKey(topic, group)]
	return cfg, ok
}

// reportError 把内部错误暴露给可观测性组件（ErrorObserver / logger）。
// 这是非破坏性扩展：老的自定义 Observer 无需实现 OnError。
func (b *Bus) reportError(ctx context.Context, op, topic, group, messageID string, err error) {
	if err == nil {
		return
	}
	if observer, ok := b.observer.(ErrorObserver); ok {
		observer.OnError(ctx, ErrorEvent{Op: op, Topic: topic, Group: group, MessageID: messageID, Err: err})
	}
	if b.logger != nil {
		b.logger.LogAttrs(ctx, slog.LevelError, "eventbus error",
			slog.String("op", op),
			slog.String("topic", topic),
			slog.String("group", group),
			slog.String("message_id", messageID),
			slog.String("error", err.Error()),
		)
	}
}

func (b *Bus) notifyDuplicate(ctx context.Context, msg Message) {
	if observer, ok := b.observer.(DuplicateObserver); ok {
		observer.OnDuplicate(ctx, msg)
	}
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

// backoffDelay 计算指数退避（带上限与 equal jitter），避免多实例同时重试造成惊群。
func backoffDelay(base, maxDelay time.Duration, attempt int) time.Duration {
	if base <= 0 {
		base = DefaultRetryBaseDelay
	}
	if maxDelay <= 0 {
		maxDelay = DefaultRetryMaxDelay
	}
	delay := base
	for i := 1; i < attempt; i++ {
		if delay >= maxDelay/2 {
			delay = maxDelay
			break
		}
		delay *= 2
	}
	if delay > maxDelay {
		delay = maxDelay
	}
	return jitterDelay(delay)
}

// jitterDelay 返回 [d/2, d] 范围内的抖动值（equal jitter）。
func jitterDelay(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

func generateMessageID() string {
	return snowflake.Generate()
}
