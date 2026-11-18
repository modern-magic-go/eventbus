package core

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runtimeWithoutInitializer 只实现 Runtime，不实现 ConsumerGroupInitializer。
type runtimeWithoutInitializer struct{ Runtime }

// runtimeWithoutDeadLetter 只实现 Runtime，不实现 DeadLetterStore。
type runtimeWithoutDeadLetter struct{ Runtime }

const (
	fakeDedupPending = "pending"
	fakeDedupDone    = "done"
)

type fakeDedupStore struct {
	mu          sync.Mutex
	keys        map[string]string
	acquireErr  error
	markDoneErr error
	releaseErr  error
	acquires    int
	markDones   int
	releases    int

	lastReleaseCtxErr  error
	lastMarkDoneCtxErr error
}

func newFakeDedupStore() *fakeDedupStore {
	return &fakeDedupStore{keys: map[string]string{}}
}

func (s *fakeDedupStore) seed(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[key] = value
}

func (s *fakeDedupStore) Acquire(_ context.Context, key string, _ time.Duration) (DedupStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acquires++
	if s.acquireErr != nil {
		return DedupInProgress, s.acquireErr
	}
	switch s.keys[key] {
	case "":
		s.keys[key] = fakeDedupPending
		return DedupAcquired, nil
	case fakeDedupDone:
		return DedupDone, nil
	default:
		return DedupInProgress, nil
	}
}

func (s *fakeDedupStore) MarkDone(ctx context.Context, key string, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastMarkDoneCtxErr = ctx.Err()
	s.markDones++
	if s.markDoneErr != nil {
		return s.markDoneErr
	}
	s.keys[key] = fakeDedupDone
	return nil
}

func (s *fakeDedupStore) Release(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastReleaseCtxErr = ctx.Err()
	s.releases++
	if s.releaseErr != nil {
		return s.releaseErr
	}
	delete(s.keys, key)
	return nil
}

func (s *fakeDedupStore) counts() (acquires, releases, markDones int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acquires, s.releases, s.markDones
}

func (s *fakeDedupStore) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.keys)
}

// ---------- StartFrom / ConsumerGroupInitializer ----------

func TestBus_StartFromValidationAndInitializer(t *testing.T) {
	rt := newFakeRuntime()
	bus := NewBus(rt)

	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic: "latest", Group: "g1", StartFrom: StartFromLatest,
		Handler: func(context.Context, Message) error { return nil },
	}))
	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic: "timestamp", Group: "g2", StartFrom: "1700000000000",
		Handler: func(context.Context, Message) error { return nil },
	}))

	err := bus.Subscribe(ConsumerConfig{
		Topic: "invalid", Group: "g3", StartFrom: "not-a-time",
		Handler: func(context.Context, Message) error { return nil },
	})
	require.ErrorIs(t, err, ErrInvalidStartFrom)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	started := rt.ensureFromStarted()
	require.Len(t, started, 2)
	assert.Contains(t, started, "latest|g1|$")
	assert.Contains(t, started, "timestamp|g2|1700000000000-0")
}

func TestBus_StartFromUnsupported(t *testing.T) {
	bus := NewBus(runtimeWithoutInitializer{})

	err := bus.Subscribe(ConsumerConfig{
		Topic: "t", Group: "g", StartFrom: StartFromLatest,
		Handler: func(context.Context, Message) error { return nil },
	})

	require.ErrorIs(t, err, ErrStartFromUnsupported)
}

// ---------- 退避上限与 jitter ----------

func TestBackoffDelayCapAndJitter(t *testing.T) {
	base := time.Second
	maxDelay := 10 * time.Minute

	assertBetween := func(t *testing.T, d, lo, hi time.Duration) {
		t.Helper()
		assert.GreaterOrEqual(t, d, lo, "delay=%s", d)
		assert.LessOrEqual(t, d, hi, "delay=%s", d)
	}

	// attempt 1：base 的 [1/2, 1]。
	for i := 0; i < 20; i++ {
		assertBetween(t, backoffDelay(base, maxDelay, 1), base/2, base)
	}
	// attempt 2：2*base 的 [1/2, 1]。
	for i := 0; i < 20; i++ {
		assertBetween(t, backoffDelay(base, maxDelay, 2), base, 2*base)
	}
	// 超过上限后固定为 maxDelay 的 [1/2, 1]。
	for i := 0; i < 20; i++ {
		assertBetween(t, backoffDelay(base, maxDelay, 20), maxDelay/2, maxDelay)
	}

	// base 大于上限时，以 maxDelay 为准。
	for i := 0; i < 20; i++ {
		assertBetween(t, backoffDelay(maxDelay*2, maxDelay, 3), maxDelay/2, maxDelay)
	}

	// 默认值兜底：非法参数不应 panic 或返回 0。
	for i := 0; i < 10; i++ {
		assert.Positive(t, backoffDelay(0, 0, 1))
	}
}

// ---------- handler 超时 ----------

func TestBus_HandlerTimeoutSchedulesRetry(t *testing.T) {
	rt := newFakeRuntime()
	bus := NewBus(rt, WithRetryPollInterval(5*time.Millisecond))

	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic:          "topic.order",
		Group:          "worker",
		MaxRetries:     3,
		RetryBaseDelay: time.Hour, // 不让重试任务立刻执行
		HandlerTimeout: 30 * time.Millisecond,
		Handler: func(ctx context.Context, _ Message) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}))

	rt.mu.Lock()
	rt.consumeBySG["topic.order|worker"] = []Message{{
		ID: "msg-timeout", Topic: "topic.order", Payload: []byte(`1`),
		ConsumerGroup: "worker", DeliveryTag: "1-0",
	}}
	rt.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	waitFor(t, 2*time.Second, func() bool { return len(rt.retryTasks()) == 1 })
	tasks := rt.retryTasks()
	require.Len(t, tasks, 1)
	assert.Equal(t, "msg-timeout", tasks[0].MessageID)
	assert.Contains(t, tasks[0].LastError, "deadline exceeded")
}

// ---------- 幂等去重 ----------

func TestBus_DedupSkipsDuplicate(t *testing.T) {
	rt := newFakeRuntime()
	obs := &recordingObserver{}
	store := newFakeDedupStore()
	bus := NewBus(rt, WithObserver(obs), WithDedup(store, DedupConfig{TTL: time.Hour}))

	var calls atomic.Int32
	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic: "topic.order", Group: "worker",
		Handler: func(context.Context, Message) error {
			calls.Add(1)
			return nil
		},
	}))

	rt.mu.Lock()
	rt.consumeBySG["topic.order|worker"] = []Message{
		{ID: "dup-1", Topic: "topic.order", Payload: []byte(`1`), ConsumerGroup: "worker", DeliveryTag: "1-0"},
		{ID: "dup-1", Topic: "topic.order", Payload: []byte(`1`), ConsumerGroup: "worker", DeliveryTag: "2-0"},
	}
	rt.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	waitFor(t, 2*time.Second, func() bool { return calls.Load() == 1 && len(rt.ackedMessages()) == 2 })
	assert.Equal(t, 1, obs.duplicateCount())
	assert.Equal(t, 1, store.size())
}

func TestBus_DedupReleaseOnFailure(t *testing.T) {
	rt := newFakeRuntime()
	store := newFakeDedupStore()
	bus := NewBus(rt, WithDedup(store, DedupConfig{TTL: time.Hour}), WithRetryPollInterval(5*time.Millisecond))

	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic: "topic.order", Group: "worker",
		MaxRetries: 3, RetryBaseDelay: time.Hour,
		Handler: func(context.Context, Message) error { return errors.New("boom") },
	}))

	rt.mu.Lock()
	rt.consumeBySG["topic.order|worker"] = []Message{{
		ID: "dedup-fail", Topic: "topic.order", Payload: []byte(`1`), ConsumerGroup: "worker", DeliveryTag: "1-0",
	}}
	rt.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	waitFor(t, 2*time.Second, func() bool { return len(rt.retryTasks()) == 1 })
	_, releases, _ := store.counts()
	assert.Equal(t, 1, releases, "失败后应释放去重占位，允许重试继续处理")
	assert.Equal(t, 0, store.size())
}

// ---------- drain / 停机 ----------

func TestBus_DrainWaitsForInflightHandler(t *testing.T) {
	rt := newFakeRuntime()
	bus := NewBus(rt)

	started := make(chan struct{})
	release := make(chan struct{})
	ctxErr := make(chan error, 1)
	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic: "topic.order", Group: "worker",
		Handler: func(ctx context.Context, _ Message) error {
			close(started)
			<-release
			ctxErr <- ctx.Err()
			return nil
		},
	}))

	rt.mu.Lock()
	rt.consumeBySG["topic.order|worker"] = []Message{{
		ID: "msg-drain", Topic: "topic.order", Payload: []byte(`1`), ConsumerGroup: "worker", DeliveryTag: "1-0",
	}}
	rt.mu.Unlock()

	require.NoError(t, bus.Start(context.Background()))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler 未启动")
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- bus.StopWithTimeout(2 * time.Second) }()

	// Stop 已发起，但 handler 还在执行：不应取消 handler ctx。
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-ctxErr:
		t.Fatalf("handler ctx 在 drain 完成前被取消: %v", err)
	default:
	}

	close(release)
	select {
	case err := <-stopDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Stop 未在超时前返回")
	}
	require.NoError(t, <-ctxErr)
	assert.Len(t, rt.ackedMessages(), 1)
}

func TestBus_StopTimeoutReturnsErrStopTimeout(t *testing.T) {
	rt := newFakeRuntime()
	bus := NewBus(rt)

	started := make(chan struct{})
	done := make(chan struct{})
	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic: "topic.order", Group: "worker",
		Handler: func(ctx context.Context, _ Message) error {
			close(started)
			<-ctx.Done()
			close(done)
			return ctx.Err()
		},
	}))

	rt.mu.Lock()
	rt.consumeBySG["topic.order|worker"] = []Message{{
		ID: "msg-timeout-stop", Topic: "topic.order", Payload: []byte(`1`), ConsumerGroup: "worker", DeliveryTag: "1-0",
	}}
	rt.mu.Unlock()

	require.NoError(t, bus.Start(context.Background()))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler 未启动")
	}

	err := bus.StopWithTimeout(50 * time.Millisecond)
	require.ErrorIs(t, err, ErrStopTimeout)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("超时后 handler ctx 应被取消")
	}
}

// ---------- 死信查询 / 重放 ----------

func TestBus_DeadLetterQueryAndReplay(t *testing.T) {
	rt := newFakeRuntime()
	bus := NewBus(rt, WithRetryPollInterval(5*time.Millisecond))

	handled := make(chan string, 2)
	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic: "topic.order", Group: "worker",
		MaxRetries: 3, RetryBaseDelay: time.Millisecond,
		Handler: func(_ context.Context, msg Message) error {
			handled <- msg.ID
			return nil
		},
	}))

	rt.mu.Lock()
	rt.dlq = []DeadLetterMessage{
		{ID: "dlq-id-1", MessageID: "msg-replay", Topic: "topic.order", Group: "worker", Payload: []byte(`{"id":1}`), Attempt: 3, MaxRetries: 3, Error: "boom"},
		{ID: "dlq-id-2", MessageID: "msg-orphan", Topic: "unknown.topic", Group: "worker", Payload: []byte(`{"id":2}`), Attempt: 3, MaxRetries: 3, Error: "boom"},
	}
	rt.mu.Unlock()

	deads, err := bus.DeadLetters(context.Background(), 0, 10)
	require.NoError(t, err)
	require.Len(t, deads, 2)
	assert.Equal(t, "dlq-id-1", deads[0].ID)

	res, err := bus.ReplayDeadLetters(context.Background(), 10)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Replayed)
	assert.Equal(t, 1, res.Skipped)
	assert.Empty(t, res.Errors)

	tasks := rt.retryTasks()
	require.Len(t, tasks, 1)
	assert.Equal(t, "msg-replay", tasks[0].MessageID)
	assert.Equal(t, 1, tasks[0].Attempt)

	// 找不到订阅的消息应被放回死信队列。
	deads, err = bus.DeadLetters(context.Background(), 0, 10)
	require.NoError(t, err)
	require.Len(t, deads, 1)
	assert.Equal(t, "msg-orphan", deads[0].MessageID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	select {
	case id := <-handled:
		assert.Equal(t, "msg-replay", id)
	case <-time.After(2 * time.Second):
		t.Fatal("重放消息未被重试队列处理")
	}
}

func TestBus_DeadLetterStoreUnsupported(t *testing.T) {
	bus := NewBus(runtimeWithoutDeadLetter{})

	_, err := bus.DeadLetters(context.Background(), 0, 10)
	require.ErrorIs(t, err, ErrDeadLetterStoreUnsupported)

	_, err = bus.ReplayDeadLetters(context.Background(), 10)
	require.ErrorIs(t, err, ErrDeadLetterStoreUnsupported)
}

// ---------- 重试任务幂等换位 ----------

func TestBus_RetryRescheduleSkipsRemoveForMessageIDTasks(t *testing.T) {
	rt := newFakeRuntime()
	bus := NewBus(rt, WithRetryPollInterval(5*time.Millisecond))

	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic: "topic.order", Group: "worker",
		MaxRetries: 3, RetryBaseDelay: time.Hour,
		Handler: func(context.Context, Message) error { return errors.New("boom") },
	}))

	rt.mu.Lock()
	rt.retries = []RetryTask{{
		MessageID: "msg-idem", Topic: "topic.order", Group: "worker",
		Payload: []byte(`1`), Attempt: 1, MaxRetries: 3,
		AvailableAt: time.Now().Add(-time.Second),
	}}
	rt.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	waitFor(t, 2*time.Second, func() bool {
		schedule, _, _ := rt.writeFailureCounts()
		return schedule >= 1
	})
	bus.Stop()
	_, _, removeRetry := rt.writeFailureCounts()
	assert.Equal(t, 0, removeRetry, "MessageID 非空时由 Runtime 幂等覆盖，不应再 RemoveRetry")
}

func TestBus_RetryRescheduleRemovesLegacyTask(t *testing.T) {
	rt := newFakeRuntime()
	bus := NewBus(rt, WithRetryPollInterval(5*time.Millisecond))

	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic: "topic.order", Group: "worker",
		MaxRetries: 3, RetryBaseDelay: time.Hour,
		Handler: func(context.Context, Message) error { return errors.New("boom") },
	}))

	rt.mu.Lock()
	rt.retries = []RetryTask{{
		// 模拟 v0.2 及更早入队的老任务：没有 MessageID。
		Topic: "topic.order", Group: "worker",
		Payload: []byte(`1`), Attempt: 1, MaxRetries: 3,
		AvailableAt: time.Now().Add(-time.Second),
	}}
	rt.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	waitFor(t, 2*time.Second, func() bool {
		schedule, _, _ := rt.writeFailureCounts()
		return schedule >= 1
	})
	bus.Stop()
	_, _, removeRetry := rt.writeFailureCounts()
	assert.GreaterOrEqual(t, removeRetry, 1, "老任务没有稳定 member，仍按先写后删处理")
}

// ---------- 内部错误可观测性 ----------

func TestBus_ConsumeErrorsReported(t *testing.T) {
	rt := newFakeRuntime()
	rt.setConsumeErr(errors.New("redis down"))

	obs := &recordingObserver{}
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	bus := NewBus(rt, WithObserver(obs), WithLogger(logger))

	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic: "topic.order", Group: "worker",
		Handler: func(context.Context, Message) error { return nil },
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))

	waitFor(t, 2*time.Second, func() bool { return len(obs.errorsSnapshot()) >= 1 })
	bus.Stop()

	events := obs.errorsSnapshot()
	require.NotEmpty(t, events)
	assert.Equal(t, "consume", events[0].Op)
	assert.Equal(t, "topic.order", events[0].Topic)
	require.Error(t, events[0].Err)
	assert.Contains(t, buf.String(), "consume")
	assert.Contains(t, buf.String(), "redis down")
}

// 去重占位处于 InProgress（其他实例处理中或崩溃残留）时必须继续处理，不能跳过丢消息。
func TestBus_DedupInProgressProcessesMessage(t *testing.T) {
	rt := newFakeRuntime()
	store := newFakeDedupStore()
	store.seed("topic.order|worker|msg-in-progress", fakeDedupPending)
	bus := NewBus(rt, WithDedup(store, DedupConfig{TTL: time.Hour}))

	var calls atomic.Int32
	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic: "topic.order", Group: "worker",
		Handler: func(context.Context, Message) error {
			calls.Add(1)
			return nil
		},
	}))

	rt.mu.Lock()
	rt.consumeBySG["topic.order|worker"] = []Message{{
		ID: "msg-in-progress", Topic: "topic.order", Payload: []byte(`1`),
		ConsumerGroup: "worker", DeliveryTag: "1-0",
	}}
	rt.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	waitFor(t, 2*time.Second, func() bool { return calls.Load() == 1 })
	_, _, markDones := store.counts()
	assert.Equal(t, 1, markDones, "InProgress 处理成功后应标记 done")
	assert.Equal(t, 1, store.size())
}

// Release 失败（占位残留）时，重试不能被误判为重复而丢弃；InProgress 应继续执行 handler。
func TestBus_DedupReleaseFailureDoesNotLoseMessage(t *testing.T) {
	rt := newFakeRuntime()
	store := newFakeDedupStore()
	store.releaseErr = errors.New("redis down")
	bus := NewBus(rt, WithDedup(store, DedupConfig{TTL: time.Hour}),
		WithRetryPollInterval(5*time.Millisecond))

	var calls atomic.Int32
	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic: "topic.order", Group: "worker",
		MaxRetries: 3, RetryBaseDelay: 5 * time.Millisecond,
		Handler: func(context.Context, Message) error {
			if calls.Add(1) == 1 {
				return errors.New("boom")
			}
			return nil
		},
	}))

	rt.mu.Lock()
	rt.consumeBySG["topic.order|worker"] = []Message{{
		ID: "msg-release-fail", Topic: "topic.order", Payload: []byte(`1`),
		ConsumerGroup: "worker", DeliveryTag: "1-0",
	}}
	rt.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	waitFor(t, 2*time.Second, func() bool { return calls.Load() == 2 })
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(2), calls.Load(), "重试不应被误判为重复而跳过")
	assert.Empty(t, rt.deadLetters())
	assert.Equal(t, 0, len(rt.retryTasks()))
}

// KeyFunc 返回空 key 时视为不做去重（fail-open）并上报错误。
func TestBus_DedupEmptyKeyDisablesDedup(t *testing.T) {
	rt := newFakeRuntime()
	store := newFakeDedupStore()
	obs := &recordingObserver{}
	bus := NewBus(rt, WithObserver(obs), WithDedup(store, DedupConfig{
		TTL:     time.Hour,
		KeyFunc: func(Message) string { return "" },
	}))

	var calls atomic.Int32
	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic: "topic.order", Group: "worker",
		Handler: func(context.Context, Message) error {
			calls.Add(1)
			return nil
		},
	}))

	rt.mu.Lock()
	rt.consumeBySG["topic.order|worker"] = []Message{
		{ID: "empty-key-1", Topic: "topic.order", Payload: []byte(`1`), ConsumerGroup: "worker", DeliveryTag: "1-0"},
		{ID: "empty-key-1", Topic: "topic.order", Payload: []byte(`1`), ConsumerGroup: "worker", DeliveryTag: "2-0"},
	}
	rt.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	waitFor(t, 2*time.Second, func() bool { return calls.Load() == 2 })
	_, _, _ = store.counts()
	acquires, _, _ := store.counts()
	assert.Equal(t, 0, acquires, "空 key 不应调用 Acquire")

	found := false
	for _, event := range obs.errorsSnapshot() {
		if event.Op == "dedup_key" {
			found = true
		}
	}
	assert.True(t, found, "空 key 应上报 dedup_key 错误")
}

// ReplayDeadLetters：Pop 部分成功后报错，也不能丢弃已弹出的记录。
func TestBus_ReplayPartialPopErrorProcessesRecords(t *testing.T) {
	rt := newFakeRuntime()
	bus := NewBus(rt, WithRetryPollInterval(5*time.Millisecond))
	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic: "topic.order", Group: "worker",
		Handler: func(context.Context, Message) error { return nil },
	}))

	rt.mu.Lock()
	rt.dlq = []DeadLetterMessage{{
		ID: "dlq-partial", MessageID: "msg-partial", Topic: "topic.order", Group: "worker",
		Payload: []byte(`1`), Attempt: 3, MaxRetries: 3, Error: "boom",
	}}
	rt.popErr = errors.New("redis down after partial pop")
	rt.mu.Unlock()

	res, err := bus.ReplayDeadLetters(context.Background(), 10)
	// Pop 报错但已有部分记录：按 best-effort 继续处理，错误记录在 Errors 中。
	require.NoError(t, err)
	assert.Equal(t, 1, res.Replayed)
	require.NotEmpty(t, res.Errors)
	assert.Contains(t, res.Errors[0].Error(), "pop dead letters")

	tasks := rt.retryTasks()
	require.Len(t, tasks, 1)
	assert.Equal(t, "msg-partial", tasks[0].MessageID)
	assert.NotEmpty(t, tasks[0].Key, "重放任务应使用独立 Key，避免与旧任务 member 冲突")
}

// CloseRuntime 关闭时序：正常停机关一次；超时停机不关闭。
func TestBus_CloseRuntimeAfterStop(t *testing.T) {
	t.Run("normal stop closes runtime", func(t *testing.T) {
		rt := newFakeRuntime()
		bus := NewBus(rt, WithCloseRuntime(true))
		require.NoError(t, bus.Subscribe(ConsumerConfig{
			Topic: "topic.order", Group: "worker",
			Handler: func(context.Context, Message) error { return nil },
		}))

		rt.mu.Lock()
		rt.consumeBySG["topic.order|worker"] = []Message{{
			ID: "msg-close", Topic: "topic.order", Payload: []byte(`1`),
			ConsumerGroup: "worker", DeliveryTag: "1-0",
		}}
		rt.mu.Unlock()

		require.NoError(t, bus.Start(context.Background()))
		waitFor(t, 2*time.Second, func() bool { return len(rt.ackedMessages()) == 1 })
		require.NoError(t, bus.Stop())
		assert.Equal(t, 1, rt.closeCount())
	})

	t.Run("timeout does not close runtime", func(t *testing.T) {
		rt := newFakeRuntime()
		bus := NewBus(rt, WithCloseRuntime(true))
		started := make(chan struct{})
		require.NoError(t, bus.Subscribe(ConsumerConfig{
			Topic: "topic.order", Group: "worker",
			Handler: func(ctx context.Context, _ Message) error {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			},
		}))

		rt.mu.Lock()
		rt.consumeBySG["topic.order|worker"] = []Message{{
			ID: "msg-close-timeout", Topic: "topic.order", Payload: []byte(`1`),
			ConsumerGroup: "worker", DeliveryTag: "1-0",
		}}
		rt.mu.Unlock()

		require.NoError(t, bus.Start(context.Background()))
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("handler 未启动")
		}
		require.ErrorIs(t, bus.StopWithTimeout(30*time.Millisecond), ErrStopTimeout)
		assert.Equal(t, 0, rt.closeCount(), "超时后不应关闭 Runtime")
	})
}

// 停机/取消场景下，Release / MarkDone 必须使用不随父 ctx 取消的 context，
// 否则去重状态写入失败会留下残留占位。
func TestDedupWritesIgnoreCanceledContext(t *testing.T) {
	rt := newFakeRuntime()
	store := newFakeDedupStore()
	bus := NewBus(rt, WithDedup(store, DedupConfig{TTL: time.Hour}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := ConsumerConfig{Topic: "topic.order", Group: "worker"}
	msg := Message{ID: "msg-canceled"}

	bus.releaseDedup(ctx, cfg, msg, "dedup-key")
	bus.markDedupDone(ctx, cfg, msg, "dedup-key")

	store.mu.Lock()
	defer store.mu.Unlock()
	assert.NoError(t, store.lastReleaseCtxErr, "Release 不应使用已取消的 ctx")
	assert.NoError(t, store.lastMarkDoneCtxErr, "MarkDone 不应使用已取消的 ctx")
}
