package core

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRuntime struct {
	mu sync.Mutex

	published   []Message
	acked       []Message
	retries     []RetryTask
	dlq         []DeadLetterMessage
	consumeBySG map[string][]Message

	claimQueue []Message
	claimCalls int

	// events 记录 schedule / ack / dlq 的调用顺序，用于断言“先转移所有权，再 XACK”。
	events    []string
	ensureErr error

	// 故障注入与写入失败重试相关计数。
	scheduleErr      error
	dlqErr           error
	scheduleCalls    int
	dlqCalls         int
	removeRetryCalls int
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{consumeBySG: map[string][]Message{}}
}

func (f *fakeRuntime) ackedMessages() []Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Message(nil), f.acked...)
}

func (f *fakeRuntime) retryTasks() []RetryTask {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]RetryTask(nil), f.retries...)
}

func (f *fakeRuntime) deadLetters() []DeadLetterMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]DeadLetterMessage(nil), f.dlq...)
}

func (f *fakeRuntime) claims() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claimCalls
}

func (f *fakeRuntime) eventLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...)
}

func (f *fakeRuntime) setEnsureErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureErr = err
}

func (f *fakeRuntime) setWriteErrors(scheduleErr, dlqErr error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scheduleErr = scheduleErr
	f.dlqErr = dlqErr
}

func (f *fakeRuntime) writeFailureCounts() (schedule, dlq, removeRetry int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.scheduleCalls, f.dlqCalls, f.removeRetryCalls
}

func (f *fakeRuntime) Publish(_ context.Context, msg Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published = append(f.published, msg)
	return nil
}

func (f *fakeRuntime) EnsureConsumerGroup(_ context.Context, topic, group string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	_ = topic
	_ = group
	return f.ensureErr
}

func (f *fakeRuntime) Consume(_ context.Context, cfg ConsumerConfig) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := cfg.Topic + "|" + cfg.Group
	msgs := f.consumeBySG[key]
	f.consumeBySG[key] = nil
	return msgs, nil
}

// ClaimPending 让 fakeRuntime 实现 PendingReclaimer，便于单测 pending 回收路径。
func (f *fakeRuntime) ClaimPending(_ context.Context, _ ConsumerConfig) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimCalls++
	msgs := f.claimQueue
	f.claimQueue = nil
	return msgs, nil
}

func (f *fakeRuntime) Ack(_ context.Context, msg Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked = append(f.acked, msg)
	f.events = append(f.events, "ack")
	return nil
}

func (f *fakeRuntime) ScheduleRetry(_ context.Context, task RetryTask) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scheduleCalls++
	if f.scheduleErr != nil {
		return f.scheduleErr
	}
	f.retries = append(f.retries, task)
	f.events = append(f.events, "schedule")
	return nil
}

func (f *fakeRuntime) PollRetry(_ context.Context, _ int, now time.Time) ([]RetryTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]RetryTask, 0, len(f.retries))
	remain := make([]RetryTask, 0, len(f.retries))
	for _, t := range f.retries {
		if !t.AvailableAt.After(now) {
			t = WithRawMember(t, t.Topic+t.Group)
			out = append(out, t)
		} else {
			remain = append(remain, t)
		}
	}
	f.retries = remain
	return out, nil
}

func (f *fakeRuntime) RemoveRetry(_ context.Context, _ RetryTask) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removeRetryCalls++
	return nil
}

func (f *fakeRuntime) MoveToDeadLetter(_ context.Context, msg DeadLetterMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dlqCalls++
	if f.dlqErr != nil {
		return f.dlqErr
	}
	f.dlq = append(f.dlq, msg)
	f.events = append(f.events, "dlq")
	return nil
}

func (f *fakeRuntime) Close() error { return nil }

func TestBus_IsolatedInstances(t *testing.T) {
	r1 := newFakeRuntime()
	r2 := newFakeRuntime()
	b1 := NewBus(r1)
	b2 := NewBus(r2)

	require.NoError(t, b1.Subscribe(ConsumerConfig{Topic: "t", Group: "g1", Handler: func(context.Context, Message) error { return nil }}))
	require.NoError(t, b2.Subscribe(ConsumerConfig{Topic: "t", Group: "g2", Handler: func(context.Context, Message) error { return nil }}))

	ctx := context.Background()
	require.NoError(t, b1.Publish(ctx, "t", map[string]any{"k": "v1"}))
	require.NoError(t, b2.Publish(ctx, "t", map[string]any{"k": "v2"}))

	assert.Len(t, r1.published, 1)
	assert.Len(t, r2.published, 1)
	assert.NotEqual(t, string(r1.published[0].Payload), string(r2.published[0].Payload))
}

func TestBus_RetryAndDLQ(t *testing.T) {
	rt := newFakeRuntime()
	bus := NewBus(rt, WithRetryPollInterval(20*time.Millisecond))

	attempts := atomic.Int32{}
	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic:          "topic.order",
		Group:          "worker",
		MaxRetries:     1,
		RetryBaseDelay: 20 * time.Millisecond,
		Handler: func(_ context.Context, _ Message) error {
			attempts.Add(1)
			return errors.New("boom")
		},
	}))

	rt.consumeBySG["topic.order|worker"] = []Message{{
		Topic:   "topic.order",
		Payload: []byte(`{"id":1}`),
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	time.Sleep(500 * time.Millisecond)

	assert.GreaterOrEqual(t, attempts.Load(), int32(1))
	dlq := rt.deadLetters()
	require.NotEmpty(t, dlq)
	assert.Equal(t, "topic.order", dlq[0].Topic)
}

// runtimeWithoutReclaim 只实现 Runtime、不实现 PendingReclaimer。
// 嵌入的 Runtime 为 nil，Subscribe 阶段不应调用它的任何方法。
type runtimeWithoutReclaim struct{ Runtime }

// recordingObserver 记录 Observer 回调，用于断言 panic 兜底后走了 OnHandleFailed。
type recordingObserver struct {
	mu           sync.Mutex
	handled      int
	handleFailed int
	retries      int
	deadLetters  int
	lastFailure  error
}

func (o *recordingObserver) OnPublished(context.Context, Message)            {}
func (o *recordingObserver) OnPublishFailed(context.Context, Message, error) {}

func (o *recordingObserver) OnHandled(context.Context, Message) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.handled++
}

func (o *recordingObserver) OnHandleFailed(_ context.Context, _ Message, cause error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.handleFailed++
	o.lastFailure = cause
}

func (o *recordingObserver) OnRetryScheduled(context.Context, RetryTask, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.retries++
}

func (o *recordingObserver) OnDeadLettered(context.Context, DeadLetterMessage) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.deadLetters++
}

func (o *recordingObserver) snapshot() (handled, failed int, lastFailure error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.handled, o.handleFailed, o.lastFailure
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// P0-2 回归：handler 失败后，重试任务接管消息并 XACK 原消息，PEL 不残留。
func TestBus_FailedMessageAckedAfterRetryScheduled(t *testing.T) {
	rt := newFakeRuntime()
	bus := NewBus(rt, WithRetryPollInterval(5*time.Millisecond))

	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic:          "topic.order",
		Group:          "worker",
		MaxRetries:     3,
		RetryBaseDelay: time.Hour, // 让重试任务不可立刻执行，便于观察转移结果
		Handler: func(context.Context, Message) error {
			return errors.New("boom")
		},
	}))

	rt.mu.Lock()
	rt.consumeBySG["topic.order|worker"] = []Message{{
		ID:            "msg-1",
		Topic:         "topic.order",
		Payload:       []byte(`{"id":1}`),
		ConsumerGroup: "worker",
		DeliveryTag:   "1-0",
	}}
	rt.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	waitFor(t, 2*time.Second, func() bool { return len(rt.retryTasks()) == 1 })

	tasks := rt.retryTasks()
	require.Len(t, tasks, 1)
	assert.Equal(t, "msg-1", tasks[0].MessageID)
	assert.Equal(t, 1, tasks[0].Attempt)

	acked := rt.ackedMessages()
	require.Len(t, acked, 1)
	assert.Equal(t, "1-0", acked[0].DeliveryTag)
	assert.Equal(t, "msg-1", acked[0].ID)
	assert.Empty(t, rt.deadLetters())
}

// P0-3 回归：handler panic 被 recover，process 不崩溃，并按失败进入重试 / 死信。
func TestBus_HandlerPanicRecoveredAndDeadLettered(t *testing.T) {
	rt := newFakeRuntime()
	obs := &recordingObserver{}
	bus := NewBus(rt, WithObserver(obs), WithRetryPollInterval(5*time.Millisecond))

	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic:          "topic.order",
		Group:          "worker",
		MaxRetries:     1,
		RetryBaseDelay: 5 * time.Millisecond,
		Handler: func(context.Context, Message) error {
			panic("dirty payload")
		},
	}))

	rt.mu.Lock()
	rt.consumeBySG["topic.order|worker"] = []Message{{
		ID:            "msg-panic",
		Topic:         "topic.order",
		Payload:       []byte(`not-json`),
		ConsumerGroup: "worker",
		DeliveryTag:   "1-0",
	}}
	rt.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	waitFor(t, 3*time.Second, func() bool { return len(rt.deadLetters()) == 1 })

	dlq := rt.deadLetters()
	require.Len(t, dlq, 1)
	assert.Equal(t, "msg-panic", dlq[0].MessageID)
	assert.Contains(t, dlq[0].Error, "handler panic")
	assert.Contains(t, dlq[0].Error, "dirty payload")

	_, failed, lastFailure := obs.snapshot()
	assert.GreaterOrEqual(t, failed, 1)
	require.Error(t, lastFailure)
	assert.ErrorIs(t, lastFailure, ErrHandlerPanic)

	// 原消息在失败转移（retry）时被 XACK 一次，之后走重试队列，不应重复 Ack。
	assert.Len(t, rt.ackedMessages(), 1)
}

// 启用 PendingReclaim 但 Runtime 不支持时必须尽早失败（Subscribe 返回错误）。
func TestBus_PendingReclaimRequiresRuntimeSupport(t *testing.T) {
	bus := NewBus(runtimeWithoutReclaim{})

	err := bus.Subscribe(ConsumerConfig{
		Topic:   "topic.order",
		Group:   "worker",
		Handler: func(context.Context, Message) error { return nil },
		PendingReclaim: PendingReclaimConfig{
			Interval: time.Second,
		},
	})

	require.ErrorIs(t, err, ErrPendingReclaimUnsupported)
}

// P0-1 回归：PEL 中空闲超时的消息被认领后走正常处理路径并 XACK。
func TestBus_PendingMessagesAreReclaimed(t *testing.T) {
	rt := newFakeRuntime()
	bus := NewBus(rt)

	handled := make(chan string, 1)
	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic: "topic.order",
		Group: "worker",
		PendingReclaim: PendingReclaimConfig{
			MinIdle:  time.Millisecond,
			Interval: 5 * time.Millisecond,
		},
		Handler: func(_ context.Context, msg Message) error {
			select {
			case handled <- msg.ID:
			default:
			}
			return nil
		},
	}))

	rt.mu.Lock()
	rt.claimQueue = []Message{{
		ID:            "msg-claimed",
		Topic:         "topic.order",
		Payload:       []byte(`{"id":1}`),
		ConsumerGroup: "worker",
		DeliveryTag:   "9-0",
	}}
	rt.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	select {
	case id := <-handled:
		assert.Equal(t, "msg-claimed", id)
	case <-time.After(2 * time.Second):
		t.Fatal("reclaimed message was not handled")
	}

	waitFor(t, time.Second, func() bool { return len(rt.ackedMessages()) == 1 })
	assert.GreaterOrEqual(t, rt.claims(), 1)
	assert.Equal(t, "9-0", rt.ackedMessages()[0].DeliveryTag)
}

// P0-1 回归：认领后的消息失败同样受 MaxRetries / 重试队列约束。
func TestBus_ReclaimedMessageFailureScheduledRetry(t *testing.T) {
	rt := newFakeRuntime()
	bus := NewBus(rt, WithRetryPollInterval(5*time.Millisecond))

	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic:          "topic.order",
		Group:          "worker",
		MaxRetries:     3,
		RetryBaseDelay: time.Hour, // 不执行重试，只验证首次失败后的转移
		PendingReclaim: PendingReclaimConfig{
			MinIdle:  time.Millisecond,
			Interval: 5 * time.Millisecond,
		},
		Handler: func(context.Context, Message) error {
			return errors.New("boom")
		},
	}))

	rt.mu.Lock()
	rt.claimQueue = []Message{{
		ID:            "msg-claimed-fail",
		Topic:         "topic.order",
		Payload:       []byte(`{"id":1}`),
		ConsumerGroup: "worker",
		DeliveryTag:   "9-0",
	}}
	rt.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	waitFor(t, 2*time.Second, func() bool { return len(rt.retryTasks()) == 1 })

	tasks := rt.retryTasks()
	require.Len(t, tasks, 1)
	assert.Equal(t, "msg-claimed-fail", tasks[0].MessageID)
	assert.Equal(t, 1, tasks[0].Attempt)
	require.Len(t, rt.ackedMessages(), 1)
	assert.Equal(t, "9-0", rt.ackedMessages()[0].DeliveryTag)
}

// 回归：失败 -> 重试成功的链路，原始消息 ID 在重试时保留，且原消息只 Ack 一次。
func TestBus_RetrySuccessKeepsMessageIDAndAcksOnce(t *testing.T) {
	rt := newFakeRuntime()
	bus := NewBus(rt, WithRetryPollInterval(5*time.Millisecond))

	var attempts atomic.Int32
	ids := make(chan string, 2)
	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic:          "topic.order",
		Group:          "worker",
		MaxRetries:     3,
		RetryBaseDelay: 5 * time.Millisecond,
		Handler: func(_ context.Context, msg Message) error {
			ids <- msg.ID
			if attempts.Add(1) == 1 {
				return errors.New("boom")
			}
			return nil
		},
	}))

	rt.mu.Lock()
	rt.consumeBySG["topic.order|worker"] = []Message{{
		ID:            "msg-retry",
		Topic:         "topic.order",
		Payload:       []byte(`{"id":1}`),
		ConsumerGroup: "worker",
		DeliveryTag:   "1-0",
	}}
	rt.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	waitFor(t, 2*time.Second, func() bool {
		return attempts.Load() >= 2 && len(rt.deadLetters()) == 0
	})

	var seen []string
	for len(seen) < 2 {
		select {
		case id := <-ids:
			seen = append(seen, id)
		case <-time.After(time.Second):
			t.Fatalf("only got %d handler calls", len(seen))
		}
	}
	assert.Equal(t, []string{"msg-retry", "msg-retry"}, seen)
	assert.Len(t, rt.ackedMessages(), 1)
	assert.Len(t, rt.deadLetters(), 0)
}

// 未启用 PendingReclaim 时不应调用 ClaimPending（零值关闭语义）。
func TestBus_PendingReclaimDisabledByDefault(t *testing.T) {
	rt := newFakeRuntime()
	bus := NewBus(rt)

	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic:   "topic.order",
		Group:   "worker",
		Handler: func(context.Context, Message) error { return nil },
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	time.Sleep(100 * time.Millisecond)
	bus.Stop()

	assert.Equal(t, 0, rt.claims())
}

// 顺序回归：失败时必须先写入重试任务（所有权转移），成功后才 XACK 原消息。
func TestBus_SchedulesRetryBeforeAck(t *testing.T) {
	rt := newFakeRuntime()
	// 拉长 retry 轮询，避免 retry worker 干扰事件顺序。
	bus := NewBus(rt, WithRetryPollInterval(time.Hour))

	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic:          "topic.order",
		Group:          "worker",
		MaxRetries:     3,
		RetryBaseDelay: time.Hour,
		Handler: func(context.Context, Message) error {
			return errors.New("boom")
		},
	}))

	rt.mu.Lock()
	rt.consumeBySG["topic.order|worker"] = []Message{{
		ID:            "msg-order",
		Topic:         "topic.order",
		Payload:       []byte(`{"id":1}`),
		ConsumerGroup: "worker",
		DeliveryTag:   "1-0",
	}}
	rt.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))
	defer bus.Stop()

	waitFor(t, 2*time.Second, func() bool { return len(rt.ackedMessages()) == 1 })

	events := rt.eventLog()
	scheduleIdx, ackIdx := -1, -1
	for i, event := range events {
		switch event {
		case "schedule":
			if scheduleIdx < 0 {
				scheduleIdx = i
			}
		case "ack":
			if ackIdx < 0 {
				ackIdx = i
			}
		}
	}
	require.NotEqual(t, -1, scheduleIdx, "应写入重试任务: %v", events)
	require.NotEqual(t, -1, ackIdx, "应 XACK 原消息: %v", events)
	assert.Less(t, scheduleIdx, ackIdx, "必须先写入重试任务再 XACK: %v", events)
}

// Start 在 EnsureConsumerGroup 失败时不应留下半启动的 worker，且修复后可重新 Start。
func TestBus_StartFailureDoesNotStartWorkersAndCanRetry(t *testing.T) {
	rt := newFakeRuntime()
	bus := NewBus(rt)

	handled := make(chan struct{}, 1)
	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic: "topic.order",
		Group: "worker",
		Handler: func(context.Context, Message) error {
			select {
			case handled <- struct{}{}:
			default:
			}
			return nil
		},
	}))

	rt.setEnsureErr(errors.New("redis down"))
	require.Error(t, bus.Start(context.Background()))
	select {
	case <-handled:
		t.Fatal("Start 失败后不应有 worker 消费消息")
	case <-time.After(50 * time.Millisecond):
	}

	rt.setEnsureErr(nil)
	rt.mu.Lock()
	rt.consumeBySG["topic.order|worker"] = []Message{{
		ID:            "msg-retry-start",
		Topic:         "topic.order",
		Payload:       []byte(`{"id":1}`),
		ConsumerGroup: "worker",
		DeliveryTag:   "1-0",
	}}
	rt.mu.Unlock()

	require.NoError(t, bus.Start(context.Background()))
	defer bus.Stop()

	select {
	case <-handled:
	case <-time.After(2 * time.Second):
		t.Fatal("重新 Start 后应恢复消费")
	}
}

// panic 堆栈必须截断，避免 RetryTask.LastError / DLQ 条目被数 KB 堆栈放大。
func TestBus_PanicStackIsTruncated(t *testing.T) {
	bus := NewBus(newFakeRuntime())

	var recurse func(int)
	recurse = func(n int) {
		if n == 0 {
			panic("deep panic")
		}
		recurse(n - 1)
	}

	err := bus.invokeHandler(context.Background(), func(context.Context, Message) error {
		recurse(500)
		return nil
	}, Message{})
	require.ErrorIs(t, err, ErrHandlerPanic)
	assert.Contains(t, err.Error(), "stack truncated")
	assert.LessOrEqual(t, len(err.Error()), maxPanicStackSize+64)
}

// retry worker 写入新重试任务失败时必须保留旧任务，不能 RemoveRetry（否则重试任务丢失）。
func TestBus_RetryWorkerKeepsTaskWhenScheduleFails(t *testing.T) {
	rt := newFakeRuntime()
	bus := NewBus(rt, WithRetryPollInterval(5*time.Millisecond))

	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic:          "topic.order",
		Group:          "worker",
		MaxRetries:     3,
		RetryBaseDelay: 5 * time.Millisecond,
		Handler: func(context.Context, Message) error {
			return errors.New("boom")
		},
	}))

	rt.setWriteErrors(errors.New("redis down"), nil)
	rt.mu.Lock()
	rt.retries = []RetryTask{{
		MessageID:   "msg-write-fail",
		Topic:       "topic.order",
		Group:       "worker",
		Payload:     []byte(`{"id":1}`),
		Attempt:     1,
		MaxRetries:  3,
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

	schedule, _, removeRetry := rt.writeFailureCounts()
	assert.GreaterOrEqual(t, schedule, 1)
	assert.Equal(t, 0, removeRetry, "写入新任务失败时不应删除旧重试任务")
}

// retry worker 写死信失败时同样必须保留旧任务。
func TestBus_RetryWorkerKeepsTaskWhenDLQFails(t *testing.T) {
	rt := newFakeRuntime()
	bus := NewBus(rt, WithRetryPollInterval(5*time.Millisecond))

	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic:          "topic.order",
		Group:          "worker",
		MaxRetries:     1,
		RetryBaseDelay: 5 * time.Millisecond,
		Handler: func(context.Context, Message) error {
			return errors.New("boom")
		},
	}))

	rt.setWriteErrors(nil, errors.New("redis down"))
	rt.mu.Lock()
	rt.retries = []RetryTask{{
		MessageID:   "msg-dlq-fail",
		Topic:       "topic.order",
		Group:       "worker",
		Payload:     []byte(`{"id":1}`),
		Attempt:     1,
		MaxRetries:  1,
		AvailableAt: time.Now().Add(-time.Second),
	}}
	rt.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, bus.Start(ctx))

	waitFor(t, 2*time.Second, func() bool {
		_, dlq, _ := rt.writeFailureCounts()
		return dlq >= 1
	})
	bus.Stop()

	_, dlq, removeRetry := rt.writeFailureCounts()
	assert.GreaterOrEqual(t, dlq, 1)
	assert.Equal(t, 0, removeRetry, "写死信失败时不应删除旧重试任务")
}
