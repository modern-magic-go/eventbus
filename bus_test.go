package eventbus

import (
	"context"
	"errors"
	"sync"
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
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{consumeBySG: map[string][]Message{}}
}

func (f *fakeRuntime) Publish(_ context.Context, msg Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published = append(f.published, msg)
	return nil
}

func (f *fakeRuntime) EnsureConsumerGroup(_ context.Context, topic, group string) error {
	_ = topic
	_ = group
	return nil
}

func (f *fakeRuntime) Consume(_ context.Context, cfg ConsumerConfig) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := cfg.Topic + "|" + cfg.Group
	msgs := f.consumeBySG[key]
	f.consumeBySG[key] = nil
	return msgs, nil
}

func (f *fakeRuntime) Ack(_ context.Context, msg Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked = append(f.acked, msg)
	return nil
}

func (f *fakeRuntime) ScheduleRetry(_ context.Context, task RetryTask) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retries = append(f.retries, task)
	return nil
}

func (f *fakeRuntime) PollRetry(_ context.Context, _ int, now time.Time) ([]RetryTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]RetryTask, 0, len(f.retries))
	remain := make([]RetryTask, 0, len(f.retries))
	for _, t := range f.retries {
		if !t.AvailableAt.After(now) {
			t.rawMember = t.Topic + t.Group
			out = append(out, t)
		} else {
			remain = append(remain, t)
		}
	}
	f.retries = remain
	return out, nil
}

func (f *fakeRuntime) RemoveRetry(_ context.Context, _ RetryTask) error { return nil }

func (f *fakeRuntime) MoveToDeadLetter(_ context.Context, msg DeadLetterMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dlq = append(f.dlq, msg)
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

	attempts := 0
	require.NoError(t, bus.Subscribe(ConsumerConfig{
		Topic:      "topic.order",
		Group:      "worker",
		MaxRetries: 1,
		RetryBaseDelay: 20 * time.Millisecond,
		Handler: func(_ context.Context, _ Message) error {
			attempts++
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

	assert.GreaterOrEqual(t, attempts, 1)
	require.NotEmpty(t, rt.dlq)
	assert.Equal(t, "topic.order", rt.dlq[0].Topic)
}
