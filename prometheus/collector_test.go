package prometheus

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/modern-magic-go/eventbus"
)

// baseRuntime 实现 eventbus.Runtime 的全部方法，供测试内嵌使用。
type baseRuntime struct{}

func (*baseRuntime) Publish(context.Context, eventbus.Message) error { return nil }
func (*baseRuntime) EnsureConsumerGroup(context.Context, string, string) error {
	return nil
}

func (*baseRuntime) Consume(context.Context, eventbus.ConsumerConfig) ([]eventbus.Message, error) {
	return nil, nil
}
func (*baseRuntime) Ack(context.Context, eventbus.Message) error { return nil }
func (*baseRuntime) ScheduleRetry(context.Context, eventbus.RetryTask) error {
	return nil
}

func (*baseRuntime) PollRetry(context.Context, int, time.Time) ([]eventbus.RetryTask, error) {
	return nil, nil
}
func (*baseRuntime) RemoveRetry(context.Context, eventbus.RetryTask) error { return nil }
func (*baseRuntime) MoveToDeadLetter(context.Context, eventbus.DeadLetterMessage) error {
	return nil
}
func (*baseRuntime) Close() error { return nil }

func subKey(topic, group string) string { return topic + "|" + group }

// fakeStats 是可注入数值与错误的 eventbus.RuntimeStats 实现。
type fakeStats struct {
	lag            map[string]int64
	pending        map[string]int64
	retryQueue     int64
	deadLetter     int64
	errs           map[string]error
	blockStreamLag bool
}

func (s *fakeStats) StreamLag(ctx context.Context, topic, group string) (int64, error) {
	if s.blockStreamLag {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	if err := s.errs["stream_lag|"+subKey(topic, group)]; err != nil {
		return 0, err
	}
	return s.lag[subKey(topic, group)], nil
}

func (s *fakeStats) PendingCount(_ context.Context, topic, group string) (int64, error) {
	if err := s.errs["pending_messages|"+subKey(topic, group)]; err != nil {
		return 0, err
	}
	return s.pending[subKey(topic, group)], nil
}

func (s *fakeStats) RetryQueueLen(context.Context) (int64, error) {
	if err := s.errs["retry_queue_length"]; err != nil {
		return 0, err
	}
	return s.retryQueue, nil
}

func (s *fakeStats) DeadLetterLen(context.Context) (int64, error) {
	if err := s.errs["dead_letter_length"]; err != nil {
		return 0, err
	}
	return s.deadLetter, nil
}

// fakeRuntime 同时实现 eventbus.Runtime、eventbus.RuntimeStats 与死信长度查询。
type fakeRuntime struct {
	baseRuntime
	fakeStats
}

var (
	_ eventbus.Runtime      = (*fakeRuntime)(nil)
	_ eventbus.RuntimeStats = (*fakeRuntime)(nil)
	_ deadLetterLener       = (*fakeRuntime)(nil)
)

// noDeadLetterRuntime 实现 Runtime 与 RuntimeStats，但不提供 DeadLetterLen。
type noDeadLetterRuntime struct {
	baseRuntime
	stats fakeStats
}

var (
	_ eventbus.Runtime      = (*noDeadLetterRuntime)(nil)
	_ eventbus.RuntimeStats = (*noDeadLetterRuntime)(nil)
)

func (r *noDeadLetterRuntime) StreamLag(ctx context.Context, topic, group string) (int64, error) {
	return r.stats.StreamLag(ctx, topic, group)
}

func (r *noDeadLetterRuntime) PendingCount(ctx context.Context, topic, group string) (int64, error) {
	return r.stats.PendingCount(ctx, topic, group)
}

func (r *noDeadLetterRuntime) RetryQueueLen(ctx context.Context) (int64, error) {
	return r.stats.RetryQueueLen(ctx)
}

func TestCollectorGathersStats(t *testing.T) {
	rt := &fakeRuntime{fakeStats: fakeStats{
		lag:        map[string]int64{subKey("orders", "g1"): 3, subKey("payments", "g2"): 7},
		pending:    map[string]int64{subKey("orders", "g1"): 1, subKey("payments", "g2"): 2},
		retryQueue: 4,
		deadLetter: 5,
	}}
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewCollector(rt, []Subscription{
		{Topic: "orders", Group: "g1"},
		{Topic: "payments", Group: "g2"},
	}))

	// 只 Gather 一次：Collect 会累加 collect_errors，重复 Gather 会改变计数。
	samples := collectSamples(t, reg)
	assertSample(t, samples, "eventbus_stream_lag", map[string]string{"topic": "orders", "group": "g1"}, 3)
	assertSample(t, samples, "eventbus_stream_lag", map[string]string{"topic": "payments", "group": "g2"}, 7)
	assertSample(t, samples, "eventbus_pending_messages", map[string]string{"topic": "orders", "group": "g1"}, 1)
	assertSample(t, samples, "eventbus_pending_messages", map[string]string{"topic": "payments", "group": "g2"}, 2)
	assertSample(t, samples, "eventbus_retry_queue_length", nil, 4)
	assertSample(t, samples, "eventbus_dead_letter_length", nil, 5)
	assertNoSample(t, samples, "eventbus_collect_errors_total", map[string]string{"op": "runtime_stats"})
}

func TestCollectorSkipsFailedStats(t *testing.T) {
	boom := errors.New("boom")
	rt := &fakeRuntime{fakeStats: fakeStats{
		lag:     map[string]int64{subKey("payments", "g2"): 7},
		pending: map[string]int64{subKey("orders", "g1"): 1},
		errs: map[string]error{
			"stream_lag|" + subKey("orders", "g1"):         boom,
			"pending_messages|" + subKey("payments", "g2"): boom,
			"retry_queue_length":                           boom,
			"dead_letter_length":                           boom,
		},
	}}
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewCollector(rt, []Subscription{
		{Topic: "orders", Group: "g1"},
		{Topic: "payments", Group: "g2"},
	}))

	samples := collectSamples(t, reg)

	// 失败的指标被跳过，其余指标照常输出。
	assertNoSample(t, samples, "eventbus_stream_lag", map[string]string{"topic": "orders", "group": "g1"})
	assertSample(t, samples, "eventbus_stream_lag", map[string]string{"topic": "payments", "group": "g2"}, 7)
	assertSample(t, samples, "eventbus_pending_messages", map[string]string{"topic": "orders", "group": "g1"}, 1)
	assertNoSample(t, samples, "eventbus_pending_messages", map[string]string{"topic": "payments", "group": "g2"})
	assertNoSample(t, samples, "eventbus_retry_queue_length", nil)
	assertNoSample(t, samples, "eventbus_dead_letter_length", nil)

	assertSample(t, samples, "eventbus_collect_errors_total", map[string]string{"op": "stream_lag"}, 1)
	assertSample(t, samples, "eventbus_collect_errors_total", map[string]string{"op": "pending_messages"}, 1)
	assertSample(t, samples, "eventbus_collect_errors_total", map[string]string{"op": "retry_queue_length"}, 1)
	assertSample(t, samples, "eventbus_collect_errors_total", map[string]string{"op": "dead_letter_length"}, 1)
}

func TestCollectorWithoutRuntimeStats(t *testing.T) {
	// baseRuntime 只实现 eventbus.Runtime，不实现 RuntimeStats。
	rt := &baseRuntime{}
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewCollector(rt, []Subscription{{Topic: "orders", Group: "g1"}}))

	samples := collectSamples(t, reg)
	assertSample(t, samples, "eventbus_stream_lag", map[string]string{"topic": "orders", "group": "g1"}, UnavailableValue)
	assertSample(t, samples, "eventbus_pending_messages", map[string]string{"topic": "orders", "group": "g1"}, UnavailableValue)
	assertSample(t, samples, "eventbus_retry_queue_length", nil, UnavailableValue)
	assertSample(t, samples, "eventbus_dead_letter_length", nil, UnavailableValue)
	assertSample(t, samples, "eventbus_collect_errors_total", map[string]string{"op": "runtime_stats"}, 1)
}

func TestCollectorNilRuntime(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewCollector(nil, []Subscription{{Topic: "orders", Group: "g1"}}))

	samples := collectSamples(t, reg)
	assertSample(t, samples, "eventbus_stream_lag", map[string]string{"topic": "orders", "group": "g1"}, UnavailableValue)
	assertSample(t, samples, "eventbus_retry_queue_length", nil, UnavailableValue)
	assertSample(t, samples, "eventbus_collect_errors_total", map[string]string{"op": "runtime_stats"}, 1)
}

func TestCollectorWithoutDeadLetterLen(t *testing.T) {
	rt := &noDeadLetterRuntime{stats: fakeStats{
		lag:        map[string]int64{subKey("orders", "g1"): 3},
		pending:    map[string]int64{subKey("orders", "g1"): 1},
		retryQueue: 4,
	}}
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewCollector(rt, []Subscription{{Topic: "orders", Group: "g1"}}))

	samples := collectSamples(t, reg)
	assertSample(t, samples, "eventbus_stream_lag", map[string]string{"topic": "orders", "group": "g1"}, 3)
	assertSample(t, samples, "eventbus_pending_messages", map[string]string{"topic": "orders", "group": "g1"}, 1)
	assertSample(t, samples, "eventbus_retry_queue_length", nil, 4)
	// 死信长度能力缺失时输出 -1，但不计为采集错误。
	assertSample(t, samples, "eventbus_dead_letter_length", nil, UnavailableValue)
	assertNoSample(t, samples, "eventbus_collect_errors_total", map[string]string{"op": "dead_letter_length"})
}

func TestCollectorTimeout(t *testing.T) {
	rt := &fakeRuntime{fakeStats: fakeStats{
		blockStreamLag: true,
		pending:        map[string]int64{subKey("orders", "g1"): 1},
	}}
	c := NewCollector(rt, []Subscription{{Topic: "orders", Group: "g1"}}, WithCollectTimeout(20*time.Millisecond))
	if c.timeout != 20*time.Millisecond {
		t.Fatalf("timeout = %v, want 20ms", c.timeout)
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)

	start := time.Now()
	samples := collectSamples(t, reg)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Gather 耗时 %v，超时 context 未生效", elapsed)
	}

	assertNoSample(t, samples, "eventbus_stream_lag", map[string]string{"topic": "orders", "group": "g1"})
	assertSample(t, samples, "eventbus_pending_messages", map[string]string{"topic": "orders", "group": "g1"}, 1)
	assertSample(t, samples, "eventbus_collect_errors_total", map[string]string{"op": "stream_lag"}, 1)
}

func TestCollectorDefaultTimeout(t *testing.T) {
	c := NewCollector(&baseRuntime{}, nil)
	if c.timeout != defaultCollectTimeout {
		t.Fatalf("默认 timeout = %v, want %v", c.timeout, defaultCollectTimeout)
	}
	if got := NewCollector(&baseRuntime{}, nil, WithCollectTimeout(0)); got.timeout != defaultCollectTimeout {
		t.Fatalf("WithCollectTimeout(0) 后 timeout = %v, want %v", got.timeout, defaultCollectTimeout)
	}
}

func TestCollectorCopiesSubscriptions(t *testing.T) {
	subs := []Subscription{{Topic: "orders", Group: "g1"}}
	rt := &fakeRuntime{fakeStats: fakeStats{lag: map[string]int64{subKey("orders", "g1"): 3}}}
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewCollector(rt, subs))

	subs[0] = Subscription{Topic: "changed", Group: "x"}
	samples := collectSamples(t, reg)
	assertSample(t, samples, "eventbus_stream_lag", map[string]string{"topic": "orders", "group": "g1"}, 3)
	assertNoSample(t, samples, "eventbus_stream_lag", map[string]string{"topic": "changed", "group": "x"})
}

func TestCollectorDescribe(t *testing.T) {
	c := NewCollector(&baseRuntime{}, nil)
	ch := make(chan *prometheus.Desc, 16)
	c.Describe(ch)
	close(ch)

	seen := make(map[string]bool)
	for desc := range ch {
		if desc == nil {
			t.Fatal("Describe 发送了 nil Desc")
		}
		if seen[desc.String()] {
			t.Fatalf("Describe 重复发送 Desc: %s", desc.String())
		}
		seen[desc.String()] = true
	}
	if len(seen) != 5 {
		t.Fatalf("Desc 数量 = %d, want 5", len(seen))
	}
}

// sampleKey 是指标名 + 规范化 label 集合的唯一键。
type sampleKey struct {
	name   string
	labels string
}

func newSampleKey(name string, labels map[string]string) sampleKey {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, key := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteString(labels[key])
	}
	return sampleKey{name: name, labels: b.String()}
}

// collectSamples 只 Gather 一次并把所有 gauge/counter 样本展平成 map，
// 避免断言过程中重复 Gather 导致 collect_errors 计数被多次累加。
func collectSamples(t *testing.T, g prometheus.Gatherer) map[sampleKey]float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	samples := make(map[sampleKey]float64)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string, len(metric.GetLabel()))
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			key := newSampleKey(family.GetName(), labels)
			if counter := metric.GetCounter(); counter != nil {
				samples[key] = counter.GetValue()
				continue
			}
			if gauge := metric.GetGauge(); gauge != nil {
				samples[key] = gauge.GetValue()
				continue
			}
			t.Fatalf("指标 %s 既不是 counter 也不是 gauge", family.GetName())
		}
	}
	return samples
}

func assertSample(t *testing.T, samples map[sampleKey]float64, name string, labels map[string]string, want float64) {
	t.Helper()
	got, ok := samples[newSampleKey(name, labels)]
	if !ok {
		t.Fatalf("未找到指标 %s%v", name, labels)
	}
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("指标 %s%v = %v, want %v", name, labels, got, want)
	}
}

func assertNoSample(t *testing.T, samples map[sampleKey]float64, name string, labels map[string]string) {
	t.Helper()
	if got, ok := samples[newSampleKey(name, labels)]; ok {
		t.Fatalf("不应找到指标 %s%v，实际值 = %v", name, labels, got)
	}
}
