package prometheus

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/modern-magic-go/eventbus"
)

func TestObserverCounters(t *testing.T) {
	reg := prometheus.NewRegistry()
	obs := NewObserver(reg)
	ctx := context.Background()

	obs.OnPublished(ctx, eventbus.Message{Topic: "orders"})
	obs.OnPublished(ctx, eventbus.Message{Topic: "orders"})
	obs.OnPublishFailed(ctx, eventbus.Message{Topic: "orders"}, errors.New("boom"))
	obs.OnPublishFailed(ctx, eventbus.Message{}, errors.New("empty topic"))
	obs.OnHandled(ctx, eventbus.Message{Topic: "orders"})
	obs.OnHandleFailed(ctx, eventbus.Message{Topic: "orders"}, errors.New("boom"))
	obs.OnRetryScheduled(ctx, eventbus.RetryTask{Topic: "orders"}, nil)
	obs.OnDeadLettered(ctx, eventbus.DeadLetterMessage{Topic: "orders"})
	obs.OnError(ctx, eventbus.ErrorEvent{Op: "consume"})
	obs.OnError(ctx, eventbus.ErrorEvent{Op: "consume"})
	obs.OnError(ctx, eventbus.ErrorEvent{Op: "ack"})
	obs.OnDuplicate(ctx, eventbus.Message{Topic: "orders"})

	tests := []struct {
		name    string
		counter prometheus.Counter
		want    float64
	}{
		{"published success", obs.published.WithLabelValues("orders", statusSuccess), 2},
		{"published failed", obs.published.WithLabelValues("orders", statusFailed), 1},
		{"published empty topic", obs.published.WithLabelValues("", statusFailed), 1},
		{"handled success", obs.handled.WithLabelValues("orders", statusSuccess), 1},
		{"handled failed", obs.handled.WithLabelValues("orders", statusFailed), 1},
		{"retry scheduled", obs.retryScheduled.WithLabelValues("orders"), 1},
		{"dead lettered", obs.deadLettered.WithLabelValues("orders"), 1},
		{"errors consume", obs.errors.WithLabelValues("consume"), 2},
		{"errors ack", obs.errors.WithLabelValues("ack"), 1},
		{"duplicates", obs.duplicates.WithLabelValues("orders"), 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := testutil.ToFloat64(tt.counter); got != tt.want {
				t.Fatalf("counter = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestObserverImplementsOptionalInterfaces(t *testing.T) {
	var obs eventbus.Observer = NewObserver(prometheus.NewRegistry())
	if _, ok := obs.(eventbus.ErrorObserver); !ok {
		t.Fatal("Observer 未实现 eventbus.ErrorObserver")
	}
	if _, ok := obs.(eventbus.DuplicateObserver); !ok {
		t.Fatal("Observer 未实现 eventbus.DuplicateObserver")
	}
}

func TestNewObserverDuplicateRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	first := NewObserver(reg)
	second := NewObserver(reg) // 不应 panic，应复用已注册的 CounterVec

	second.OnPublished(context.Background(), eventbus.Message{Topic: "orders"})

	if got := testutil.ToFloat64(first.published.WithLabelValues("orders", statusSuccess)); got != 1 {
		t.Fatalf("第一个 Observer 的计数器 = %v, want 1", got)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var count int
	for _, family := range families {
		if family.GetName() == "eventbus_published_total" {
			count += len(family.GetMetric())
		}
	}
	if count != 1 {
		t.Fatalf("eventbus_published_total 样本数 = %d, want 1（重复创建不应产生重复指标）", count)
	}
}

func TestObserverWithConstLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	obs := NewObserver(reg, WithConstLabels(prometheus.Labels{"service": "orders"}))
	obs.OnHandled(context.Background(), eventbus.Message{Topic: "orders"})

	samples := collectSamples(t, reg)
	assertSample(t, samples, "eventbus_handled_total", map[string]string{
		"topic":   "orders",
		"status":  statusSuccess,
		"service": "orders",
	}, 1)
}

func TestNewObserverNilRegisterer(t *testing.T) {
	// nil 表示使用默认 registry；测试结束前注销，避免污染其它用例。
	obs := NewObserver(nil)
	obs.OnPublished(context.Background(), eventbus.Message{Topic: "orders"})
	t.Cleanup(func() {
		prometheus.Unregister(obs.published)
		prometheus.Unregister(obs.handled)
		prometheus.Unregister(obs.retryScheduled)
		prometheus.Unregister(obs.deadLettered)
		prometheus.Unregister(obs.errors)
		prometheus.Unregister(obs.duplicates)
	})

	if got := testutil.ToFloat64(obs.published.WithLabelValues("orders", statusSuccess)); got != 1 {
		t.Fatalf("默认 registry 上的计数器 = %v, want 1", got)
	}
}
