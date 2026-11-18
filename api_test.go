package eventbus_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/modern-magic-go/eventbus"
)

// customDedupStore 是外部包实现 eventbus.DedupStore 的示例，
// 用于保证去重状态相关符号（DedupStatus 与三个状态常量）已从根包导出。
type customDedupStore struct{}

func (customDedupStore) Acquire(context.Context, string, time.Duration) (eventbus.DedupStatus, error) {
	return eventbus.DedupAcquired, nil
}

func (customDedupStore) MarkDone(context.Context, string, time.Duration) error { return nil }

func (customDedupStore) Release(context.Context, string) error { return nil }

func TestDedupStatusSymbolsAreExported(t *testing.T) {
	var status eventbus.DedupStatus = eventbus.DedupInProgress
	assert.Equal(t, "in_progress", status.String())

	bus := eventbus.NewBus(nil, eventbus.WithDedup(customDedupStore{}, eventbus.DedupConfig{TTL: time.Minute}))
	require.NotNil(t, bus)
}
