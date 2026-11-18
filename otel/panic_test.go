package otel

import (
	"context"
	"testing"

	eventbus "github.com/modern-magic-go/eventbus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// handler panic 必须记录到 consumer span（RecordError + Error status），并继续向上抛出，
// 交给 Bus 的 recover 兜底；不能因为 span 未记录而丢失排障信息。
func TestWrapHandlerPanicRecordedInSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	handler := WrapHandler(func(context.Context, eventbus.Message) error {
		panic("boom in handler")
	}, WithTracerProvider(tp))

	require.Panics(t, func() {
		_ = handler(context.Background(), eventbus.Message{Topic: "order.created"})
	})

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	span := spans[0]
	assert.Equal(t, sdktrace.Status{Code: 1, Description: "handler panic"}, span.Status())
	assert.NotEmpty(t, span.Events(), "panic 应作为 span event 记录")
}
