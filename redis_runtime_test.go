package eventbus

import (
	"context"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisRuntime_PublishConsumeAckRetryDLQ(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rt := NewRedisRuntime(client, RedisRuntimeConfig{Namespace: "testbus", ConsumerName: "c1"})
	ctx := context.Background()

	msg := Message{Topic: "order.created", Payload: []byte(`{"id":1}`), PublishedAt: time.Now()}
	require.NoError(t, rt.Publish(ctx, msg))

	require.NoError(t, rt.EnsureConsumerGroup(ctx, "order.created", "g1"))
	items, err := rt.Consume(ctx, ConsumerConfig{Topic: "order.created", Group: "g1", PollBatchSize: 1, PollBlock: 10 * time.Millisecond})
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.NoError(t, rt.Ack(ctx, items[0]))

	pending, err := client.XPending(ctx, "testbus:topic:order.created", "g1").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)

	task := RetryTask{
		Topic:       "order.created",
		Group:       "g1",
		Payload:     []byte(`{"id":1}`),
		Attempt:     1,
		MaxRetries:  3,
		AvailableAt: time.Now().Add(-time.Second),
	}
	require.NoError(t, rt.ScheduleRetry(ctx, task))

	retries, err := rt.PollRetry(ctx, 10, time.Now())
	require.NoError(t, err)
	require.Len(t, retries, 1)
	require.NoError(t, rt.RemoveRetry(ctx, retries[0]))

	dlq := DeadLetterMessage{
		Topic:      "order.created",
		Group:      "g1",
		Payload:    []byte(`{"id":1}`),
		Attempt:    3,
		MaxRetries: 3,
		Error:      "boom",
		OccurredAt: time.Now(),
	}
	require.NoError(t, rt.MoveToDeadLetter(ctx, dlq))

	length, err := client.LLen(ctx, "testbus:dlq").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), length)
}

