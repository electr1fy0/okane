package queue

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestQueue(t *testing.T) (*RedisQueue, func()) {
	mr, err := miniredis.Run()

	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	q := NewRedis(client)

	teardown := func() {
		client.Close()
		mr.Close()
	}

	return q, teardown
}

func TestRedisQueuePendingWorkflow(t *testing.T) {
	q, teardown := setupTestQueue(t)

	defer teardown()

	ctx := context.Background()
	paymentID := "pay-123"

	err := q.Ping(ctx)

	assert.NoError(t, err)

	err = q.EnqueuePending(ctx, paymentID)
	assert.NoError(t, err)

	err = q.EnqueuePending(ctx, paymentID)
	assert.NoError(t, err)

	movedID, err := q.MovePendingToProcessing(ctx, 100*time.Millisecond)

	assert.NoError(t, err)
	assert.Equal(t, paymentID, movedID)
	processing, err := q.ListProcessing(ctx, 10)
	assert.NoError(t, err)

	assert.Contains(t, processing, paymentID)

}

func TestRedisQueue_DelayedJobs(t *testing.T) {
	q, teardown := setupTestQueue(t)

	defer teardown()

	ctx := context.Background()

	paymentID := "pay-delayed"

	runAt := time.Now().Add(1 * time.Hour).Truncate(time.Second)

	err := q.EnqueueDelayed(ctx, paymentID, runAt)
	assert.NoError(t, err)

	job, err := q.NextDelayed(ctx)

	assert.NoError(t, err)

	require.NotNil(t, job)
	assert.Equal(t, paymentID, job.PaymentID)
	assert.Equal(t, runAt.Unix(), job.RunAt.Unix())

	err = q.RemoveDelayed(ctx, paymentID)
	assert.NoError(t, err)

	job, err = q.NextDelayed(ctx)
	assert.NoError(t, err)
	assert.Nil(t, job)
}
