package queue

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	pendingKey         = "payments:pending"
	processingKey      = "payments:processing"
	processingTimesKey = "payments:processing:times"
	delayedKey         = "payments:delayed"
	deadKey            = "payments:dead"
)

type RedisQueue struct {
	client *redis.Client
}

func NewRedis(client *redis.Client) *RedisQueue {
	return &RedisQueue{client: client}
}

func (q *RedisQueue) EnqueuePending(ctx context.Context, paymentID string) error {
	return q.client.LPush(ctx, pendingKey, paymentID).Err()
}

func (q *RedisQueue) MovePendingToProcessing(ctx context.Context, timeout time.Duration) (string, error) {
	paymentID, err := q.client.BLMove(ctx, pendingKey, processingKey, "RIGHT", "LEFT", timeout).Result()
	if err == redis.Nil {
		return "", nil
	}
	return paymentID, err
}

func (q *RedisQueue) MarkProcessing(ctx context.Context, paymentID string, startedAt time.Time) error {
	return q.client.HSet(ctx, processingTimesKey, paymentID, startedAt.Unix()).Err()
}

func (q *RedisQueue) RemoveProcessing(ctx context.Context, paymentID string) error {
	if err := q.client.LRem(ctx, processingKey, 1, paymentID).Err(); err != nil {
		return err
	}
	return q.client.HDel(ctx, processingTimesKey, paymentID).Err()
}

func (q *RedisQueue) ListProcessing(ctx context.Context, limit int64) ([]string, error) {
	return q.client.LRange(ctx, processingKey, 0, limit-1).Result()
}

func (q *RedisQueue) GetProcessingTime(ctx context.Context, paymentID string) (time.Time, bool, error) {
	ts, err := q.client.HGet(ctx, processingTimesKey, paymentID).Result()
	if err == redis.Nil {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}

	unixTs, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return time.Time{}, false, err
	}

	return time.Unix(unixTs, 0), true, nil
}

func (q *RedisQueue) EnqueueDelayed(ctx context.Context, paymentID string, runAt time.Time) error {
	return q.client.ZAdd(ctx, delayedKey, redis.Z{
		Score:  float64(runAt.Unix()),
		Member: paymentID,
	}).Err()
}

func (q *RedisQueue) NextDelayed(ctx context.Context) (*DelayedJob, error) {
	jobs, err := q.client.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key:     delayedKey,
		ByScore: true,
		Start:   "-inf",
		Stop:    "+inf",
		Offset:  0,
		Count:   1,
	}).Result()
	if err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return nil, nil
	}

	paymentID, ok := jobs[0].Member.(string)
	if !ok {
		return &DelayedJob{}, nil
	}

	return &DelayedJob{
		PaymentID: paymentID,
		RunAt:     time.Unix(int64(jobs[0].Score), 0),
	}, nil
}

func (q *RedisQueue) RemoveDelayed(ctx context.Context, paymentID string) error {
	return q.client.ZRem(ctx, delayedKey, paymentID).Err()
}

func (q *RedisQueue) EnqueueDead(ctx context.Context, paymentID string) error {
	return q.client.LPush(ctx, deadKey, paymentID).Err()
}
