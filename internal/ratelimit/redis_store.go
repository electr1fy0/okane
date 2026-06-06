package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisLimiterStore struct {
	rdb *redis.Client
}

func NewRedisLimiterStore(rdb *redis.Client) *RedisLimiterStore {
	return &RedisLimiterStore{rdb}
}

func (s *RedisLimiterStore) Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error) {
	now := time.Now()
	nowMs := now.UnixMilli()
	clearBefore := now.Add(-window).UnixMilli()

	redisKey := fmt.Sprintf("ratelimit:%s", key)

	member := fmt.Sprintf("%d:%d", nowMs, now.UnixNano())

	pipe := s.rdb.Pipeline()

	pipe.ZRemRangeByScore(ctx, redisKey, "-inf", strconv.FormatInt(clearBefore, 10))

	pipe.ZAdd(ctx, redisKey, redis.Z{
		Score:  float64(nowMs),
		Member: member,
	})

	zCard := pipe.ZCard(ctx, redisKey)
	pipe.Expire(ctx, redisKey, window+(1*time.Second))

	_, err := pipe.Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to execute sliding window pipeline: %w", err)
	}

	count, err := zCard.Result()
	if err != nil {
		return false, fmt.Errorf("failed to read window capacity: %w")
	}

	return count <= int64(limit), nil
}
