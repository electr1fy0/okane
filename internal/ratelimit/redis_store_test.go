package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestRedisLimiterStore(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err, "failed to start miniredis")

	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	defer rdb.Close()

	store := NewRedisLimiterStore(rdb)
	ctx := context.Background()
	key := "test-client-ip"
	limit := 3
	window := 10 * time.Second

	t.Run("allows requests within limit", func(t *testing.T) {
		mr.FlushAll()

		allowed, err := store.Allow(ctx, key, limit, window)

		assert.NoError(t, err)
		assert.True(t, allowed)

		allowed, err = store.Allow(ctx, key, limit, window)
		assert.NoError(t, err)
		assert.True(t, allowed)

		allowed, err = store.Allow(ctx, key, limit, window)
		assert.NoError(t, err)
		assert.True(t, allowed)

	})
	t.Run("rejects requests exceeding limit", func(t *testing.T) {
		mr.FlushAll()

		for range limit {
			allowed, err := store.Allow(ctx, key, limit, window)
			assert.NoError(t, err)
			assert.True(t, allowed)
		}

		allowed, err := store.Allow(ctx, key, limit, window)
		assert.NoError(t, err)
		assert.False(t, allowed)
	})

	t.Run("accepts requests again after window expires", func(t *testing.T) {
		mr.FlushAll()
		for range limit {
			_, _ = store.Allow(ctx, key, limit, window)
		}
		mr.FastForward(window + 1*time.Second)

		allowed, err := store.Allow(ctx, key, limit, window)

		assert.NoError(t, err)
		assert.True(t, allowed)
	})
}

func BenchmarkRedisLimiterStore_Allow(b *testing.B) {
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer rdb.Close()

	store := NewRedisLimiterStore(rdb)
	ctx := context.Background()
	key := "benchmark-key"
	limit := 1000000
	window := 1 * time.Minute

	b.ResetTimer()
	for b.Loop() {
		_, err := store.Allow(ctx, key, limit, window)
		if err != nil {
			b.Fatalf("Allow failed: %v", err)
		}
	}
}
