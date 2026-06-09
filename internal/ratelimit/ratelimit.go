package ratelimit

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

type LimiterStore interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error)
}

type RateLimiter struct {
	store  LimiterStore
	limit  int
	window time.Duration
}

func NewRateLimiter(store LimiterStore, limit int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		store:  store,
		limit:  limit,
		window: window,
	}
}

func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientIP := r.RemoteAddr

		allowed, err := rl.store.Allow(r.Context(), clientIP, rl.limit, rl.window)

		if err != nil {
			slog.Error("rate limiter failed", "error", err)
			// we don't know what happened
			// so we allow the request
			next.ServeHTTP(w, r)
			return
		}

		if !allowed {
			slog.Debug("rate limit exceeded", "client_ip", clientIP, "limit", rl.limit, "window", rl.window)
			w.Header().Set("Retry-After", rl.window.String())
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}

		slog.Debug("rate limit allowed", "client_ip", clientIP)
		next.ServeHTTP(w, r)
	})
}
