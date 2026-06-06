package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestRateLimiterMiddleware(t *testing.T) {
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)

		_, _ = w.Write([]byte("success"))
	})

	t.Run("allows request when under limit", func(t *testing.T) {
		mockStore := NewMockLimiterStore(t)
		mockStore.On("Allow", mock.Anything, mock.Anything, 5, 1*time.Minute).Return(true, nil)

		limiter := NewRateLimiter(mockStore, 5, 1*time.Minute)

		handlerToTest := limiter.Middleware(nextHandler)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rr := httptest.NewRecorder()

		handlerToTest.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, "success", rr.Body.String())
	})
	t.Run("rejects requests with 429 when lim exceeded", func(t *testing.T) {
		mockStore := NewMockLimiterStore(t)
		mockStore.On("Allow", mock.Anything, mock.Anything, 5, 1*time.Minute).Return(false, nil)

		limiter := NewRateLimiter(mockStore, 5, 1*time.Minute)
		handlerToTest := limiter.Middleware(nextHandler)

		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)

		handlerToTest.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusTooManyRequests, rr.Code)
		assert.Equal(t, "Too Many Requests\n", rr.Body.String())
		assert.Equal(t, "1m0s", rr.Header().Get("Retry-After"))
	})
}
