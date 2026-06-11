package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	"time"

	"github.com/hibiken/asynq"
)

type PaymentProcessor interface {
	ProcessPayment(ctx context.Context, paymentID string) error
}

type paymentTask struct {
	PaymentID string `json:"payment_id"`
}

type Server struct {
	srv *asynq.Server
	mux *asynq.ServeMux
}

func PaymentRetryDelay(n int, err error, task *asynq.Task) time.Duration {
	baseDelay := 15 * time.Second
	maxDelay := 8 * time.Hour

	exponent := min(n, 20)
	delay := min(maxDelay, baseDelay*time.Duration(1<<exponent))

	jitterRange := delay / 10
	if jitterRange > 0 {
		randomJitter := rand.Int63n(int64(jitterRange * 2))
		delay = (delay - jitterRange) + time.Duration(randomJitter)
	}

	return delay
}

func NewServer(redisOpt asynq.RedisClientOpt, processor PaymentProcessor, concurrency int) *Server {
	mux := asynq.NewServeMux()
	mux.HandleFunc("payment:process", HandlePayment(processor))

	srv := asynq.NewServer(redisOpt, asynq.Config{
		Concurrency: concurrency,
		Queues: map[string]int{
			"critical": 6,
			"default":  3,
			"low":      1,
		},
		RetryDelayFunc: PaymentRetryDelay,
	})

	return &Server{srv: srv, mux: mux}
}

func HandlePayment(processor PaymentProcessor) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		var payload paymentTask
		if err := json.Unmarshal(t.Payload(), &payload); err != nil {
			slog.Error("failed to decode payment task payload", "error", err)
			return err
		}
		slog.Info("worker picked payment", "payment_id", payload.PaymentID)
		return processor.ProcessPayment(ctx, payload.PaymentID)
	}
}

func (s *Server) Run() error {
	return s.srv.Run(s.mux)
}

func (s *Server) Shutdown() {
	s.srv.Shutdown()
}
