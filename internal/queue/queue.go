package queue

import (
	"context"
	"time"
)

type DelayedJob struct {
	PaymentID string
	RunAt     time.Time
}

type Queue interface {
	EnqueuePending(ctx context.Context, paymentID string) error
	MovePendingToProcessing(ctx context.Context, timeout time.Duration) (string, error)
	MarkProcessing(ctx context.Context, paymentID string, startedAt time.Time) error
	RemoveProcessing(ctx context.Context, paymentID string) error
	ListProcessing(ctx context.Context, limit int64) ([]string, error)
	GetProcessingTime(ctx context.Context, paymentID string) (time.Time, bool, error)
	EnqueueDelayed(ctx context.Context, paymentID string, runAt time.Time) error
	NextDelayed(ctx context.Context) (*DelayedJob, error)
	RemoveDelayed(ctx context.Context, paymentID string) error
	EnqueueDead(ctx context.Context, paymentID string) error
}
