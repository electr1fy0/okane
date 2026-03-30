package store

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Payment struct {
	ID             uuid.UUID `json:"id"`
	Amount         int64     `json:"amount"`
	Status         string    `json:"status"`
	IdempotencyKey string    `json:"idempotency_key"`
	ProviderRef    *string   `json:"provider_ref,omitempty"`
	Attempts       int       `json:"attempts"`
	LastError      *string   `json:"last_error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type CreatePaymentParams struct {
	Amount         int64  `json:"amount"`
	Status         string `json:"status"`
	IdempotencyKey string `json:"idempotency_key"`
}

type Store interface {
	CreatePayment(ctx context.Context, params CreatePaymentParams) (*Payment, bool, error)
	GetPaymentByID(ctx context.Context, id string) (Payment, error)
	GetPaymentByIdempotencyKey(ctx context.Context, key string) (Payment, error)
	UpdatePayment(ctx context.Context, id, fromStatus, toStatus, lastError string, incrementAttempts bool) (Payment, error)
	UpdateProviderRef(ctx context.Context, id, providerRef string) error
	RecordProcessingFailure(ctx context.Context, id, lastError string, incrementAttempts bool) error
}
