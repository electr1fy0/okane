package payment

import (
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending         Status = "pending"
	StatusProcessing      Status = "processing"
	StatusSuccess         Status = "success"
	StatusFailedRetryable Status = "failed_retryable"
	StatusFailedFinal     Status = "failed_final"
)

func (s Status) String() string { return string(s) }

type Payment struct {
	ID             uuid.UUID `json:"id"`
	Amount         int64     `json:"amount"`
	Status         Status    `json:"status"`
	IdempotencyKey string    `json:"idempotency_key"`
	ProviderRef    *string   `json:"provider_ref,omitempty"`
	Attempts       int       `json:"attempts"`
	LastError      *string   `json:"last_error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type CreatePaymentParams struct {
	Amount         int64  `json:"amount"`
	Status         Status `json:"status"`
	IdempotencyKey string `json:"idempotency_key"`
}
