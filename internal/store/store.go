package store

import (
	"context"

	"github.com/electr1fy0/okane/internal/payment"
)

type Store interface {
	CreatePayment(ctx context.Context, params payment.CreatePaymentParams) (*payment.Payment, bool, error)
	GetPaymentByID(ctx context.Context, id string) (payment.Payment, error)
	GetPaymentByIdempotencyKey(ctx context.Context, key string) (payment.Payment, error)
	UpdatePayment(ctx context.Context, id string, fromStatus, toStatus payment.Status, lastError string, incrementAttempts bool) (payment.Payment, error)
	UpdateProviderRef(ctx context.Context, id, providerRef string) error
	RecordProcessingFailure(ctx context.Context, id, lastError string, incrementAttempts bool) error
}
