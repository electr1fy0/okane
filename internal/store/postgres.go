package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/electr1fy0/okane/internal/types"
	"github.com/electr1fy0/okane/internal/utils"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var errInvalidStateTransition = errors.New("invalid state transition")

type PostgresStore struct {
	db *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{db: pool}
}

func (s *PostgresStore) RecordProcessingFailure(ctx context.Context, id, lastError string, incrementAttempts bool) error {
	query := `
		update payments
		set last_error = $1,
			attempts = attempts + $2,
			updated_at = now()
		where id = $3
		  and status = $4`

	attemptDelta := 0
	if incrementAttempts {
		attemptDelta = 1
	}

	tag, err := s.db.Exec(ctx, query, utils.NullableText(lastError), attemptDelta, id, types.PaymentStatusProcessing)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: expected %s for payment %s", errInvalidStateTransition, types.PaymentStatusProcessing, id)
	}

	return nil
}

func (s *PostgresStore) UpdatePayment(ctx context.Context, id, fromStatus, toStatus, lastError string, incrementAttempts bool) error {
	query := `
		update payments
		set status = $1,
			last_error = $2,
			attempts = attempts + $3,
			updated_at = now()
		where id = $4
		  and status = $5`

	attemptDelta := 0
	if incrementAttempts {
		attemptDelta = 1
	}

	tag, err := s.db.Exec(ctx, query, toStatus, utils.NullableText(lastError), attemptDelta, id, fromStatus)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s -> %s for payment %s", errInvalidStateTransition, fromStatus, toStatus, id)
	}

	return nil
}

func (s *PostgresStore) UpdateProviderRef(ctx context.Context, id, providerRef string) error {
	query := `
		update payments
		set provider_ref = $1,
			updated_at = now()
		where id = $2
		  and status = $3`

	tag, err := s.db.Exec(ctx, query, providerRef, id, types.PaymentStatusSuccess)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: expected %s before provider ref update for payment %s", errInvalidStateTransition, types.PaymentStatusSuccess, id)
	}

	return nil
}

func (s *PostgresStore) GetPaymentByID(ctx context.Context, id string) (Payment, error) {
	query := `
	select id, amount, status, idempotency_key, provider_ref, attempts, last_error, created_at, updated_at from payments where id = $1;
	`
	payment := Payment{}

	err := s.db.QueryRow(ctx, query, id).Scan(
		&payment.ID,
		&payment.Amount,
		&payment.Status,
		&payment.IdempotencyKey,
		&payment.ProviderRef,
		&payment.Attempts,
		&payment.LastError,
		&payment.CreatedAt,
		&payment.UpdatedAt,
	)
	return payment, err
}

func (s *PostgresStore) GetPaymentByIdempotencyKey(ctx context.Context, key string) (Payment, error) {
	query := `
	select id, amount, status, idempotency_key, provider_ref, attempts, last_error, created_at, updated_at
	from payments where idempotency_key = $1
	limit 1`

	payment := Payment{}
	err := s.db.QueryRow(ctx, query, key).Scan(
		&payment.ID,
		&payment.Amount,
		&payment.Status,
		&payment.IdempotencyKey,
		&payment.ProviderRef,
		&payment.Attempts,
		&payment.LastError,
		&payment.CreatedAt,
		&payment.UpdatedAt,
	)
	return payment, err
}

func (s *PostgresStore) CreatePayment(ctx context.Context, params CreatePaymentParams) (*Payment, bool, error) {
	query := `
		insert into payments (id, amount, status, idempotency_key)
		values ($1, $2, $3, $4)
		returning id, amount, status, idempotency_key, created_at, updated_at;
	`
	payment := &Payment{}
	err := s.db.QueryRow(ctx, query,
		uuid.New(),
		params.Amount,
		params.Status,
		params.IdempotencyKey,
	).Scan(
		&payment.ID,
		&payment.Amount,
		&payment.Status,
		&payment.IdempotencyKey,
		&payment.CreatedAt,
		&payment.UpdatedAt,
	)

	if err != nil {
		var pgError *pgconn.PgError
		if errors.As(err, &pgError) && pgError.Code == "23505" {
			existingPayment, lookupErr := s.GetPaymentByIdempotencyKey(ctx, params.IdempotencyKey)
			if lookupErr != nil {
				return nil, false, lookupErr
			}
			return &existingPayment, false, nil
		}
		return nil, false, err
	}

	return payment, true, nil
}
