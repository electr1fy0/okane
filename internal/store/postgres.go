package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/electr1fy0/okane/internal/payment"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

func scanPayment(row interface{ Scan(...any) error }) (payment.Payment, error) {
	var p payment.Payment
	var status string
	err := row.Scan(
		&p.ID,
		&p.Amount,
		&status,
		&p.IdempotencyKey,
		&p.ProviderRef,
		&p.Attempts,
		&p.LastError,
		&p.CreatedAt,
		&p.UpdatedAt,
	)
	p.Status = payment.Status(status)
	return p, err
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

	tag, err := s.db.Exec(ctx, query, nullableText(lastError), attemptDelta, id, payment.StatusProcessing)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: expected %s for payment %s", errInvalidStateTransition, payment.StatusProcessing, id)
	}

	return nil
}

func (s *PostgresStore) UpdatePayment(ctx context.Context, id string, fromStatus, toStatus payment.Status, lastError string, incrementAttempts bool) (payment.Payment, error) {
	query := `
		update payments
		set status = $1,
			last_error = $2,
			attempts = attempts + $3,
			updated_at = now()
		where id = $4
		  and status = $5
		returning id, amount, status, idempotency_key, provider_ref, attempts, last_error, created_at, updated_at`

	attemptDelta := 0
	if incrementAttempts {
		attemptDelta = 1
	}

	p, err := scanPayment(s.db.QueryRow(ctx, query, toStatus.String(), nullableText(lastError), attemptDelta, id, fromStatus.String()))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return payment.Payment{}, fmt.Errorf("%w: %s -> %s for payment %s", errInvalidStateTransition, fromStatus, toStatus, id)
		}
		return payment.Payment{}, err
	}

	return p, nil
}

func (s *PostgresStore) UpdateProviderRef(ctx context.Context, id, providerRef string) error {
	query := `
		update payments
		set provider_ref = $1,
			updated_at = now()
		where id = $2
		  and status = $3`

	tag, err := s.db.Exec(ctx, query, providerRef, id, payment.StatusSuccess)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: expected %s before provider ref update for payment %s", errInvalidStateTransition, payment.StatusSuccess, id)
	}

	return nil
}

func (s *PostgresStore) GetPaymentByID(ctx context.Context, id string) (payment.Payment, error) {
	query := `
	select id, amount, status, idempotency_key, provider_ref, attempts, last_error, created_at, updated_at from payments where id = $1;
	`
	return scanPayment(s.db.QueryRow(ctx, query, id))
}

func (s *PostgresStore) GetPaymentByIdempotencyKey(ctx context.Context, key string) (payment.Payment, error) {
	query := `
	select id, amount, status, idempotency_key, provider_ref, attempts, last_error, created_at, updated_at
	from payments where idempotency_key = $1
	limit 1`

	return scanPayment(s.db.QueryRow(ctx, query, key))
}

func (s *PostgresStore) CreatePayment(ctx context.Context, params payment.CreatePaymentParams) (*payment.Payment, bool, error) {
	query := `
		insert into payments (id, amount, status, idempotency_key)
		values ($1, $2, $3, $4)
		returning id, amount, status, idempotency_key, created_at, updated_at;
	`

	var p payment.Payment
	var status string
	err := s.db.QueryRow(ctx, query,
		uuid.New(),
		params.Amount,
		params.Status.String(),
		params.IdempotencyKey,
	).Scan(
		&p.ID,
		&p.Amount,
		&status,
		&p.IdempotencyKey,
		&p.CreatedAt,
		&p.UpdatedAt,
	)
	p.Status = payment.Status(status)

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

	return &p, true, nil
}

func nullableText(value string) any {
	if value == "" {
		return nil
	}
	return value
}
