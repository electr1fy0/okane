package store

import (
	"context"
	"os"
	"testing"

	"github.com/electr1fy0/okane/internal/payment"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestDB(t *testing.T) (*pgxpool.Pool, func()) {
	ctx := context.Background()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://okanedbuser:okanedbpass@localhost:5432/okanedb"
	}

	pool, err := pgxpool.New(ctx, dbURL)
	require.NoError(t, err)

	// read and apply schema.sql
	schemaBytes, err := os.ReadFile("../../schema.sql")
	require.NoError(t, err, "failed to read schema.sql: %v", err)

	_, err = pool.Exec(ctx, string(schemaBytes))
	require.NoError(t, err, "failed to apply schema: %v", err)

	// clean slate everytime
	_, err = pool.Exec(ctx, "truncate table payments cascade;")
	require.NoError(t, err)

	teardown := func() {
		pool.Close()
	}

	return pool, teardown
}

func TestPostgresStore_CreatePayment(t *testing.T) {
	pool, teardown := setupTestDB(t)
	defer teardown()
	ctx := context.Background()

	s := New(pool)
	params := payment.CreatePaymentParams{
		Amount:         500,
		Status:         payment.StatusPending,
		IdempotencyKey: "test-key-1",
	}

	p, created, err := s.CreatePayment(ctx, params)
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, params.Amount, p.Amount)
	assert.Equal(t, params.IdempotencyKey, p.IdempotencyKey)
	assert.Equal(t, params.Status, p.Status)

	existingPayment, createdAgain, err := s.CreatePayment(ctx, params)
	require.NoError(t, err)
	assert.False(t, createdAgain)
	assert.Equal(t, p.ID, existingPayment.ID)
}

func TestPostgresStore_GetPaymentByID(t *testing.T) {
	pool, teardown := setupTestDB(t)
	defer teardown()
	ctx := context.Background()

	s := New(pool)

	p, _, err := s.CreatePayment(ctx, payment.CreatePaymentParams{
		Amount:         500,
		Status:         payment.StatusPending,
		IdempotencyKey: "test-key-1",
	})
	require.NoError(t, err)

	found, err := s.GetPaymentByID(ctx, p.ID.String())
	require.NoError(t, err)

	assert.Equal(t, p.ID, found.ID)
	assert.Equal(t, int64(500), found.Amount)
}
