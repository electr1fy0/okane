package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/electr1fy0/okane/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testPayment() store.Payment {
	now := time.Unix(1710000000, 0).UTC()
	return store.Payment{
		ID:             uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Amount:         440,
		Status:         "pending",
		IdempotencyKey: "demo-key-1",
		Attempts:       0,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

type fakePaymentAPI struct {
	createPaymentFn  func(ctx context.Context, params store.CreatePaymentParams) (*store.Payment, bool, error)
	getPaymentByIDFn func(ctx context.Context, id string) (store.Payment, error)
	enqueuePaymentFn func(ctx context.Context, paymentID string) error
}

func (f fakePaymentAPI) CreatePayment(ctx context.Context, params store.CreatePaymentParams) (*store.Payment, bool, error) {
	if f.createPaymentFn == nil {
		return nil, false, errors.New("unexpected CreatePayment call")
	}
	return f.createPaymentFn(ctx, params)
}

func (f fakePaymentAPI) GetPaymentByID(ctx context.Context, id string) (store.Payment, error) {
	if f.getPaymentByIDFn == nil {
		return store.Payment{}, errors.New("unexpected GetPaymentByID call")
	}
	return f.getPaymentByIDFn(ctx, id)
}

func (f fakePaymentAPI) EnqueuePayment(ctx context.Context, paymentID string) error {
	if f.enqueuePaymentFn == nil {
		return nil
	}
	return f.enqueuePaymentFn(ctx, paymentID)
}

func TestCreatePaymentAcceptedAndEnqueued(t *testing.T) {
	payment := testPayment()

	var gotParams store.CreatePaymentParams
	var enqueuedID string

	handler := &APIHandler{
		svc: fakePaymentAPI{
			createPaymentFn: func(ctx context.Context, params store.CreatePaymentParams) (*store.Payment, bool, error) {
				gotParams = params
				return &payment, true, nil
			},
			enqueuePaymentFn: func(ctx context.Context, paymentID string) error {
				enqueuedID = paymentID
				return nil
			},
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(`{"amount":440,"idempotency_key":"demo-key-1"}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler.CreatePayment(rr, req)
	assert.Equal(t, http.StatusAccepted, rr.Code, "body=%s", rr.Body.String())

	assert.Equal(t, int64(440), gotParams.Amount)
	assert.Equal(t, "demo-key-1", gotParams.IdempotencyKey)
	assert.Equal(t, payment.ID.String(), enqueuedID)

	var resp CreatePaymentResponse
	err := json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.True(t, resp.Created)
	assert.True(t, resp.Enqueued)
	assert.Equal(t, payment.ID, resp.Payment.ID)
}

func TestCreatePaymentRejectsInvalidJSON(t *testing.T) {
	handler := &APIHandler{fakePaymentAPI{}}
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(`{"amount":`))
	rr := httptest.NewRecorder()

	handler.CreatePayment(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code, "body=%s", rr.Body.String())

	req = httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(`{"amount": 0, "idempotency_key":"key"}`))

	handler.CreatePayment(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestCreatePaymentRejectsDuplicateIdempotencyKey(t *testing.T) {
	handler := &APIHandler{
		svc: fakePaymentAPI{
			createPaymentFn: func(ctx context.Context, params store.CreatePaymentParams) (*store.Payment, bool, error) {
				return nil, false, &pgconn.PgError{Code: "23505"}
			},
			enqueuePaymentFn: func(ctx context.Context, paymentID string) error {
				assert.Fail(t, "enqueue should not be called for duplicate idempotency key")
				return nil
			},
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(`{"amount": 440, "idempotency_key": "demo-key-1"}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler.CreatePayment(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
}

func TestGetPaymentByIDReturnsPayment(t *testing.T) {
	payment := testPayment()

	handler := &APIHandler{
		svc: fakePaymentAPI{
			getPaymentByIDFn: func(ctx context.Context, id string) (store.Payment, error) {
				assert.Equal(t, payment.ID.String(), id)
				return payment, nil
			},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/payments/"+payment.ID.String(), nil)
	req.SetPathValue("id", payment.ID.String())
	rr := httptest.NewRecorder()

	handler.GetPaymentID(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())

	var resp GetPaymentResponse
	err := json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Equal(t, payment.ID, resp.Payment.ID)
}
