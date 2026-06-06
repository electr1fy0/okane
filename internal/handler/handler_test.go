package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/electr1fy0/okane/internal/store"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

func TestCreatePaymentAcceptedAndEnqueued(t *testing.T) {
	payment := testPayment()
	mockSvc := NewMockPaymentService(t)

	mockSvc.On("CreatePayment", mock.Anything, store.CreatePaymentParams{
		Amount:         440,
		Status:         "pending",
		IdempotencyKey: "demo-key-1",
	}).Return(&payment, true, nil)

	mockSvc.On("EnqueuePayment", mock.Anything, payment.ID.String()).Return(nil)

	handler := &APIHandler{svc: mockSvc}

	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(`{"amount":440,"idempotency_key":"demo-key-1"}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	Handle(handler.CreatePayment)(rr, req)
	assert.Equal(t, http.StatusAccepted, rr.Code, "body=%s", rr.Body.String())

	var resp CreatePaymentResponse
	err := json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.True(t, resp.Created)
	assert.True(t, resp.Enqueued)
	assert.Equal(t, payment.ID, resp.Payment.ID)
}

func TestCreatePaymentRejectsInvalidJSON(t *testing.T) {
	mockSvc := NewMockPaymentService(t)
	handler := &APIHandler{svc: mockSvc}
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(`{"amount":`))
	rr := httptest.NewRecorder()

	Handle(handler.CreatePayment)(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code, "body=%s", rr.Body.String())

	req = httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(`{"amount": 0, "idempotency_key":"key"}`))
	rr = httptest.NewRecorder()

	Handle(handler.CreatePayment)(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestCreatePaymentRejectsDuplicateIdempotencyKey(t *testing.T) {
	payment := testPayment()
	mockSvc := NewMockPaymentService(t)

	mockSvc.On("CreatePayment", mock.Anything, store.CreatePaymentParams{
		Amount:         440,
		Status:         "pending",
		IdempotencyKey: "demo-key-1",
	}).Return(&payment, false, nil)

	handler := &APIHandler{svc: mockSvc}

	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(`{"amount": 440, "idempotency_key": "demo-key-1"}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	Handle(handler.CreatePayment)(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
}

func TestGetPaymentByIDReturnsPayment(t *testing.T) {
	payment := testPayment()
	mockSvc := NewMockPaymentService(t)

	mockSvc.On("GetPaymentByID", mock.Anything, payment.ID.String()).Return(payment, nil)

	handler := &APIHandler{svc: mockSvc}

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
