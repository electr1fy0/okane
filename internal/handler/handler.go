package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/electr1fy0/okane/internal/store"
	"github.com/electr1fy0/okane/internal/types"
	"github.com/jackc/pgx/v5"
)

type CreatePaymentResponse struct {
	Payment  store.Payment `json:"payment"`
	Created  bool          `json:"created"`
	Enqueued bool          `json:"enqueued"`
}

type GetPaymentResponse struct {
	Payment store.Payment `json:"payment"`
}

type createPaymentRequest struct {
	Amount         int64  `json:"amount"`
	IdempotencyKey string `json:"idempotency_key"`
}

type PaymentService interface {
	CreatePayment(ctx context.Context, params store.CreatePaymentParams) (*store.Payment, bool, error)
	GetPaymentByID(ctx context.Context, id string) (store.Payment, error)
	EnqueuePayment(ctx context.Context, paymentID string) error
}

type APIHandler struct {
	svc PaymentService
}

func New(svc PaymentService) *APIHandler {
	return &APIHandler{svc: svc}
}

func writeJSON(w http.ResponseWriter, data any, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}

func (h *APIHandler) CreatePayment(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req createPaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Error("failed to decode create payment request", "error", err)
		http.Error(w, "failed to decode the payment", http.StatusBadRequest)
		return
	}

	payment, created, err := h.svc.CreatePayment(ctx, store.CreatePaymentParams{
		Amount:         req.Amount,
		Status:         types.PaymentStatusPending,
		IdempotencyKey: req.IdempotencyKey,
	})

	if err != nil {
		slog.Error("failed to create payment", "idempotency_key", req.IdempotencyKey, "error", err)
		http.Error(w, "failed to create payment", http.StatusInternalServerError)
		return
	}

	if !created {
		writeJSON(w, CreatePaymentResponse{
			Payment:  *payment,
			Created:  false,
			Enqueued: false,
		}, http.StatusOK)
		return
	}

	if err := h.svc.EnqueuePayment(ctx, payment.ID.String()); err != nil {
		slog.Error("failed to enqueue payment", "payment_id", payment.ID, "error", err)
		http.Error(w, "failed to enqueue payment", http.StatusInternalServerError)
		return
	}

	writeJSON(w, CreatePaymentResponse{
		Payment:  *payment,
		Created:  true,
		Enqueued: true,
	}, http.StatusAccepted)
}

func (h *APIHandler) GetPaymentID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "failed to get payment id", http.StatusBadRequest)
		return
	}

	payment, err := h.svc.GetPaymentByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "payment not found", http.StatusNotFound)
			return
		}
		slog.Error("failed to get payment", "payment_id", id, "error", err)
		http.Error(w, "failed to get payment", http.StatusInternalServerError)
		return
	}

	writeJSON(w, GetPaymentResponse{
		Payment: payment,
	}, http.StatusOK)
}
