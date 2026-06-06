package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/electr1fy0/okane/internal/store"
	"github.com/electr1fy0/okane/internal/types"
	"github.com/go-playground/validator/v10"
	"github.com/jackc/pgx/v5"
)

var validate = validator.New()

type CreatePaymentResponse struct {
	Payment  store.Payment `json:"payment"`
	Created  bool          `json:"created"`
	Enqueued bool          `json:"enqueued"`
}

type GetPaymentResponse struct {
	Payment store.Payment `json:"payment"`
}

type createPaymentRequest struct {
	Amount         int64  `json:"amount" validate:"gt=0"`
	IdempotencyKey string `json:"idempotency_key" validate:"required"`
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

type HttpError struct {
	status  int
	message string
	err     error
}

func (h *HttpError) Error() string {
	return h.message
}

func (h *APIHandler) Health(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "don't worry about me, mate")
}

func HttpErrorResponse(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	var httpError *HttpError
	if ok := errors.As(err, &httpError); ok {
		slog.Error(httpError.Error(), "error", err)
		http.Error(w, httpError.Error(), httpError.status)
		return
	}

	slog.Error(httpError.Error(), "error", err)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func Handle(fn func(http.ResponseWriter, *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		HttpErrorResponse(w, fn(w, r))
	}
}

func (h *APIHandler) CreatePayment(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	var req createPaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return &HttpError{
			status:  http.StatusBadRequest,
			message: "failed to decode create payment request",
			err:     err,
		}
	}
	if err := validate.Struct(req); err != nil {
		return &HttpError{
			status:  http.StatusBadRequest,
			message: "validation failed, invalid request body" + err.Error(),
			err:     err,
		}
	}

	payment, created, err := h.svc.CreatePayment(ctx, store.CreatePaymentParams{
		Amount:         req.Amount,
		Status:         types.PaymentStatusPending,
		IdempotencyKey: req.IdempotencyKey,
	})

	if err != nil {
		return &HttpError{
			status:  http.StatusInternalServerError,
			message: "failed to create payment: " + err.Error(),
			err:     err,
		}
	}

	if !created {
		slog.Info("payment already exists", "idempotency_key", payment.IdempotencyKey)
		writeJSON(w, CreatePaymentResponse{
			Payment:  *payment,
			Created:  false,
			Enqueued: false,
		}, http.StatusOK)
		return nil
	}

	if err := h.svc.EnqueuePayment(ctx, payment.ID.String()); err != nil {
		return &HttpError{
			status:  http.StatusInternalServerError,
			message: "failed to enqueue payment: " + err.Error(),
			err:     err,
		}
	}

	writeJSON(w, CreatePaymentResponse{
		Payment:  *payment,
		Created:  true,
		Enqueued: true,
	}, http.StatusAccepted)

	return nil
}

func (h *APIHandler) GetPaymentID(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("id")
	if id == "" {
		return &HttpError{
			status:  http.StatusBadRequest,
			message: "failed to get payment id in req",
			err:     fmt.Errorf("pathvalue id is empty"),
		}
	}

	payment, err := h.svc.GetPaymentByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &HttpError{
				status:  http.StatusNotFound,
				message: "payment not found",
				err:     err,
			}
		}
		slog.Error("failed to get payment", "payment_id", id, "error", err)
		http.Error(w, "failed to get payment", http.StatusInternalServerError)
		return &HttpError{
			status:  http.StatusInternalServerError,
			message: "failed to get payment",
			err:     err,
		}
	}

	writeJSON(w, GetPaymentResponse{
		Payment: payment,
	}, http.StatusOK)
	return nil
}
