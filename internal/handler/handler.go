package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/electr1fy0/okane/internal/payment"
	"github.com/go-playground/validator/v10"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5"
)

var validate = validator.New()

type CreatePaymentResponse struct {
	Payment  payment.Payment `json:"payment"`
	Created  bool            `json:"created"`
	Enqueued bool            `json:"enqueued"`
}

type GetPaymentResponse struct {
	Payment payment.Payment `json:"payment"`
}

type createPaymentRequest struct {
	Amount         int64  `json:"amount" validate:"gt=0"`
	IdempotencyKey string `json:"idempotency_key" validate:"required"`
}

type PaymentService interface {
	CreatePayment(ctx context.Context, params payment.CreatePaymentParams) (*payment.Payment, bool, error)
	GetPaymentByID(ctx context.Context, id string) (payment.Payment, error)
	EnqueuePayment(ctx context.Context, paymentID string) error
	RetryFailedPayment(ctx context.Context, paymentID string) error
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

	slog.Error(err.Error(), "error", err)
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
		}
	}
	if err := validate.Struct(req); err != nil {
		return &HttpError{
			status:  http.StatusBadRequest,
			message: "validation failed, invalid request body" + err.Error(),
		}
	}

	paymentRes, created, err := h.svc.CreatePayment(ctx, payment.CreatePaymentParams{
		Amount:         req.Amount,
		Status:         payment.StatusPending,
		IdempotencyKey: req.IdempotencyKey,
	})

	if err != nil {
		return &HttpError{
			status:  http.StatusInternalServerError,
			message: "failed to create payment: " + err.Error(),
		}
	}

	if !created {
		slog.Info("payment already exists", "idempotency_key", paymentRes.IdempotencyKey)
		writeJSON(w, CreatePaymentResponse{
			Payment:  *paymentRes,
			Created:  false,
			Enqueued: false,
		}, http.StatusOK)
		return nil
	}

	if err := h.svc.EnqueuePayment(ctx, paymentRes.ID.String()); err != nil {
		return &HttpError{
			status:  http.StatusInternalServerError,
			message: "failed to enqueue payment: " + err.Error(),
		}
	}

	writeJSON(w, CreatePaymentResponse{
		Payment:  *paymentRes,
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
		}
	}

	p, err := h.svc.GetPaymentByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &HttpError{
				status:  http.StatusNotFound,
				message: "payment not found",
			}
		}
		slog.Error("failed to get payment", "payment_id", id, "error", err)
		return &HttpError{
			status:  http.StatusInternalServerError,
			message: "failed to get payment",
		}
	}

	writeJSON(w, GetPaymentResponse{
		Payment: p,
	}, http.StatusOK)
	return nil
}

func (h *APIHandler) RetryPayment(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("id")
	if id == "" {
		return &HttpError{
			status:  http.StatusBadRequest,
			message: "missing payment id",
		}
	}

	err := h.svc.RetryFailedPayment(r.Context(), id)
	if err != nil {
		if errors.Is(err, asynq.ErrTaskNotFound) {
			return &HttpError{
				status:  http.StatusNotFound,
				message: "payment task not found in archived or retry queue",
			}
		}
		return &HttpError{
			status:  http.StatusInternalServerError,
			message: "failed to retry payment: " + err.Error(),
		}
	}

	writeJSON(w, map[string]string{"status": "queued"}, http.StatusOK)
	return nil
}
