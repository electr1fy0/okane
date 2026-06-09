package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/electr1fy0/okane/internal/payment"
	"github.com/electr1fy0/okane/internal/store"
	"github.com/hibiken/asynq"
)

const (
	PaymentProcessingTimeout = 5 * time.Second
)

type paymentTask struct {
	PaymentID string `json:"payment_id"`
}

type Service struct {
	store          store.Store
	providerClient *ProviderClient
	asynqClient    *asynq.Client
	asynqInspector *asynq.Inspector
}

func New(paymentStore store.Store, providerClient *ProviderClient, ac *asynq.Client, redisOpt asynq.RedisClientOpt) *Service {
	return &Service{
		store:          paymentStore,
		providerClient: providerClient,
		asynqClient:    ac,
		asynqInspector: asynq.NewInspector(redisOpt),
	}
}

func (s *Service) EnqueuePayment(ctx context.Context, paymentID string) error {
	slog.Debug("EnqueuePayment called", "payment_id", paymentID)
	p, err := s.store.GetPaymentByID(ctx, paymentID)
	if err != nil {
		slog.Error("failed to load payment for enqueueing", "payment_id", paymentID, "error", err)
		return err
	}
	slog.Debug("loaded payment details for enqueueing", "payment_id", paymentID, "amount", p.Amount, "status", p.Status)

	payload, err := json.Marshal(paymentTask{PaymentID: paymentID})
	if err != nil {
		slog.Error("failed to marshal payment task", "payment_id", paymentID, "error", err)
		return err
	}

	queue := "default"
	if p.Amount >= 10000 {
		queue = "critical"
	} else if p.Amount < 1000 {
		queue = "low"
	}
	slog.Debug("determined destination queue", "payment_id", paymentID, "amount", p.Amount, "queue", queue)

	task := asynq.NewTask("payment:process", payload)
	_, err = s.asynqClient.Enqueue(task,
		asynq.Queue(queue),
		asynq.MaxRetry(8),
		asynq.Timeout(PaymentProcessingTimeout),
		asynq.TaskID(paymentID),
	)
	if err != nil {
		slog.Error("failed to enqueue payment task", "payment_id", paymentID, "queue", queue, "error", err)
	} else {
		slog.Debug("successfully enqueued task to Asynq", "payment_id", paymentID, "queue", queue)
	}
	return err
}

func (s *Service) CreatePayment(ctx context.Context, params payment.CreatePaymentParams) (*payment.Payment, bool, error) {
	slog.Debug("CreatePayment called in service", "amount", params.Amount, "idempotency_key", params.IdempotencyKey)
	p, created, err := s.store.CreatePayment(ctx, params)
	if err != nil {
		slog.Error("failed to create payment in store", "error", err)
		return nil, false, err
	}
	slog.Debug("CreatePayment store result", "payment_id", p.ID, "created", created, "status", p.Status)
	return p, created, nil
}

func (s *Service) GetPaymentByID(ctx context.Context, id string) (payment.Payment, error) {
	slog.Debug("GetPaymentByID called", "payment_id", id)
	return s.store.GetPaymentByID(ctx, id)
}

func (c *ProviderClient) executePayment(ctx context.Context) (string, int, error) {
	slog.Debug("executing payment call to provider", "url", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL, nil)
	if err != nil {
		slog.Error("failed to create provider request", "error", err)
		return "", -1, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		slog.Error("failed to execute provider request", "error", err)
		return "", -1, err
	}
	defer resp.Body.Close()

	var respStruct struct {
		ProviderRef string `json:"provider_ref"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&respStruct)
	slog.Debug("received response from provider", "status_code", resp.StatusCode, "provider_ref", respStruct.ProviderRef)

	return respStruct.ProviderRef, resp.StatusCode, err
}

func (s *Service) ProcessPayment(ctx context.Context, paymentID string) error {
	slog.Info("payment processing started", "payment_id", paymentID)

	p, err := s.store.GetPaymentByID(ctx, paymentID)
	if err != nil {
		slog.Error("failed to load payment", "payment_id", paymentID, "error", err)
		return err
	}
	slog.Debug("loaded payment details before updating processing status", "payment_id", paymentID, "status", p.Status)

	switch p.Status {
	case payment.StatusSuccess, payment.StatusFailedFinal:
		slog.Info("payment already terminal, skipping", "payment_id", paymentID, "status", p.Status)
		return nil
	case payment.StatusPending:
		slog.Debug("transitioning status from pending to processing", "payment_id", paymentID)
		p, err = s.store.UpdatePayment(ctx, paymentID, payment.StatusPending, payment.StatusProcessing, "", false)
		if err != nil {
			slog.Error("failed to move payment to processing", "payment_id", paymentID, "error", err)
			return err
		}
		slog.Debug("payment transitioned to processing in store", "payment_id", paymentID)
	case payment.StatusFailedRetryable:
		slog.Debug("transitioning status from failed_retryable to processing", "payment_id", paymentID)
		p, err = s.store.UpdatePayment(ctx, paymentID, payment.StatusFailedRetryable, payment.StatusProcessing, "", false)
		if err != nil {
			slog.Error("failed to resume retryable payment", "payment_id", paymentID, "error", err)
			return err
		}
		slog.Debug("payment transitioned to processing in store", "payment_id", paymentID)
	default:
		slog.Warn("skipping payment with non-processable status", "payment_id", paymentID, "status", p.Status)
		return nil
	}

	slog.Info("calling provider", "payment_id", paymentID)
	providerRef, status, err := s.providerClient.executePayment(ctx)
	if err != nil {
		slog.Error("failed to request payment provider", "payment_id", paymentID, "error", err)
		slog.Debug("recording connection/provider request failure in store", "payment_id", paymentID, "error", err.Error())
		_ = s.store.RecordProcessingFailure(ctx, paymentID, err.Error(), true)
		return err
	}

	switch status {
	case http.StatusOK:
		slog.Debug("updating Postgres payment status to success", "payment_id", paymentID)
		_, err = s.store.UpdatePayment(ctx, paymentID, payment.StatusProcessing, payment.StatusSuccess, "", true)
		if err != nil {
			slog.Error("failed to update payment success state", "payment_id", paymentID, "error", err)
			return err
		}
		slog.Debug("updating provider ref in Postgres", "payment_id", paymentID, "provider_ref", providerRef)
		err = s.store.UpdateProviderRef(ctx, paymentID, providerRef)
		if err != nil {
			slog.Error("failed to update provider ref", "payment_id", paymentID, "error", err)
			return err
		}
		slog.Info("payment succeeded", "payment_id", paymentID, "provider_ref", providerRef)
		return nil

	case http.StatusServiceUnavailable:
		slog.Warn("provider unavailable", "payment_id", paymentID)
		slog.Debug("recording 503 service unavailable failure in Postgres", "payment_id", paymentID)
		err = s.store.RecordProcessingFailure(ctx, paymentID, "service unavailable", true)
		if err != nil {
			slog.Error("failed to record processing failure", "payment_id", paymentID, "error", err)
			return err
		}
		slog.Debug("updating status from processing to failed_retryable", "payment_id", paymentID)
		if _, err := s.store.UpdatePayment(ctx, paymentID, payment.StatusProcessing, payment.StatusFailedRetryable, "service unavailable", false); err != nil {
			slog.Error("failed to update payment to retryable", "payment_id", paymentID, "error", err)
		}
		return fmt.Errorf("provider unavailable for payment %s", paymentID)

	case http.StatusUnprocessableEntity:
		slog.Warn("payment rejected by provider", "payment_id", paymentID)
		slog.Debug("updating status from processing to failed_final (terminal)", "payment_id", paymentID)
		_, err = s.store.UpdatePayment(ctx, paymentID, payment.StatusProcessing, payment.StatusFailedFinal, "unprocessable payment", false)
		if err != nil {
			slog.Error("failed to update failed payment state", "payment_id", paymentID, "error", err)
			return err
		}
		return nil

	default:
		slog.Warn("provider returned unexpected status", "payment_id", paymentID, "status_code", status)
		slog.Debug("recording unexpected status failure in Postgres", "payment_id", paymentID, "status_code", status)
		err = s.store.RecordProcessingFailure(ctx, paymentID, fmt.Sprintf("provider returned status %d", status), true)
		if err != nil {
			slog.Error("failed to record processing failure", "payment_id", paymentID, "error", err)
			return err
		}
		slog.Debug("updating status from processing to failed_retryable due to unexpected status", "payment_id", paymentID)
		if _, err := s.store.UpdatePayment(ctx, paymentID, payment.StatusProcessing, payment.StatusFailedRetryable, fmt.Sprintf("provider returned status %d", status), false); err != nil {
			slog.Error("failed to update payment to retryable", "payment_id", paymentID, "error", err)
		}
		return fmt.Errorf("unexpected status %d from provider for payment %s", status, paymentID)
	}
}

func (s *Service) RetryFailedPayment(ctx context.Context, paymentID string) error {
	slog.Debug("manual RetryFailedPayment requested", "payment_id", paymentID)
	p, err := s.store.GetPaymentByID(ctx, paymentID)
	if err != nil {
		slog.Error("failed to load payment details for manual retry", "payment_id", paymentID, "error", err)
		return err
	}

	queue := "default"
	if p.Amount >= 10000 {
		queue = "critical"
	} else if p.Amount < 1000 {
		queue = "low"
	}
	slog.Debug("determined queue for manual retry", "payment_id", paymentID, "queue", queue)

	slog.Debug("triggering task replay via Asynq Inspector", "payment_id", paymentID, "queue", queue)
	err = s.asynqInspector.RunTask(queue, paymentID)
	if err != nil {
		if errors.Is(err, asynq.ErrTaskNotFound) {
			slog.Error("failed manual retry: task not found in queues", "payment_id", paymentID, "queue", queue)
			return fmt.Errorf("task not found in retryable/archived queues: %w", err)
		}
		slog.Error("failed manual retry: run task error", "payment_id", paymentID, "queue", queue, "error", err)
		return fmt.Errorf("failed to run task: %w", err)
	}

	slog.Debug("updating status from failed_final to pending in store for manual retry", "payment_id", paymentID)
	_, err = s.store.UpdatePayment(ctx, paymentID, payment.StatusFailedFinal, payment.StatusPending, "manual retry initiated", false)
	if err != nil {
		slog.Error("failed to update payment status back to pending after manual retry", "payment_id", paymentID, "error", err)
	} else {
		slog.Debug("successfully reset payment status to pending in store", "payment_id", paymentID)
	}
	return err
}
