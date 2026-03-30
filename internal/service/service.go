package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/electr1fy0/okane/internal/queue"
	"github.com/electr1fy0/okane/internal/store"
	"github.com/electr1fy0/okane/internal/types"
	"golang.org/x/sync/errgroup"
)

const (
	MaxImmediateRetries  = 1
	MaxAttemptsBeforeDLQ = 8
	MaxBackoff           = 30 * time.Minute
	WorkerCount          = 5

	QueueBlockTimeout          = 1 * time.Second
	ReaperInterval             = 10 * time.Second
	ProcessingTimeout          = 1 * time.Minute
	PaymentProcessingTimeout   = 5 * time.Second
	RetryWorkerPollPeriod      = 1 * time.Second
	RetryWorkerMaxPollInterval = 30 * time.Second
)

type Service struct {
	store          store.Store
	queue          queue.Queue
	providerClient *ProviderClient
	workers        int
}

func New(paymentStore store.Store, paymentQueue queue.Queue, providerClient *ProviderClient, workers int) *Service {
	return &Service{
		store:          paymentStore,
		queue:          paymentQueue,
		providerClient: providerClient,
		workers:        workers,
	}
}
func (s *Service) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	slog.Info("starting workers", "count", s.workers)
	for i := range s.workers {
		workerID := i
		g.Go(func() error {
			slog.Info("worker started", "worker_id", workerID)
			return s.Work(ctx, workerID)
		})
	}
	return g.Wait()
}

func (s *Service) EnqueuePayment(ctx context.Context, paymentID string) error {
	return s.queue.EnqueuePending(ctx, paymentID)
}

func (s *Service) CreatePayment(ctx context.Context, params store.CreatePaymentParams) (*store.Payment, bool, error) {
	return s.store.CreatePayment(ctx, params)
}

func (s *Service) GetPaymentByID(ctx context.Context, id string) (store.Payment, error) {
	return s.store.GetPaymentByID(ctx, id)
}

func (c *ProviderClient) executePayment(ctx context.Context) (string, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL, nil)
	if err != nil {
		return "", -1, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", -1, err
	}
	defer resp.Body.Close()

	var respStruct struct {
		ProviderRef string `json:"provider_ref"`
	}
	// assuming that the resp body is not malformed and will just ignore
	// the err in case of empty body for other status codes
	_ = json.NewDecoder(resp.Body).Decode(&respStruct)

	return respStruct.ProviderRef, resp.StatusCode, err

}

func (s *Service) processPayment(ctx context.Context, paymentID string) error {
	slog.Info("payment processing started", "payment_id", paymentID)
	success := false
	unprocessable := false

retryLoop:
	for i := range MaxImmediateRetries {
		slog.Info("calling provider", "payment_id", paymentID, "attempt", i+1)
		providerRef, status, err := s.providerClient.executePayment(ctx)
		if err != nil {
			slog.Error("failed to request payment provider", "error", err)
			continue
		}

		switch status {
		case http.StatusOK:
			_, err = s.store.UpdatePayment(ctx, paymentID, types.PaymentStatusProcessing, types.PaymentStatusSuccess, "", true)
			if err != nil {
				slog.Error("failed to update payment success state", "payment_id", paymentID, "error", err)
				return err
			}
			err = s.store.UpdateProviderRef(ctx, paymentID, providerRef)
			if err != nil {
				slog.Error("failed to update provider ref", "payment_id", paymentID, "error", err)
				return err
			}
			slog.Info("payment succeeded", "payment_id", paymentID, "provider_ref", providerRef)
			success = true
			break retryLoop
		case http.StatusServiceUnavailable:
			slog.Warn("provider unavailable", "payment_id", paymentID, "attempt", i+1)
			err = s.store.RecordProcessingFailure(ctx, paymentID, "service unavailable", true)
			if err != nil {
				slog.Error("failed to update retryable payment state", "payment_id", paymentID, "attempt", i, "error", err)
				return err
			}
		case http.StatusUnprocessableEntity:
			slog.Warn("payment rejected by provider", "payment_id", paymentID)
			_, err = s.store.UpdatePayment(ctx, paymentID, types.PaymentStatusProcessing, types.PaymentStatusFailedFinal, "unprocessable payment", false)
			if err != nil {
				slog.Error("failed to update failed payment state", "payment_id", paymentID, "error", err)
				return err
			}
			unprocessable = true
			break retryLoop
		default:
			slog.Warn("provider returned unexpected status", "payment_id", paymentID, "status_code", status)
			err = s.store.RecordProcessingFailure(ctx, paymentID, fmt.Sprintf("provider returned status %d", status), true)
			if err != nil {
				return err
			}
		}
	}

	_ = s.queue.RemoveProcessing(ctx, paymentID)

	if success || unprocessable {
		return nil
	}

	payment, err := s.store.UpdatePayment(ctx, paymentID, types.PaymentStatusProcessing, types.PaymentStatusFailedRetryable, "service unavailable", false)
	if err != nil {
		slog.Error("failed to update retryable failure state", "payment_id", paymentID, "error", err)
		return err
	}

	return s.addToRetryLater(ctx, paymentID, payment.Attempts)
}

func (s *Service) addToRetryLater(ctx context.Context, paymentID string, attempts int) error {
	backoff := 5 * time.Second << attempts
	backoff = min(backoff, MaxBackoff)

	retryAt := time.Now().Add(backoff)
	if err := s.queue.EnqueueDelayed(ctx, paymentID, retryAt); err != nil {
		return err
	}
	slog.Info("payment scheduled for retry", "payment_id", paymentID, "retry_at_unix", retryAt.Unix())
	return nil
}

func (s *Service) claimNextPayment(ctx context.Context) (string, error) {
	return s.queue.MovePendingToProcessing(ctx, QueueBlockTimeout)
}

func (s *Service) Work(ctx context.Context, id int) error {
	for {
		paymentID, err := s.claimNextPayment(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if paymentID == "" {
			continue
		}

		err = s.queue.MarkProcessing(ctx, paymentID, time.Now())
		if err != nil {
			slog.Error("failed to mark payment processing time", "payment_id", paymentID, "error", err)
			return err
		}
		slog.Info("worker picked payment", "worker_id", id, "payment_id", paymentID)

		payment, err := s.store.GetPaymentByID(ctx, paymentID)
		if err != nil {
			slog.Error("failed to load payment", "payment_id", paymentID, "worker_id", id, "error", err)
			return err
		}

		if payment.Status == types.PaymentStatusSuccess || payment.Status == types.PaymentStatusFailedFinal {
			_ = s.queue.RemoveProcessing(ctx, paymentID)
			continue
		}
		if payment.Status == types.PaymentStatusPending {
			payment, err = s.store.UpdatePayment(ctx, paymentID, types.PaymentStatusPending, types.PaymentStatusProcessing, "", false)
			if err != nil {
				slog.Error("failed to move payment to processing", "payment_id", paymentID, "worker_id", id, "error", err)
				return err
			}
		}

		if payment.Status == types.PaymentStatusFailedRetryable {
			payment, err = s.store.UpdatePayment(ctx, paymentID, types.PaymentStatusFailedRetryable, types.PaymentStatusProcessing, "", false)
			if err != nil {
				slog.Error("failed to resume retryable payment", "payment_id", paymentID, "worker_id", id, "error", err)
				return err
			}
		}

		if payment.Status != types.PaymentStatusProcessing {
			slog.Warn("skipping payment with non-processable status", "payment_id", paymentID, "worker_id", id, "status", payment.Status)
			_ = s.queue.RemoveProcessing(ctx, paymentID)
			continue
		}

		if payment.Attempts >= MaxAttemptsBeforeDLQ {
			_ = s.queue.RemoveProcessing(ctx, paymentID)
			_ = s.queue.EnqueueDead(ctx, paymentID)
			lastError := ""
			if payment.LastError != nil {
				lastError = *payment.LastError
			}

			_, err = s.store.UpdatePayment(ctx, paymentID, types.PaymentStatusProcessing, types.PaymentStatusFailedFinal, lastError, false)
			if err != nil {
				slog.Error("failed to update dead-lettered payment", "payment_id", paymentID, "error", err)
				return err
			}
			slog.Warn("payment moved to dead queue", "payment_id", paymentID, "attempts", payment.Attempts)
			continue
		}

		paymentCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), PaymentProcessingTimeout)
		err = s.processPayment(paymentCtx, paymentID)
		cancel()
		if err != nil {
			slog.Error("failed to process payment", "payment_id", paymentID, "worker_id", id, "error", err)
			return err
		}
	}
}

func (s *Service) ReaperWorker(ctx context.Context) error {
	slog.Info("reaper worker started")
	for {
		select {
		case <-time.After(ReaperInterval):
		case <-ctx.Done():
			return nil
		}

		paymentIDs, err := s.queue.ListProcessing(ctx, 10)
		if err != nil {
			return err
		}

		for _, paymentID := range paymentIDs {
			startedAt, ok, err := s.queue.GetProcessingTime(ctx, paymentID)
			if err != nil {
				slog.Error("failed to get processing time", "id", paymentID, "error", err)
				continue
			}
			if ok && time.Since(startedAt) < ProcessingTimeout {
				continue
			}

			_ = s.queue.RemoveProcessing(ctx, paymentID)
			if err := s.queue.EnqueuePending(ctx, paymentID); err != nil {
				return err
			}
			slog.Warn("reaper requeued payment", "payment_id", paymentID)
		}
	}
}

func (s *Service) RetryWorker(ctx context.Context) error {
	slog.Info("retry worker started")
	for {
		job, err := s.queue.NextDelayed(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if job == nil {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(RetryWorkerPollPeriod):
				continue
			}
		}

		if job.RunAt.After(time.Now()) {
			select {
			case <-time.After(min(time.Until(job.RunAt), RetryWorkerMaxPollInterval)):
				continue
			case <-ctx.Done():
				return nil
			}
		}
		paymentID := job.PaymentID

		payment, err := s.store.GetPaymentByID(ctx, paymentID)
		if err != nil {
			slog.Error("retry worker failed to load payment", "payment_id", paymentID, "error", err)
			continue
		}
		if payment.Status != types.PaymentStatusFailedRetryable {
			slog.Warn("retry worker dropping non-retryable payment", "payment_id", paymentID, "status", payment.Status)
			_ = s.queue.RemoveDelayed(ctx, paymentID)
			continue
		}

		if err := s.queue.EnqueuePending(ctx, paymentID); err != nil {
			return err
		}
		if err := s.queue.RemoveDelayed(ctx, paymentID); err != nil {
			return err
		}
		slog.Info("retry worker requeued payment", "payment_id", paymentID)
	}
}
