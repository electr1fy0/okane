package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

const (
	maxImmediateRetries  = 1
	maxAttemptsBeforeDLQ = 8
	maxBackoff           = 30 * time.Minute
	workerCount          = 5

	queueBlockTimeout          = 1 * time.Second
	reaperInterval             = 10 * time.Second
	processingTimeout          = 1 * time.Minute
	paymentProcessingTimeout   = 5 * time.Second
	retryWorkerPollPeriod      = 1 * time.Second
	retryWorkerMaxPollInterval = 30 * time.Second

	serverReadTimeout     = 5 * time.Second
	serverWriteTimeout    = 10 * time.Second
	serverIdleTimeout     = 60 * time.Second
	serverShutdownTimeout = 30 * time.Second
)

const (
	paymentStatusPending         = "pending"
	paymentStatusProcessing      = "processing"
	paymentStatusSuccess         = "success"
	paymentStatusFailedRetryable = "failed_retryable"
	paymentStatusFailedFinal     = "failed_final"
)

var errInvalidStateTransition = errors.New("invalid state transition")

type Payment struct {
	ID             uuid.UUID `json:"id"`
	Amount         int64     `json:"amount"`
	Status         string    `json:"status"`
	IdempotencyKey string    `json:"idempotency_key"`
	ProviderRef    *string   `json:"provider_ref,omitempty"`
	Attempts       int       `json:"attempts"`
	LastError      *string   `json:"last_error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type CreatePaymentParams struct {
	Amount         int64  `json:"amount"`
	Status         string `json:"status"`
	IdempotencyKey string `json:"idempotency_key"`
}

type Service struct {
	db             *pgxpool.Pool
	rdb            *redis.Client
	providerClient *ProviderClient
	workers        int
}

func mustGetEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("missing required env var %s", key)
	}
	return value
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

func (s *Service) processPayment(ctx context.Context, paymentID string) error {
	slog.Info("payment processing started", "payment_id", paymentID)
	success := false
	unprocessable := false
retryLoop:
	for i := range maxImmediateRetries {
		slog.Info("calling provider", "payment_id", paymentID, "attempt", i+1)

		req, err := http.NewRequestWithContext(ctx, "GET", s.providerClient.baseURL, nil)
		if err != nil {
			return err
		}
		resp, err := s.providerClient.http.Do(req)
		if err != nil {
			slog.Error("failed to request payment provider", "error", err)
			continue
		}

		switch resp.StatusCode {
		case http.StatusOK:
			var respStruct struct {
				ProviderRef string `json:"provider_ref"`
			}
			err = json.NewDecoder(resp.Body).Decode(&respStruct)
			_ = resp.Body.Close()
			if err != nil {
				slog.Error("failed to decode provider response", "payment_id", paymentID, "error", err)
				return err
			}
			err = s.UpdatePayment(ctx, paymentID, paymentStatusProcessing, paymentStatusSuccess, "", true)
			if err != nil {
				slog.Error("failed to update payment success state", "payment_id", paymentID, "error", err)
				return err
			}
			err = s.UpdateProviderRef(ctx, paymentID, respStruct.ProviderRef)
			if err != nil {
				slog.Error("failed to update provider ref", "payment_id", paymentID, "error", err)
				return err
			}
			slog.Info("payment succeeded", "payment_id", paymentID, "provider_ref", respStruct.ProviderRef)
			success = true
			break retryLoop
		case http.StatusServiceUnavailable:
			_ = resp.Body.Close()
			slog.Warn("provider unavailable", "payment_id", paymentID, "attempt", i+1)
			err = s.RecordProcessingFailure(ctx, paymentID, "service unavailable", true)
			if err != nil {
				slog.Error("failed to update retryable payment state", "payment_id", paymentID, "attempt", i, "error", err)
				return err
			}
		case http.StatusUnprocessableEntity:
			_ = resp.Body.Close()
			slog.Warn("payment rejected by provider", "payment_id", paymentID)
			err = s.UpdatePayment(ctx, paymentID, paymentStatusProcessing, paymentStatusFailedFinal, "unprocessable payment", false)
			if err != nil {
				slog.Error("failed to update failed payment state", "payment_id", paymentID, "error", err)
				return err
			}
			unprocessable = true
			break retryLoop
		default:
			_ = resp.Body.Close()
			slog.Warn("provider returned unexpected status", "payment_id", paymentID, "status_code", resp.StatusCode)
			err = s.RecordProcessingFailure(ctx, paymentID, fmt.Sprintf("provider returned status %d", resp.StatusCode), true)
			if err != nil {
				return err
			}
		}
	}

	_ = s.rdb.LRem(ctx, "payments:processing", 1, paymentID).Err()
	_ = s.rdb.HDel(ctx, "payments:processing:times", paymentID).Err()

	if success || unprocessable {
		return nil
	}

	err := s.UpdatePayment(ctx, paymentID, paymentStatusProcessing, paymentStatusFailedRetryable, "service unavailable", false)
	if err != nil {
		slog.Error("failed to update retryable failure state", "payment_id", paymentID, "error", err)
		return err
	}

	payment, err := s.GetPaymentByID(ctx, paymentID)
	if err != nil {
		slog.Error("payment process error", "error", err)
		return err
	}
	backoff := time.Minute << payment.Attempts
	backoff = min(backoff, maxBackoff)

	retryAt := time.Now().Add(backoff).Unix()

	s.rdb.ZAdd(ctx, "payments:delayed", redis.Z{
		Score:  float64(retryAt),
		Member: paymentID,
	})
	slog.Info("payment scheduled for retry", "payment_id", paymentID, "retry_at_unix", retryAt)

	return nil
}

func (s *Service) Work(ctx context.Context, id int) error {
	for {
		paymentID, err := s.rdb.BLMove(ctx, "payments:pending", "payments:processing", "RIGHT", "LEFT", queueBlockTimeout).Result()

		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if err == redis.Nil {
				continue
			}
			return err
		}

		err = s.rdb.HSet(ctx, "payments:processing:times", paymentID, time.Now().Unix()).Err()
		if err != nil {
			slog.Error("failed to mark payment processing time", "payment_id", paymentID, "error", err)
			return err
		}
		slog.Info("worker picked payment", "worker_id", id, "payment_id", paymentID)

		payment, err := s.GetPaymentByID(ctx, paymentID)
		if err != nil {
			slog.Error("failed to load payment", "payment_id", paymentID, "worker_id", id, "error", err)
			return err
		}

		if payment.Status == paymentStatusSuccess || payment.Status == paymentStatusFailedFinal {
			s.rdb.LRem(ctx, "payments:processing", 1, paymentID)
			s.rdb.HDel(ctx, "payments:processing:times", paymentID)
			continue
		}
		if payment.Status == paymentStatusPending {
			err = s.UpdatePayment(ctx, paymentID, paymentStatusPending, paymentStatusProcessing, "", false)
			if err != nil {
				slog.Error("failed to move payment to processing", "payment_id", paymentID, "worker_id", id, "error", err)
				return err
			}
			payment.Status = paymentStatusProcessing
		}

		if payment.Status == paymentStatusFailedRetryable {
			err = s.UpdatePayment(ctx, paymentID, paymentStatusFailedRetryable, paymentStatusProcessing, "", false)
			if err != nil {
				slog.Error("failed to resume retryable payment", "payment_id", paymentID, "worker_id", id, "error", err)
				return err
			}
			payment.Status = paymentStatusProcessing
		}

		if payment.Status != paymentStatusProcessing {
			slog.Warn("skipping payment with non-processable status", "payment_id", paymentID, "worker_id", id, "status", payment.Status)
			s.rdb.LRem(ctx, "payments:processing", 1, paymentID)
			s.rdb.HDel(ctx, "payments:processing:times", paymentID)
			continue
		}

		if payment.Attempts >= maxAttemptsBeforeDLQ {
			s.rdb.LRem(ctx, "payments:processing", 1, paymentID)
			s.rdb.HDel(ctx, "payments:processing:times", paymentID)
			s.rdb.LPush(ctx, "payments:dead", paymentID)
			lastError := ""
			if payment.LastError != nil {
				lastError = *payment.LastError
			}

			err = s.UpdatePayment(ctx, paymentID, paymentStatusProcessing, paymentStatusFailedFinal, lastError, false)
			if err != nil {
				slog.Error("failed to update dead-lettered payment", "payment_id", paymentID, "error", err)
				return err
			}
			slog.Warn("payment moved to dead queue", "payment_id", paymentID, "attempts", payment.Attempts)
			continue
		}
		paymentCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), paymentProcessingTimeout)
		err = s.processPayment(paymentCtx, paymentID)
		cancel()

		if err != nil {
			slog.Error("failed to process payment", "payment_id", paymentID, "worker_id", id, "error", err)
			return err
		}
	}
}

type ProviderClient struct {
	baseURL string
	http    *http.Client
}

func (s *Service) ReaperWorker(ctx context.Context) error {
	slog.Info("reaper worker started")
	for {
		select {
		case <-time.After(reaperInterval):
		case <-ctx.Done():
			return nil
		}

		paymentIDs, err := s.rdb.LRange(ctx, "payments:processing", 0, 9).Result()
		if err != nil {
			return err
		}

		for _, paymentID := range paymentIDs {
			ts, err := s.rdb.HGet(ctx, "payments:processing:times", paymentID).Result()
			if err == redis.Nil {
			} else if err != nil {
				slog.Error("failed to get processing time", "id", paymentID, "error", err)
				continue
			} else {
				tsInt, err := strconv.ParseInt(ts, 10, 64)
				if err != nil {
					slog.Error("failed to parse timestamp", "id", paymentID, "error", err)
					continue
				}
				if time.Since(time.Unix(tsInt, 0)) < processingTimeout {
					continue
				}
			}

			s.rdb.LRem(ctx, "payments:processing", 1, paymentID)
			s.rdb.HDel(ctx, "payments:processing:times", paymentID)
			s.rdb.LPush(ctx, "payments:pending", paymentID)
			slog.Warn("reaper requeued payment", "payment_id", paymentID)
		}
	}
}

func (s *Service) RetryWorker(ctx context.Context) error {
	slog.Info("retry worker started")
	for {
		jobs, err := s.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
			Key:     "payments:delayed",
			ByScore: true,
			Start:   "-inf",
			Stop:    "+inf",
			Offset:  0,
			Count:   1,
		}).Result()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return err
		}
		if len(jobs) == 0 {
			select {
			case <-ctx.Done():
				return err
			case <-time.After(retryWorkerPollPeriod):
				continue
			}
		}
		now := float64(time.Now().Unix())

		if jobs[0].Score > now {
			select {
			case <-time.After(min(time.Duration(jobs[0].Score-now)*time.Second, retryWorkerMaxPollInterval)):
				continue
			case <-ctx.Done():
				return err
			}
		}

		paymentID, ok := jobs[0].Member.(string)
		if !ok {
			slog.Error("retry worker received non-string payment id", "member", jobs[0].Member)
			s.rdb.ZRem(ctx, "payments:delayed", jobs[0].Member)
			continue
		}

		payment, err := s.GetPaymentByID(ctx, paymentID)
		if err != nil {
			slog.Error("retry worker failed to load payment", "payment_id", paymentID, "error", err)
			continue
		}
		if payment.Status != paymentStatusFailedRetryable {
			slog.Warn("retry worker dropping non-retryable payment", "payment_id", paymentID, "status", payment.Status)
			s.rdb.ZRem(ctx, "payments:delayed", paymentID)
			continue
		}

		s.rdb.LPush(ctx, "payments:pending", paymentID)
		s.rdb.ZRem(ctx, "payments:delayed", paymentID)
		slog.Info("retry worker requeued payment", "payment_id", paymentID)
	}
}

func (s *Service) UpdatePayment(ctx context.Context, id, fromStatus, toStatus, lastError string, incrementAttempts bool) error {
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

	tag, err := s.db.Exec(ctx, query, toStatus, nullableText(lastError), attemptDelta, id, fromStatus)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s -> %s for payment %s", errInvalidStateTransition, fromStatus, toStatus, id)
	}

	return nil
}

func (s *Service) RecordProcessingFailure(ctx context.Context, id, lastError string, incrementAttempts bool) error {
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

	tag, err := s.db.Exec(ctx, query, nullableText(lastError), attemptDelta, id, paymentStatusProcessing)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: expected %s for payment %s", errInvalidStateTransition, paymentStatusProcessing, id)
	}

	return err
}

func (s *Service) UpdateProviderRef(ctx context.Context, id, providerRef string) error {
	query := `
		update payments
		set provider_ref = $1,
			updated_at = now()
		where id = $2
		  and status = $3`

	tag, err := s.db.Exec(ctx, query, providerRef, id, paymentStatusSuccess)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: expected %s before provider ref update for payment %s", errInvalidStateTransition, paymentStatusSuccess, id)
	}

	return err
}

func nullableText(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func (s *Service) GetPaymentByID(ctx context.Context, id string) (Payment, error) {
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

func (s *Service) CreatePayment(ctx context.Context, params CreatePaymentParams) (*Payment, error) {
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
		return nil, err
	}

	return payment, nil
}

type APIHandler struct {
	svc *Service
}

type CreatePaymentResponse struct {
	Payment  Payment `json:"payment"`
	Created  bool    `json:"created"`
	Enqueued bool    `json:"enqueued"`
}

type GetPaymentResponse struct {
	Payment Payment `json:"payment"`
}

func WriteJson(w http.ResponseWriter, data any, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

func (s *Service) GetPaymentByIdempotencyKey(ctx context.Context, key string) (Payment, error) {
	query := `
	select id, amount, status, idempotency_key, provider_ref, attempts, last_error, created_at, updated_at
	from payments where idempotency_key = $1
	limit 1`
	var payment Payment

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

func (h *APIHandler) CreatePayment(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var recPayment Payment
	err := json.NewDecoder(r.Body).Decode(&recPayment)
	if err != nil {
		slog.Error("failed to decode create payment request", "error", err)
		http.Error(w, "failed to decode the payment", http.StatusBadRequest)
		return
	}

	payment, err := h.svc.CreatePayment(ctx, CreatePaymentParams{
		Amount:         recPayment.Amount,
		Status:         paymentStatusPending,
		IdempotencyKey: recPayment.IdempotencyKey,
	})

	var pgError *pgconn.PgError
	if err != nil {
		if errors.As(err, &pgError) {
			if pgError.Code == "23505" {
				payment, err := h.svc.GetPaymentByIdempotencyKey(ctx, recPayment.IdempotencyKey)
				if err != nil {
					slog.Error("failed to get payment by idempotency key", "idempotency_key", recPayment.IdempotencyKey, "error", err)
					http.Error(w, "failed to get payment by idempotency key", http.StatusInternalServerError)
					return
				}

				WriteJson(w, CreatePaymentResponse{
					Payment:  payment,
					Created:  false,
					Enqueued: false,
				}, http.StatusOK)
				return
			}
		}
		slog.Error("failed to create payment", "idempotency_key", recPayment.IdempotencyKey, "error", err)
		http.Error(w, "failed to create payment ", http.StatusInternalServerError)
		return
	}

	err = h.svc.rdb.LPush(ctx, "payments:pending", payment.ID.String()).Err()
	if err != nil {
		slog.Error("failed to enqueue payment", "payment_id", payment.ID, "error", err)
		http.Error(w, "failed to push to redis"+err.Error(), http.StatusInternalServerError)
		return
	}
	WriteJson(w, CreatePaymentResponse{
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
		slog.Error("failed to get payment", "payment_id", id, "error", err)
		http.Error(w, "failed to create payment ", http.StatusInternalServerError)
		return
	}
	WriteJson(w, GetPaymentResponse{
		Payment: payment,
	}, http.StatusOK)
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	_ = godotenv.Load()

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	addr := ":" + port

	providerBaseURL := mustGetEnv("PROVIDER_BASE_URL")
	databaseURL := mustGetEnv("DATABASE_URL")
	redisAddr := mustGetEnv("REDIS_ADDR")

	providerClient := ProviderClient{
		baseURL: providerBaseURL,
		http:    &http.Client{},
	}

	g, workerCtx := errgroup.WithContext(sigCtx)

	r := http.NewServeMux()
	dbPool, err := pgxpool.New(sigCtx, databaseURL)
	if err != nil {
		log.Fatalln("failed to connect to db", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	svc := &Service{
		db:             dbPool,
		rdb:            rdb,
		providerClient: &providerClient,
		workers:        workerCount,
	}

	h := &APIHandler{
		svc: svc,
	}

	r.HandleFunc("/payments", h.CreatePayment)
	r.HandleFunc("GET /payments/{id}", h.GetPaymentID)
	server := &http.Server{
		Addr:    addr,
		Handler: r,
		BaseContext: func(_ net.Listener) context.Context {
			return sigCtx
		},
		ReadTimeout:  serverReadTimeout,
		WriteTimeout: serverWriteTimeout,
		IdleTimeout:  serverIdleTimeout,
	}

	g.Go(func() error {
		slog.Info("starting server", "addr", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	g.Go(func() error { return svc.Start(workerCtx) })

	g.Go(func() error {
		return svc.RetryWorker(workerCtx)
	})
	g.Go(func() error {
		return svc.ReaperWorker(workerCtx)
	})

	g.Go(func() error {
		<-sigCtx.Done()
		shutDownCtx, cancel := context.WithTimeout(context.Background(), serverShutdownTimeout)
		defer cancel()

		err = server.Shutdown(shutDownCtx)
		return err
	})
	if err := g.Wait(); err != nil {
		slog.Error("errgroup", "error", err)
	}

	slog.Info("server shut down")
}
