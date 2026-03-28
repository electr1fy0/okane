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

func (s *Service) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for i := range s.workers {
		g.Go(func() error {
			return s.Work(ctx, i)
		})
	}
	return g.Wait()
}

func (s *Service) processPayment(ctx context.Context, paymentID string) error {
	s.UpdatePayment(ctx, paymentID, "processing", "", false)
	maxImmediateRetries := 1
	success := false
	unproccessable := false
retryLoop:
	for i := range maxImmediateRetries {

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
			if err != nil {
				slog.Error("failed to decode provider response", "payment_id", paymentID, "error", err)
				return err
			}
			err = s.UpdatePayment(ctx, paymentID, "success", "", true)
			if err != nil {
				slog.Error("failed to update payment success state", "payment_id", paymentID, "error", err)
				return err
			}
			err = s.UpdateProviderRef(ctx, paymentID, respStruct.ProviderRef)
			if err != nil {
				slog.Error("failed to update provider ref", "payment_id", paymentID, "error", err)
				return err
			}
			success = true
			break retryLoop
		case http.StatusServiceUnavailable:
			err = s.UpdatePayment(ctx, paymentID, "processing", "service unavailable", true)
			if err != nil {
				slog.Error("failed to update retryable payment state", "payment_id", paymentID, "attempt", i, "error", err)
				return err
			}
		case http.StatusUnprocessableEntity:
			err = s.UpdatePayment(ctx, paymentID, "failed", "unprocessable payment", false)
			if err != nil {
				slog.Error("failed to update failed payment state", "payment_id", paymentID, "error", err)
				return err
			}
			unproccessable = true
			break retryLoop
		}
	}

	_ = s.rdb.LRem(ctx, "payments:processing", 1, paymentID).Err()
	_ = s.rdb.HDel(ctx, "payments:processing:times", paymentID).Err()

	if success || unproccessable {
		return nil
	}

	backoff := 10 * time.Second
	retryAt := time.Now().Add(backoff).Unix()

	s.rdb.ZAdd(ctx, "payments:delayed", redis.Z{
		Score:  float64(retryAt),
		Member: paymentID,
	})

	return nil
}

func (s *Service) Work(ctx context.Context, id int) error {
	for {
		paymentID, err := s.rdb.BLMove(ctx, "payments:pending", "payments:processing", "RIGHT", "LEFT", 1*time.Second).Result()

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

		payment, err := s.GetPaymentByID(ctx, paymentID)
		if err != nil {
			slog.Error("failed to load payment", "payment_id", paymentID, "worker_id", id, "error", err)
			return err
		}
		if payment.Attempts >= 8 {
			s.rdb.LRem(ctx, "payments:processing", 1, paymentID)
			s.rdb.HDel(ctx, "payments:processing:times", paymentID)
			s.rdb.LPush(ctx, "payments:dead", paymentID)
			lastError := ""
			if payment.LastError != nil {
				lastError = *payment.LastError
			}
			err = s.UpdatePayment(ctx, paymentID, "failed", lastError, false)
			if err != nil {
				slog.Error("failed to update dead-lettered payment", "payment_id", paymentID, "error", err)
				return err
			}

			continue
		}

		err = s.processPayment(context.WithoutCancel(ctx), paymentID)
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
	for {
		select {
		case <-time.After(10 * time.Second):
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
				if time.Since(time.Unix(tsInt, 0)) < time.Minute {
					continue
				}
			}

			s.rdb.LRem(ctx, "payments:processing", 1, paymentID)
			s.rdb.HDel(ctx, "payments:processing:times", paymentID)
			s.rdb.LPush(ctx, "payments:pending", paymentID)
		}
	}
}

func (s *Service) RetryWorker(ctx context.Context) error {
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
			case <-time.After(1 * time.Second):
				continue
			}
		}
		now := float64(time.Now().Unix())
		if jobs[0].Score > now {
			select {
			case <-time.After(time.Duration(jobs[0].Score-now) * time.Second):
				continue
			case <-ctx.Done():
				return err
			}
		}

		s.rdb.LPush(ctx, "payments:pending", jobs[0].Member)
		s.rdb.ZRem(ctx, "payments:delayed", jobs[0].Member)
	}
}

func (s *Service) UpdatePayment(ctx context.Context, id, status, lastError string, incrementAttempts bool) error {
	query := `
		update payments
		set status = $1,
			last_error = $2,
			attempts = attempts + $3,
			updated_at = now()
		where id = $4`

	attemptDelta := 0
	if incrementAttempts {
		attemptDelta = 1
	}

	_, err := s.db.Exec(ctx, query, status, lastError, attemptDelta, id)

	return err
}

func (s *Service) UpdateProviderRef(ctx context.Context, id, providerRef string) error {
	query := `
		update payments
		set provider_ref = $1,
			updated_at = now()
		where id = $2`

	_, err := s.db.Exec(ctx, query, providerRef, id)

	return err
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
		Status:         "pending",
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

	providerClient := ProviderClient{
		baseURL: "http://localhost:3000",
		http:    &http.Client{},
	}

	g, workerCtx := errgroup.WithContext(sigCtx)

	r := http.NewServeMux()
	dbPool, err := pgxpool.New(sigCtx, "postgresql://ayush:ayush@localhost:5432/okanedb")
	if err != nil {
		log.Fatalln("failed to connect to db", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     ":6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	svc := &Service{
		db:             dbPool,
		rdb:            rdb,
		providerClient: &providerClient,
		workers:        5,
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
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
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
		fmt.Print("sigctx done")
		shutDownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = server.Shutdown(shutDownCtx)
		fmt.Println(err)
		return err
	})
	if err := g.Wait(); err != nil {
		slog.Error("errgroup", "error", err)
	}

	slog.Info("server shut down")
}
