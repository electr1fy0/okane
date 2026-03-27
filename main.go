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
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
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
			slog.Info("Starting worker", "id", i)
			return s.Work(ctx, i)
		})
	}
	return g.Wait()
}

func (s *Service) Work(ctx context.Context, id int) error {
	for {
		paymentID, err := s.rdb.BLMove(ctx, "main_queue", "processing_queue", "RIGHT", "LEFT", 2*time.Second).Result()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if err == redis.Nil {
				continue
			}
			return err
		}
		slog.Info("working", "payment id", paymentID, "workerid", id)
		s.UpdatePaymentStatus(ctx, paymentID, "processing")

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
				slog.Info("payment succcess", "id", paymentID)
				s.UpdatePaymentStatus(ctx, paymentID, "success")
				success = true
				break retryLoop
			case http.StatusServiceUnavailable:
				slog.Info("service unavailable, should retry later", "id", paymentID, "attempt", i)
				continue
			case http.StatusUnprocessableEntity:
				slog.Info("unprocessable payment, shouldn't retry this")
				unproccessable = true
				break retryLoop
			}
		}
		err = s.rdb.LRem(ctx, "processing_queue", 1, paymentID).Err()
		if err != nil {
			slog.Error("failed to remove req from processing queue", "error", err)
		}
		if success || unproccessable {
			continue
		}

		backoff := 10 * time.Second
		retryAt := time.Now().Add(backoff).Unix()

		s.rdb.ZAdd(ctx, "delayed_queue", redis.Z{
			Score:  float64(retryAt),
			Member: paymentID,
		})
	}
}

type ProviderClient struct {
	baseURL string
	http    *http.Client
}

func (s *Service) RetryWorker(ctx context.Context) error {
	for {
		jobs, err := s.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
			Key:     "delayed_queue",
			ByScore: true,
			Start:   "-inf",
			Stop:    "+inf",

			Offset: 0,
			Count:  1,
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
		slog.Info("retrying: ", "id", jobs[0].Member)
		now := float64(time.Now().Unix())
		if jobs[0].Score > now {
			select {
			case <-time.After(time.Duration(jobs[0].Score-now) * time.Second):
				continue
			case <-ctx.Done():
				return err
			}
		}

		s.rdb.LPush(ctx, "main_queue", jobs[0].Member)
		s.rdb.ZRem(ctx, "delayed_queue", jobs[0].Member)
	}
}

func (s *Service) UpdatePaymentStatus(ctx context.Context, id string, status string) error {
	query := `
		update payments set status = $1 where id = $2`

	_, err := s.db.Exec(ctx, query, status, id)

	return err
}

func (s *Service) GetPaymentByID(ctx context.Context, id string) (Payment, error) {
	query := `
	select id, amount, status, idempotency_key, created_at from payments where id = $1;
	`
	payment := Payment{}

	err := s.db.QueryRow(ctx, query, id).Scan(&payment.ID, &payment.Amount, &payment.Status, &payment.IdempotencyKey, &payment.CreatedAt)
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

func WriteJson(w http.ResponseWriter, data any, statusCode int) {
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

func (s *Service) GetPaymentByIdempotencyKey(ctx context.Context, key string) (Payment, error) {
	query := `
	select id, amount, status, idempotency_key
	from payments where idempotency_key = $1
	limit 1`
	var payment Payment

	err := s.db.QueryRow(ctx, query, key).Scan(&payment.ID, &payment.Amount, &payment.Status, &payment.IdempotencyKey)
	return payment, err
}

func (h *APIHandler) CreatePayment(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var recPayment Payment
	err := json.NewDecoder(r.Body).Decode(&recPayment)
	if err != nil {
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
			slog.Warn("mapped to pgerror", "code", pgError.Code)

			if pgError.Code == "23505" {
				payment, err := h.svc.GetPaymentByIdempotencyKey(ctx, recPayment.IdempotencyKey)
				if err != nil {
					http.Error(w, "failed to get payment by idempotency key", http.StatusInternalServerError)
					return
				}

				WriteJson(w, payment, http.StatusOK)
				return
			}
		}
		http.Error(w, "failed to create payment ", http.StatusInternalServerError)
		return
	}

	err = h.svc.rdb.LPush(ctx, "main_queue", payment.ID.String()).Err()
	if err != nil {
		http.Error(w, "failed to push to redis"+err.Error(), http.StatusInternalServerError)
		return
	}

	WriteJson(w, payment, http.StatusAccepted)
}

func (h *APIHandler) GetPaymentID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "failed to get payment id", http.StatusBadRequest)
		return
	}

	payment, err := h.svc.GetPaymentByID(r.Context(), id)
	if err != nil {
		http.Error(w, "failed to create payment ", http.StatusInternalServerError)
		return
	}
	WriteJson(w, payment, http.StatusOK)
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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
	server := &http.Server{
		Addr:    ":8080",
		Handler: r,
		BaseContext: func(_ net.Listener) context.Context {
			return sigCtx
		},
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	g.Go(func() error {
		slog.Info("starting server on :8080")
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
