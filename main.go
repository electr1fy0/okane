package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
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
	Amount         int64
	Status         string
	IdempotencyKey string
}

type Service struct {
	db  *pgxpool.Pool
	rdb *redis.Client
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

func (h *APIHandler) CreatePayment(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	payment, err := h.svc.CreatePayment(ctx, CreatePaymentParams{
		Amount:         100,
		Status:         "pending",
		IdempotencyKey: uuid.NewString(),
	})
	if err != nil {
		http.Error(w, "failed to create payment ", http.StatusInternalServerError)
		return
	}
	err = h.svc.rdb.LPush(ctx, "main_queue", payment.ID.String()).Err()
	if err != nil {
		http.Error(w, "failed to push to redis"+err.Error(), http.StatusInternalServerError)
		return
	}

	WriteJson(w, payment, http.StatusAccepted)
	// w.Write([]byte("meow"))
}

func process(paymentId string) {
	fmt.Println("processing: ", paymentId)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	r := http.NewServeMux()
	dbPool, err := pgxpool.New(ctx, "postgresql://ayush:ayush@localhost:5432/okanedb")
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
		db:  dbPool,
		rdb: rdb,
	}

	h := &APIHandler{
		svc: svc,
	}

	r.HandleFunc("/payments", h.CreatePayment)
	server := &http.Server{
		Addr:    ":8080",
		Handler: r,
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Println("starting server on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalln("server crashed:", err)
		}
	}()

	go func() {
		for {
			res, err := rdb.BLMove(ctx, "main_queue", "processing_queue", "RIGHT", "LEFT", 0).Result()
			if err != nil {
				continue
			}
			process(res)
		}
	}()
	<-ctx.Done()
	stop()
	shutDownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = server.Shutdown(shutDownCtx)
	if err != nil {
		log.Println("failed to wait for ongoing reqs to finish")
	}

	log.Println("server shut down")

}
