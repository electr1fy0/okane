package main

import (
	"context"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/electr1fy0/okane/internal/handler"
	"github.com/electr1fy0/okane/internal/queue"
	"github.com/electr1fy0/okane/internal/service"
	"github.com/electr1fy0/okane/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

const (
	serverReadTimeout     = 5 * time.Second
	serverWriteTimeout    = 10 * time.Second
	serverIdleTimeout     = 60 * time.Second
	serverShutdownTimeout = 30 * time.Second
)

func mustGetEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("missing required env var %s", key)
	}
	return value
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

	providerClient := service.NewProviderClient(providerBaseURL, &http.Client{})

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

	paymentStore := store.New(dbPool)
	paymentQueue := queue.NewRedis(rdb)
	svc := service.New(paymentStore, paymentQueue, providerClient, service.WorkerCount)
	h := handler.New(svc)

	r := http.NewServeMux()
	r.HandleFunc("POST /payments", h.CreatePayment)
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

	g, workerCtx := errgroup.WithContext(sigCtx)

	g.Go(func() error {
		slog.Info("starting server", "addr", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})
	g.Go(func() error { return svc.Start(workerCtx) })
	g.Go(func() error { return svc.RetryWorker(workerCtx) })
	g.Go(func() error { return svc.ReaperWorker(workerCtx) })
	g.Go(func() error {
		<-sigCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), serverShutdownTimeout)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	})

	if err := g.Wait(); err != nil {
		slog.Error("errgroup", "error", err)
	}

	slog.Info("server shut down")
}
