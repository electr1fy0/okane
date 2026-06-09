package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/electr1fy0/okane/internal/handler"
	"github.com/electr1fy0/okane/internal/ratelimit"
	"github.com/electr1fy0/okane/internal/service"
	"github.com/electr1fy0/okane/internal/store"
	"github.com/electr1fy0/okane/internal/worker"
	"github.com/hibiken/asynq"
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
	workerCnt             = 5
)

func mustGetEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		slog.Error("missing required env var", "key", key)
		os.Exit(1)
	}
	return value
}

func setupRouter(h *handler.APIHandler) *http.ServeMux {
	r := http.NewServeMux()
	r.HandleFunc("POST /v1/payments", handler.Handle(h.CreatePayment))
	r.HandleFunc("GET /v1/payments/{id}", handler.Handle(h.GetPaymentID))
	r.HandleFunc("POST /v1/payments/{id}/retry", handler.Handle(h.RetryPayment))
	r.HandleFunc("GET /v1/health", h.Health)

	return r
}

type envConfig struct {
	providerBaseURL string
	databaseURL     string
	redisAddr       string
	appPort         string
}

func (e *envConfig) load() {
	if err := godotenv.Load(); err != nil {
		slog.Warn("could not load .env file", "error", err)
	}
	e.providerBaseURL = mustGetEnv("PROVIDER_BASE_URL")
	e.databaseURL = mustGetEnv("DATABASE_URL")
	e.redisAddr = mustGetEnv("REDIS_ADDR")

	e.appPort = os.Getenv("PORT")
	if e.appPort == "" {
		e.appPort = "8080"
	}
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	env := envConfig{}
	env.load()

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dbPool, err := pgxpool.New(sigCtx, env.databaseURL)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}

	paymentStore := store.New(dbPool)

	// mockprovider
	providerClient := service.NewProviderClient(env.providerBaseURL, &http.Client{})

	// asynq client
	redisOpt := asynq.RedisClientOpt{Addr: env.redisAddr}
	ac := asynq.NewClient(redisOpt)

	// service
	svc := service.New(paymentStore, providerClient, ac, redisOpt)
	h := handler.New(svc)

	// asynq server
	workerSrv := worker.NewServer(redisOpt, svc, workerCnt)

	// rate limiter
	rdb := redis.NewClient(&redis.Options{Addr: env.redisAddr})
	limiterStore := ratelimit.NewRedisLimiterStore(rdb)
	rateLimiter := ratelimit.NewRateLimiter(limiterStore, 100, 1*time.Minute)

	// app server
	r := setupRouter(h)
	addr := ":" + env.appPort
	server := &http.Server{
		Addr:    addr,
		Handler: rateLimiter.Middleware(r),
		BaseContext: func(_ net.Listener) context.Context {
			return sigCtx
		},
		ReadTimeout:  serverReadTimeout,
		WriteTimeout: serverWriteTimeout,
		IdleTimeout:  serverIdleTimeout,
	}

	g, _ := errgroup.WithContext(sigCtx)

	g.Go(func() error {
		slog.Info("starting worker", "concurrency", workerCnt)
		return workerSrv.Run()
	})

	g.Go(func() error {
		slog.Info("starting server", "addr", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	g.Go(func() error {
		<-sigCtx.Done()
		ac.Close()
		workerSrv.Shutdown()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), serverShutdownTimeout)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	})

	if err := g.Wait(); err != nil {
		slog.Error("errgroup", "error", err)
	}

	slog.Info("server shut down")
}
