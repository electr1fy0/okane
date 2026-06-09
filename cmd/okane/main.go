package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
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

type envConfig struct {
	providerBaseURL string
	databaseURL     string
	redisAddr       string
	appPort         string
	logLevel        string
	logFilePath     string
}

func main() {
	env := envConfig{}
	env.load()

	logCloser := initLogger(&env)
	if logCloser != nil {
		defer logCloser.Close()
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dbPool, err := pgxpool.New(sigCtx, env.databaseURL)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer dbPool.Close()

	paymentStore := store.New(dbPool)
	providerClient := service.NewProviderClient(env.providerBaseURL, &http.Client{})

	redisOpt := asynq.RedisClientOpt{Addr: env.redisAddr}
	asynqClient := asynq.NewClient(redisOpt)
	defer asynqClient.Close()

	svc := service.New(paymentStore, providerClient, asynqClient, redisOpt)
	apiHandler := handler.New(svc)

	workerSrv := worker.NewServer(redisOpt, svc, workerCnt)
	rateLimiter := initRateLimiter(env.redisAddr)

	router := setupRouter(apiHandler)
	addr := ":" + env.appPort
	server := &http.Server{
		Addr:    addr,
		Handler: handler.LoggingMiddleware(rateLimiter.Middleware(router)),
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
		slog.Info("shutting down worker and server gracefully...")
		workerSrv.Shutdown()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), serverShutdownTimeout)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	})

	if err := g.Wait(); err != nil {
		slog.Error("errgroup execution error", "error", err)
	}

	slog.Info("server shut down completely")
}

func initLogger(env *envConfig) io.Closer {
	logLevel := slog.LevelInfo
	if env.logLevel == "DEBUG" {
		logLevel = slog.LevelDebug
	}

	var logWriter io.Writer = os.Stdout
	var logFile io.Closer

	if env.logFilePath != "" {
		dir := filepath.Dir(env.logFilePath)
		if err := os.MkdirAll(dir, 0755); err == nil {
			if file, err := os.OpenFile(env.logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err == nil {
				logWriter = io.MultiWriter(os.Stdout, file)
				logFile = file
			} else {
				slog.Error("failed to open log file", "path", env.logFilePath, "error", err)
			}
		} else {
			slog.Error("failed to create log directory", "path", dir, "error", err)
		}
	}

	logger := slog.New(slog.NewJSONHandler(logWriter, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	return logFile
}

func initRateLimiter(redisAddr string) *ratelimit.RateLimiter {
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	limiterStore := ratelimit.NewRedisLimiterStore(rdb)
	return ratelimit.NewRateLimiter(limiterStore, 100, 1*time.Minute)
}

func setupRouter(h *handler.APIHandler) *http.ServeMux {
	r := http.NewServeMux()
	r.HandleFunc("POST /v1/payments", handler.Handle(h.CreatePayment))
	r.HandleFunc("GET /v1/payments/{id}", handler.Handle(h.GetPaymentID))
	r.HandleFunc("POST /v1/payments/{id}/retry", handler.Handle(h.RetryPayment))
	r.HandleFunc("GET /v1/health", h.Health)
	return r
}

func mustGetEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		slog.Error("missing required env var", "key", key)
		os.Exit(1)
	}
	return value
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
	e.logLevel = os.Getenv("LOG_LEVEL")
	e.logFilePath = os.Getenv("LOG_FILE_PATH")
}
