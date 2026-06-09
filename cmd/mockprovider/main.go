package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

type MockProvider struct{}

func (m *MockProvider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := rand.Intn(100)
	switch {
	case n < 80:
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"provider_ref": uuid.NewString(),
		})
		slog.Info("mock provider response", "status", http.StatusOK)
	case n < 90:
		w.WriteHeader(http.StatusServiceUnavailable)
		slog.Info("mock provider response", "status", http.StatusServiceUnavailable)
	default:
		w.WriteHeader(http.StatusUnprocessableEntity)
		slog.Info("mock provider response", "status", http.StatusUnprocessableEntity)
	}
}

func (m *MockProvider) Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"message": "don't worry about me, mate"})
}

func main() {
	if err := godotenv.Load(); err != nil {
		slog.Warn("could not load .env file", "error", err)
	}

	logLevel := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "DEBUG" {
		logLevel = slog.LevelDebug
	}

	var logWriter io.Writer = os.Stdout
	logFilePath := os.Getenv("LOG_FILE_PATH")
	if logFilePath != "" {
		if err := os.MkdirAll(filepath.Dir(logFilePath), 0755); err == nil {
			if file, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err == nil {
				logWriter = io.MultiWriter(os.Stdout, file)
			} else {
				slog.Error("failed to open log file", "path", logFilePath, "error", err)
			}
		} else {
			slog.Error("failed to create log directory", "path", filepath.Dir(logFilePath), "error", err)
		}
	}

	logger := slog.New(slog.NewJSONHandler(logWriter, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	port := os.Getenv("MOCK_PROVIDER_PORT")
	if port == "" {
		port = "3000"
	}

	m := MockProvider{}
	http.HandleFunc("/", m.ServeHTTP)
	http.HandleFunc("/health", m.Health)
	slog.Info("starting mock provider server", "port", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}
