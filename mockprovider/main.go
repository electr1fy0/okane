package main

import (
	"encoding/json"
	"log/slog"
	"math/rand"
	"net/http"
	"os"

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

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	_ = godotenv.Load()

	port := os.Getenv("MOCK_PROVIDER_PORT")
	if port == "" {
		port = "3000"
	}

	m := MockProvider{}
	http.HandleFunc("/", m.ServeHTTP)
	slog.Info("starting mock provider server", "port", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}
