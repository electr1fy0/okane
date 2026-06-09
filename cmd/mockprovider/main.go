package main

import (
	"encoding/json"
	"fmt"
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

func (m *MockProvider) Health(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "don't worry about me, mate")
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if err := godotenv.Load(); err != nil {
		slog.Warn("could not load .env file", "error", err)
	}

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
