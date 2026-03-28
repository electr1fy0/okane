package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"

	"github.com/google/uuid"
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
	case n < 90:
		w.WriteHeader(http.StatusServiceUnavailable)
	default:
		w.WriteHeader(http.StatusUnprocessableEntity)
	}
}

func main() {
	m := MockProvider{}
	http.HandleFunc("/", m.ServeHTTP)
	log.Fatal(http.ListenAndServe(":3000", nil))
}
