package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMockProvider_Health(t *testing.T) {
	m := MockProvider{}
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	m.Health(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "don't worry about me, mate", w.Body.String())
}

func TestMockProvider_ServeHTTP(t *testing.T) {
	m := MockProvider{}

	for range 20 {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		w := httptest.NewRecorder()

		m.ServeHTTP(w, req)

		assert.Contains(t, []int{
			http.StatusOK,
			http.StatusServiceUnavailable,
			http.StatusUnprocessableEntity,
		}, w.Code, "unexpected status code %d", w.Code)

		if w.Code == http.StatusOK {
			var body map[string]string
			err := json.NewDecoder(w.Body).Decode(&body)
			require.NoError(t, err)
			assert.NotEmpty(t, body["provider_ref"])
		}
	}
}

func TestMockProvider_ServeHTTP_OK(t *testing.T) {
	m := MockProvider{}

	okCount, unavailableCount, unprocessableCount := 0, 0, 0
	for range 100 {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		w := httptest.NewRecorder()

		m.ServeHTTP(w, req)

		switch w.Code {
		case http.StatusOK:
			okCount++
		case http.StatusServiceUnavailable:
			unavailableCount++
		case http.StatusUnprocessableEntity:
			unprocessableCount++
		}
	}

	assert.Greater(t, okCount, 0, "expected at least one 200 OK response")
	assert.Greater(t, unavailableCount, 0, "expected at least one 503 response")
	assert.Greater(t, unprocessableCount, 0, "expected at least one 422 response")
}

func TestMockProvider_ServeHTTP_OK_BodyFormat(t *testing.T) {
	m := MockProvider{}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	for range 20 {
		w = httptest.NewRecorder()
		m.ServeHTTP(w, req)
		if w.Code == http.StatusOK {
			break
		}
	}

	if w.Code == http.StatusOK {
		var body map[string]string
		err := json.NewDecoder(w.Body).Decode(&body)
		require.NoError(t, err)
		assert.Contains(t, body["provider_ref"], "-", "provider_ref should be a UUID")
	}
}
