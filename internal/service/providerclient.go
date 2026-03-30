package service

import (
	"net/http"
)

type ProviderClient struct {
	baseURL string
	http    *http.Client
}

func NewProviderClient(baseURL string, client *http.Client) *ProviderClient {
	return &ProviderClient{
		baseURL: baseURL,
		http:    client,
	}
}
