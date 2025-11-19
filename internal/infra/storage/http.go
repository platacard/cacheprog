package storage

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/platacard/cacheprog/internal/app/cacheprog"
)

type HTTP struct {
	client       *http.Client
	endpoint     string
	extraHeaders http.Header
}

func NewHTTP(client *http.Client, endpoint string, extraHeaders http.Header) *HTTP {
	return &HTTP{
		client:       http.DefaultClient,
		endpoint:     endpoint,
		extraHeaders: extraHeaders,
	}
}

func ConfigureHTTP(baseURL string, extraHeaders http.Header) (*HTTP, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("base URL is required")
	}

	slog.Info("Configuring HTTP storage with base URL", slog.String("baseURL", baseURL))

	return NewHTTP(http.DefaultClient, baseURL, extraHeaders), nil
}

func (h *HTTP) Get(ctx context.Context, request *cacheprog.GetRequest) (*cacheprog.GetResponse, error) {
	req, err := NewGetRequest(ctx, h.endpoint, request)
	if err != nil {
		return nil, err
	}

	for k, vs := range h.extraHeaders {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}

	resp, err := h.client.Do(req) //nolint:bodyclose // closed in upper layer
	if err != nil {
		return nil, err
	}

	return ParseGetResponse(resp)
}

func (h *HTTP) Put(ctx context.Context, request *cacheprog.PutRequest) (*cacheprog.PutResponse, error) {
	req, err := NewPutRequest(ctx, h.endpoint, request)
	if err != nil {
		return nil, err
	}

	for k, vs := range h.extraHeaders {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}

	resp, err := h.client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	return &cacheprog.PutResponse{}, nil
}
