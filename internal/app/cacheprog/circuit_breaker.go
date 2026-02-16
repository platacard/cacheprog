package cacheprog

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"

	"github.com/platacard/cacheprog/internal/infra/logging"
)

type RemoteStorageCircuitBreaker struct {
	upstream             RemoteStorage
	maxConsecutiveErrors int64

	currentConsecutiveErrors atomic.Int64
}

func NewRemoteStorageCircuitBreaker(upstream RemoteStorage, maxConsecutiveErrors int64) *RemoteStorageCircuitBreaker {
	return &RemoteStorageCircuitBreaker{
		upstream:             upstream,
		maxConsecutiveErrors: maxConsecutiveErrors,
	}
}

func (b *RemoteStorageCircuitBreaker) Get(ctx context.Context, request *GetRequest) (*GetResponse, error) {
	if b.currentConsecutiveErrors.Load() >= b.maxConsecutiveErrors {
		return nil, ErrNotFound // report as "not found" to avoid logging
	}

	resp, err := b.upstream.Get(ctx, request)
	switch {
	case errors.Is(err, nil):
		b.currentConsecutiveErrors.Store(0)
		return resp, nil
	case errors.Is(err, ErrNotFound):
		// do not touch error counter because it's expected error but may be a problem with upstream
		return nil, err
	}

	if b.currentConsecutiveErrors.Add(1) >= b.maxConsecutiveErrors {
		// this may be logged multiple times because of concurrent requests but it's not a problem
		slog.ErrorContext(ctx, "Remote storage has been disabled because of too many consecutive errors", logging.Error(err))
		return nil, ErrNotFound
	}

	return nil, err
}

func (b *RemoteStorageCircuitBreaker) Put(ctx context.Context, request *PutRequest) (*PutResponse, error) {
	if b.currentConsecutiveErrors.Load() >= b.maxConsecutiveErrors {
		_, _ = io.Copy(io.Discard, request.Body)
		return &PutResponse{}, nil // just discard the request
	}

	resp, err := b.upstream.Put(ctx, request)
	if err == nil {
		b.currentConsecutiveErrors.Store(0)
		return resp, nil
	}

	if b.currentConsecutiveErrors.Add(1) >= b.maxConsecutiveErrors {
		slog.ErrorContext(ctx, "Remote storage has been disabled because of too many consecutive errors", logging.Error(err))
		return &PutResponse{}, nil
	}

	return nil, err
}
