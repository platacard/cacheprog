package cacheprog

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/platacard/cacheprog/internal/infra/logging"
)

type RemoteStorageCircuitBreaker struct {
	upstream             RemoteStorage
	maxConsecutiveErrors int64
	retryAfter           time.Duration

	currentConsecutiveErrors atomic.Int64
	lastTrippedNano          atomic.Int64
}

func NewRemoteStorageCircuitBreaker(upstream RemoteStorage, maxConsecutiveErrors int64, retryAfter time.Duration) *RemoteStorageCircuitBreaker {
	return &RemoteStorageCircuitBreaker{
		upstream:             upstream,
		maxConsecutiveErrors: maxConsecutiveErrors,
		retryAfter:           retryAfter,
	}
}

// isOpen reports whether the circuit is open (tripped).
// If a retryAfter duration is configured and enough time has passed since the last trip,
// it allows a single probe request through by resetting the trip timestamp.
func (b *RemoteStorageCircuitBreaker) isOpen() bool {
	if b.currentConsecutiveErrors.Load() < b.maxConsecutiveErrors {
		return false
	}

	if b.retryAfter <= 0 {
		return true
	}

	lastTripped := b.lastTrippedNano.Load()
	now := time.Now().UnixNano()
	if now-lastTripped >= b.retryAfter.Nanoseconds() {
		// Use CompareAndSwap so only one concurrent caller wins the probe
		if b.lastTrippedNano.CompareAndSwap(lastTripped, now) {
			slog.Info("Circuit breaker half-open: allowing probe request")
			return false
		}
	}

	return true
}

func (b *RemoteStorageCircuitBreaker) trip(ctx context.Context, err error) {
	b.lastTrippedNano.Store(time.Now().UnixNano())
	// this may be logged multiple times because of concurrent requests but it's not a problem
	slog.ErrorContext(ctx, "Remote storage has been disabled because of too many consecutive errors", logging.Error(err))
}

func (b *RemoteStorageCircuitBreaker) Get(ctx context.Context, request *GetRequest) (*GetResponse, error) {
	if b.isOpen() {
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
		b.trip(ctx, err)
		return nil, ErrNotFound
	}

	return nil, err
}

func (b *RemoteStorageCircuitBreaker) Put(ctx context.Context, request *PutRequest) (*PutResponse, error) {
	if b.isOpen() {
		_, _ = io.Copy(io.Discard, request.Body)
		return &PutResponse{}, nil // just discard the request
	}

	resp, err := b.upstream.Put(ctx, request)
	if err == nil {
		b.currentConsecutiveErrors.Store(0)
		return resp, nil
	}

	if b.currentConsecutiveErrors.Add(1) >= b.maxConsecutiveErrors {
		b.trip(ctx, err)
		return &PutResponse{}, nil
	}

	return nil, err
}
