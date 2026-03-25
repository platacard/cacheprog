package cacheprog

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestRemoteStorageCircuitBreaker_Get(t *testing.T) {
	t.Run("disabled after errors threshold is reached", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		remoteStorage := NewMockRemoteStorage(ctrl)
		expectedError := errors.New("test error")
		remoteStorage.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, expectedError).AnyTimes()

		const maxConsecutiveErrors = 10
		breaker := NewRemoteStorageCircuitBreaker(remoteStorage, maxConsecutiveErrors, 0)
		for range maxConsecutiveErrors - 1 {
			_, err := breaker.Get(context.Background(), &GetRequest{
				ActionID: []byte("test"),
			})
			require.ErrorIs(t, err, expectedError)
		}

		_, err := breaker.Get(context.Background(), &GetRequest{
			ActionID: []byte("test"),
		})
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("'Not found' error is not counted as error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		remoteStorage := NewMockRemoteStorage(ctrl)
		remoteStorage.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, ErrNotFound).Times(1)
		expectedError := errors.New("test error")
		remoteStorage.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, expectedError).AnyTimes()

		const maxConsecutiveErrors = 10
		breaker := NewRemoteStorageCircuitBreaker(remoteStorage, maxConsecutiveErrors, 0)

		// first get should return 'not found'
		_, err := breaker.Get(context.Background(), &GetRequest{
			ActionID: []byte("test"),
		})
		require.ErrorIs(t, err, ErrNotFound)

		// reach errors threshold
		for range maxConsecutiveErrors - 1 {
			_, err := breaker.Get(context.Background(), &GetRequest{
				ActionID: []byte("test"),
			})
			require.ErrorIs(t, err, expectedError)
		}

		// next get should return 'not found'
		_, err = breaker.Get(context.Background(), &GetRequest{
			ActionID: []byte("test"),
		})
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("success call resets error counter", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		remoteStorage := NewMockRemoteStorage(ctrl)

		const maxConsecutiveErrors = 10
		expectedError := errors.New("test error")
		remoteStorage.EXPECT().Get(gomock.Any(), &GetRequest{ActionID: []byte("test")}).Return(nil, expectedError).Times(maxConsecutiveErrors - 1)
		remoteStorage.EXPECT().Get(gomock.Any(), &GetRequest{ActionID: []byte("test")}).Return(&GetResponse{}, nil).Times(1)
		remoteStorage.EXPECT().Get(gomock.Any(), &GetRequest{ActionID: []byte("test1")}).Return(nil, expectedError).AnyTimes()

		breaker := NewRemoteStorageCircuitBreaker(remoteStorage, maxConsecutiveErrors, 0)

		for range maxConsecutiveErrors - 1 {
			_, err := breaker.Get(context.Background(), &GetRequest{
				ActionID: []byte("test"),
			})
			require.ErrorIs(t, err, expectedError)
		}

		_, err := breaker.Get(context.Background(), &GetRequest{
			ActionID: []byte("test"),
		})
		require.NoError(t, err)

		// check that error counter is reset

		for range maxConsecutiveErrors - 1 {
			_, err := breaker.Get(context.Background(), &GetRequest{
				ActionID: []byte("test1"),
			})
			require.ErrorIs(t, err, expectedError)
		}

		_, err = breaker.Get(context.Background(), &GetRequest{
			ActionID: []byte("test1"),
		})
		require.ErrorIs(t, err, ErrNotFound)
	})
}

func TestRemoteStorageCircuitBreaker_Put(t *testing.T) {
	t.Run("disabled after errors threshold is reached", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		remoteStorage := NewMockRemoteStorage(ctrl)
		expectedError := errors.New("test error")
		remoteStorage.EXPECT().Put(gomock.Any(), gomock.Any()).Return(nil, expectedError).AnyTimes()

		const maxConsecutiveErrors = 10
		breaker := NewRemoteStorageCircuitBreaker(remoteStorage, maxConsecutiveErrors, 0)
		for range maxConsecutiveErrors - 1 {
			_, err := breaker.Put(context.Background(), &PutRequest{
				ActionID: []byte("test"),
			})
			require.ErrorIs(t, err, expectedError)
		}

		_, err := breaker.Put(context.Background(), &PutRequest{
			ActionID: []byte("test"),
		})
		require.NoError(t, err)
	})

	t.Run("success call resets error counter", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		remoteStorage := NewMockRemoteStorage(ctrl)

		const maxConsecutiveErrors = 10
		expectedError := errors.New("test error")
		remoteStorage.EXPECT().Put(gomock.Any(), &PutRequest{ActionID: []byte("test")}).Return(nil, expectedError).Times(maxConsecutiveErrors - 1)
		remoteStorage.EXPECT().Put(gomock.Any(), &PutRequest{ActionID: []byte("test")}).Return(&PutResponse{}, nil).Times(1)
		remoteStorage.EXPECT().Put(gomock.Any(), &PutRequest{ActionID: []byte("test1")}).Return(nil, expectedError).AnyTimes()

		breaker := NewRemoteStorageCircuitBreaker(remoteStorage, maxConsecutiveErrors, 0)

		for range maxConsecutiveErrors - 1 {
			_, err := breaker.Put(context.Background(), &PutRequest{
				ActionID: []byte("test"),
			})
			require.ErrorIs(t, err, expectedError)
		}

		_, err := breaker.Put(context.Background(), &PutRequest{
			ActionID: []byte("test"),
		})
		require.NoError(t, err)

		// check that error counter is reset

		for range maxConsecutiveErrors - 1 {
			_, err := breaker.Put(context.Background(), &PutRequest{
				ActionID: []byte("test1"),
			})
			require.ErrorIs(t, err, expectedError)
		}

		_, err = breaker.Put(context.Background(), &PutRequest{
			ActionID: []byte("test1"),
		})
		require.NoError(t, err)
	})
}

func TestRemoteStorageCircuitBreaker_HalfOpen(t *testing.T) {
	t.Run("Get recovers after retryAfter elapses", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		remoteStorage := NewMockRemoteStorage(ctrl)

		const maxConsecutiveErrors = 3
		retryAfter := 30 * time.Second
		breaker := NewRemoteStorageCircuitBreaker(remoteStorage, maxConsecutiveErrors, retryAfter)

		now := time.Now()
		breaker.now = func() time.Time { return now }

		// Trip the circuit breaker
		expectedError := errors.New("test error")
		remoteStorage.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, expectedError).Times(maxConsecutiveErrors)

		for range maxConsecutiveErrors {
			breaker.Get(context.Background(), &GetRequest{ActionID: []byte("test")})
		}

		// Circuit is open — should return ErrNotFound without calling upstream
		_, err := breaker.Get(context.Background(), &GetRequest{ActionID: []byte("test")})
		require.ErrorIs(t, err, ErrNotFound)

		// Advance time past retryAfter — next call should probe upstream
		now = now.Add(retryAfter)
		remoteStorage.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&GetResponse{}, nil).Times(1)

		resp, err := breaker.Get(context.Background(), &GetRequest{ActionID: []byte("test")})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Circuit should be closed — subsequent calls go through
		remoteStorage.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&GetResponse{}, nil).Times(1)
		_, err = breaker.Get(context.Background(), &GetRequest{ActionID: []byte("test")})
		require.NoError(t, err)
	})

	t.Run("Get probe failure keeps circuit open", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		remoteStorage := NewMockRemoteStorage(ctrl)

		const maxConsecutiveErrors = 3
		retryAfter := 30 * time.Second
		breaker := NewRemoteStorageCircuitBreaker(remoteStorage, maxConsecutiveErrors, retryAfter)

		now := time.Now()
		breaker.now = func() time.Time { return now }

		// Trip the circuit breaker
		expectedError := errors.New("test error")
		remoteStorage.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, expectedError).Times(maxConsecutiveErrors)
		for range maxConsecutiveErrors {
			breaker.Get(context.Background(), &GetRequest{ActionID: []byte("test")})
		}

		// Advance time and probe — but upstream still fails
		now = now.Add(retryAfter)
		remoteStorage.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, expectedError).Times(1)
		_, err := breaker.Get(context.Background(), &GetRequest{ActionID: []byte("test")})
		require.ErrorIs(t, err, ErrNotFound)

		// Circuit should still be open — no upstream call before retryAfter
		_, err = breaker.Get(context.Background(), &GetRequest{ActionID: []byte("test")})
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("Put recovers after retryAfter elapses", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		remoteStorage := NewMockRemoteStorage(ctrl)

		const maxConsecutiveErrors = 3
		retryAfter := 30 * time.Second
		breaker := NewRemoteStorageCircuitBreaker(remoteStorage, maxConsecutiveErrors, retryAfter)

		now := time.Now()
		breaker.now = func() time.Time { return now }

		// Trip the circuit breaker
		expectedError := errors.New("test error")
		remoteStorage.EXPECT().Put(gomock.Any(), gomock.Any()).Return(nil, expectedError).Times(maxConsecutiveErrors)
		for range maxConsecutiveErrors {
			breaker.Put(context.Background(), &PutRequest{ActionID: []byte("test")})
		}

		// Circuit is open — should discard
		_, err := breaker.Put(context.Background(), &PutRequest{ActionID: []byte("test"), Body: bytes.NewReader(nil)})
		require.NoError(t, err)

		// Advance time past retryAfter — probe should succeed
		now = now.Add(retryAfter)
		remoteStorage.EXPECT().Put(gomock.Any(), gomock.Any()).Return(&PutResponse{}, nil).Times(1)
		_, err = breaker.Put(context.Background(), &PutRequest{ActionID: []byte("test")})
		require.NoError(t, err)

		// Circuit should be closed
		remoteStorage.EXPECT().Put(gomock.Any(), gomock.Any()).Return(&PutResponse{}, nil).Times(1)
		_, err = breaker.Put(context.Background(), &PutRequest{ActionID: []byte("test")})
		require.NoError(t, err)
	})

	t.Run("no recovery when retryAfter is zero", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		remoteStorage := NewMockRemoteStorage(ctrl)

		const maxConsecutiveErrors = 3
		breaker := NewRemoteStorageCircuitBreaker(remoteStorage, maxConsecutiveErrors, 0)

		// Trip the circuit breaker
		expectedError := errors.New("test error")
		remoteStorage.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, expectedError).Times(maxConsecutiveErrors)
		for range maxConsecutiveErrors {
			breaker.Get(context.Background(), &GetRequest{ActionID: []byte("test")})
		}

		// Circuit stays permanently open — no probe even with default time
		_, err := breaker.Get(context.Background(), &GetRequest{ActionID: []byte("test")})
		require.ErrorIs(t, err, ErrNotFound)
	})
}
