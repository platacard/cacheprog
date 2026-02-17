package cacheprog

import (
	"context"
	"errors"
	"testing"

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
		breaker := NewRemoteStorageCircuitBreaker(remoteStorage, maxConsecutiveErrors)
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
		breaker := NewRemoteStorageCircuitBreaker(remoteStorage, maxConsecutiveErrors)

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

		breaker := NewRemoteStorageCircuitBreaker(remoteStorage, maxConsecutiveErrors)

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
		breaker := NewRemoteStorageCircuitBreaker(remoteStorage, maxConsecutiveErrors)
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

		breaker := NewRemoteStorageCircuitBreaker(remoteStorage, maxConsecutiveErrors)

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
