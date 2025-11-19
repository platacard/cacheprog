package cacheprog

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/platacard/cacheprog/internal/infra/cacheproto"
)

type mockResponseWriter struct {
	mu        sync.Mutex
	responses []*cacheproto.Response
}

func (w *mockResponseWriter) WriteResponse(resp *cacheproto.Response) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.responses = append(w.responses, resp)
	return nil
}

type mockReadSeekCloser struct {
	*bytes.Reader
}

func (*mockReadSeekCloser) Close() error {
	return nil
}

func TestHandler_Handle_Get(t *testing.T) {
	ctx := context.Background()
	actionID := []byte("test-action")
	outputID := []byte("test-output")
	modTime := time.Now()
	size := int64(100)

	t.Run("local cache hit", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		localStorage := NewMockLocalStorage(ctrl)
		remoteStorage := NewMockRemoteStorage(ctrl)
		compressionCodec := NewMockCompressionCodec(ctrl)
		writer := &mockResponseWriter{}

		h := NewHandler(HandlerOptions{
			RemoteStorage:    remoteStorage,
			LocalStorage:     localStorage,
			CompressionCodec: compressionCodec,
		})

		localStorage.EXPECT().
			GetLocal(gomock.Any(), &LocalGetRequest{ActionID: actionID}).
			Return(&LocalGetResponse{
				OutputID: outputID,
				ModTime:  modTime,
				Size:     size,
				DiskPath: "/tmp/cache/123",
			}, nil)

		h.Handle(ctx, writer, &cacheproto.Request{
			ID:       1,
			Command:  cacheproto.CmdGet,
			ActionID: actionID,
		})

		require.Len(t, writer.responses, 1)
		resp := writer.responses[0]
		assert.Equal(t, int64(1), resp.ID)
		assert.Equal(t, outputID, resp.OutputID)
		assert.Equal(t, size, resp.Size)
		assert.Equal(t, "/tmp/cache/123", resp.DiskPath)
		assert.Equal(t, modTime.Unix(), resp.Time.Unix())
	})

	t.Run("local miss, remote hit", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		localStorage := NewMockLocalStorage(ctrl)
		remoteStorage := NewMockRemoteStorage(ctrl)
		compressionCodec := NewMockCompressionCodec(ctrl)
		writer := &mockResponseWriter{}

		h := NewHandler(HandlerOptions{
			RemoteStorage:    remoteStorage,
			LocalStorage:     localStorage,
			CompressionCodec: compressionCodec,
		})

		localStorage.EXPECT().
			GetLocal(gomock.Any(), &LocalGetRequest{ActionID: actionID}).
			Return(nil, ErrNotFound)

		compressedBody := io.NopCloser(bytes.NewReader([]byte("compressed-data")))
		decompressedBody := io.NopCloser(bytes.NewReader([]byte("data")))

		remoteStorage.EXPECT().
			Get(gomock.Any(), &GetRequest{ActionID: actionID}).
			Return(&GetResponse{
				OutputID:             outputID,
				ModTime:              modTime,
				Size:                 size,
				Body:                 compressedBody,
				CompressionAlgorithm: "zstd",
				UncompressedSize:     size,
			}, nil)

		compressionCodec.EXPECT().
			Decompress(&DecompressRequest{
				Body:      compressedBody,
				Algorithm: "zstd",
			}).
			Return(&DecompressResponse{
				Body: decompressedBody,
			}, nil)

		localStorage.EXPECT().
			PutLocal(gomock.Any(), &LocalPutRequest{
				ActionID: actionID,
				OutputID: outputID,
				Size:     size,
				Body:     decompressedBody,
			}).
			Return(&LocalPutResponse{DiskPath: "/tmp/cache/456"}, nil)

		h.Handle(ctx, writer, &cacheproto.Request{
			ID:       2,
			Command:  cacheproto.CmdGet,
			ActionID: actionID,
		})

		require.Len(t, writer.responses, 1)
		resp := writer.responses[0]
		assert.Equal(t, int64(2), resp.ID)
		assert.Equal(t, outputID, resp.OutputID)
		assert.Equal(t, size, resp.Size)
		assert.Equal(t, "/tmp/cache/456", resp.DiskPath)
		assert.Equal(t, modTime.Unix(), resp.Time.Unix())
	})

	t.Run("complete miss", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		localStorage := NewMockLocalStorage(ctrl)
		remoteStorage := NewMockRemoteStorage(ctrl)
		compressionCodec := NewMockCompressionCodec(ctrl)
		writer := &mockResponseWriter{}

		h := NewHandler(HandlerOptions{
			RemoteStorage:    remoteStorage,
			LocalStorage:     localStorage,
			CompressionCodec: compressionCodec,
		})
		localStorage.EXPECT().
			GetLocal(gomock.Any(), &LocalGetRequest{ActionID: actionID}).
			Return(nil, ErrNotFound)

		remoteStorage.EXPECT().
			Get(gomock.Any(), &GetRequest{ActionID: actionID}).
			Return(nil, ErrNotFound)

		h.Handle(ctx, writer, &cacheproto.Request{
			ID:       3,
			Command:  cacheproto.CmdGet,
			ActionID: actionID,
		})

		require.Len(t, writer.responses, 1)
		resp := writer.responses[0]
		assert.Equal(t, int64(3), resp.ID)
		assert.True(t, resp.Miss)
	})

	t.Run("disable get", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		localStorage := NewMockLocalStorage(ctrl)
		remoteStorage := NewMockRemoteStorage(ctrl)
		compressionCodec := NewMockCompressionCodec(ctrl)
		writer := &mockResponseWriter{}

		h := NewHandler(HandlerOptions{
			RemoteStorage:    remoteStorage,
			LocalStorage:     localStorage,
			CompressionCodec: compressionCodec,
			DisableGet:       true,
		})

		h.Handle(ctx, writer, &cacheproto.Request{
			ID:       4,
			Command:  cacheproto.CmdGet,
			ActionID: actionID,
		})

		require.Len(t, writer.responses, 1)
		resp := writer.responses[0]
		assert.Equal(t, int64(4), resp.ID)
		assert.Equal(t, "getting objects from any storage is disabled", resp.Err)
	})
}

func TestHandler_Handle_Put(t *testing.T) {
	ctx := context.Background()
	actionID := []byte("test-action")
	outputID := []byte("test-output")

	t.Run("successful put", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		localStorage := NewMockLocalStorage(ctrl)
		remoteStorage := NewMockRemoteStorage(ctrl)
		compressionCodec := NewMockCompressionCodec(ctrl)
		writer := &mockResponseWriter{}

		h := NewHandler(HandlerOptions{
			RemoteStorage:    remoteStorage,
			LocalStorage:     localStorage,
			CompressionCodec: compressionCodec,
			CloseTimeout:     time.Second,
		})
		payload := []byte("data")
		body := &mockReadSeekCloser{bytes.NewReader(payload)}
		localStorage.EXPECT().
			PutLocal(gomock.Any(), &LocalPutRequest{
				ActionID: actionID,
				OutputID: outputID,
				Size:     int64(len(payload)),
				Body:     body,
			}).
			Return(&LocalPutResponse{DiskPath: "/tmp/cache/789"}, nil)

		localBody := &mockReadSeekCloser{bytes.NewReader(payload)}
		compressedPayload := []byte("compressed-data")
		compressedBody := &mockReadSeekCloser{bytes.NewReader(compressedPayload)}
		md5sum := md5.Sum(compressedPayload)
		sha256sum := sha256.Sum256(compressedPayload)

		localStorage.EXPECT().
			GetLocalObject(gomock.Any(), &LocalObjectGetRequest{
				ActionID: actionID,
			}).
			Return(&LocalObjectGetResponse{
				ActionID: actionID,
				OutputID: outputID,
				Size:     int64(len(payload)),
				Body:     localBody,
			}, nil)

		compressionCodec.EXPECT().
			Compress(&CompressRequest{
				Body: localBody,
				Size: int64(len(payload)),
			}).
			Return(&CompressResponse{
				Size:      int64(len("compressed-data")),
				Body:      compressedBody,
				Algorithm: "zstd",
			}, nil)

		var remotePushWait sync.WaitGroup
		remotePushWait.Add(1)
		remoteStorage.EXPECT().
			Put(gomock.Any(), &PutRequest{
				ActionID:             actionID,
				OutputID:             outputID,
				Size:                 int64(len("compressed-data")),
				Body:                 compressedBody,
				MD5Sum:               md5sum[:],
				Sha256Sum:            sha256sum[:],
				CompressionAlgorithm: "zstd",
				UncompressedSize:     int64(len(payload)),
			}).
			Do(func(context.Context, *PutRequest) {
				remotePushWait.Done()
			}).
			Return(&PutResponse{}, nil)

		h.Handle(ctx, writer, &cacheproto.Request{
			ID:       1,
			Command:  cacheproto.CmdPut,
			ActionID: actionID,
			OutputID: outputID,
			BodySize: int64(len(payload)),
			Body:     body,
		})

		require.Len(t, writer.responses, 1)
		resp := writer.responses[0]
		assert.Equal(t, int64(1), resp.ID)
		assert.Equal(t, "/tmp/cache/789", resp.DiskPath)

		// Wait for background upload to complete
		remotePushWait.Wait()
	})

	t.Run("successful put, using ObjectID instead of OutputID", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		localStorage := NewMockLocalStorage(ctrl)
		remoteStorage := NewMockRemoteStorage(ctrl)
		compressionCodec := NewMockCompressionCodec(ctrl)
		writer := &mockResponseWriter{}

		h := NewHandler(HandlerOptions{
			RemoteStorage:    remoteStorage,
			LocalStorage:     localStorage,
			CompressionCodec: compressionCodec,
			CloseTimeout:     time.Second,
		})
		payload := []byte("data")
		body := &mockReadSeekCloser{bytes.NewReader(payload)}
		localStorage.EXPECT().
			PutLocal(gomock.Any(), &LocalPutRequest{
				ActionID: actionID,
				OutputID: outputID,
				Size:     int64(len(payload)),
				Body:     body,
			}).
			Return(&LocalPutResponse{DiskPath: "/tmp/cache/789"}, nil)

		localBody := &mockReadSeekCloser{bytes.NewReader(payload)}
		compressedPayload := []byte("compressed-data")
		compressedBody := &mockReadSeekCloser{bytes.NewReader(compressedPayload)}
		md5sum := md5.Sum(compressedPayload)
		sha256sum := sha256.Sum256(compressedPayload)

		localStorage.EXPECT().
			GetLocalObject(gomock.Any(), &LocalObjectGetRequest{
				ActionID: actionID,
			}).
			Return(&LocalObjectGetResponse{
				ActionID: actionID,
				OutputID: outputID,
				Size:     int64(len(payload)),
				Body:     localBody,
			}, nil)

		compressionCodec.EXPECT().
			Compress(&CompressRequest{
				Body: localBody,
				Size: int64(len(payload)),
			}).
			Return(&CompressResponse{
				Size:      int64(len("compressed-data")),
				Body:      compressedBody,
				Algorithm: "zstd",
			}, nil)

		var remotePushWait sync.WaitGroup
		remotePushWait.Add(1)
		remoteStorage.EXPECT().
			Put(gomock.Any(), &PutRequest{
				ActionID:             actionID,
				OutputID:             outputID,
				Size:                 int64(len("compressed-data")),
				Body:                 compressedBody,
				MD5Sum:               md5sum[:],
				Sha256Sum:            sha256sum[:],
				CompressionAlgorithm: "zstd",
				UncompressedSize:     int64(len(payload)),
			}).
			Do(func(context.Context, *PutRequest) {
				remotePushWait.Done()
			}).
			Return(&PutResponse{}, nil)

		h.Handle(ctx, writer, &cacheproto.Request{
			ID:       1,
			Command:  cacheproto.CmdPut,
			ActionID: actionID,
			ObjectID: outputID,
			BodySize: int64(len(payload)),
			Body:     body,
		})

		require.Len(t, writer.responses, 1)
		resp := writer.responses[0]
		assert.Equal(t, int64(1), resp.ID)
		assert.Equal(t, "/tmp/cache/789", resp.DiskPath)

		// Wait for background upload to complete
		remotePushWait.Wait()
	})

	t.Run("skip remote put for small objects", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		localStorage := NewMockLocalStorage(ctrl)
		remoteStorage := NewMockRemoteStorage(ctrl)
		compressionCodec := NewMockCompressionCodec(ctrl)
		writer := &mockResponseWriter{}

		h := NewHandler(HandlerOptions{
			RemoteStorage:    remoteStorage,
			LocalStorage:     localStorage,
			CompressionCodec: compressionCodec,
			CloseTimeout:     time.Second,
			MinRemotePutSize: 100,
		})

		payload := []byte("data")
		body := &mockReadSeekCloser{bytes.NewReader(payload)}
		localStorage.EXPECT().
			PutLocal(gomock.Any(), &LocalPutRequest{
				ActionID: actionID,
				OutputID: outputID,
				Size:     int64(len(payload)),
				Body:     body,
			}).
			Return(&LocalPutResponse{DiskPath: "/tmp/cache/789"}, nil)

		h.Handle(ctx, writer, &cacheproto.Request{
			ID:       1,
			Command:  cacheproto.CmdPut,
			ActionID: actionID,
			ObjectID: outputID,
			BodySize: int64(len(payload)),
			Body:     body,
		})

		require.Len(t, writer.responses, 1)
		resp := writer.responses[0]
		assert.Equal(t, int64(1), resp.ID)
		assert.Equal(t, "/tmp/cache/789", resp.DiskPath)
	})
}

func TestHandler_Handle_Close(t *testing.T) {
	h := NewHandler(HandlerOptions{
		CloseTimeout: time.Second,
	})

	writer := &mockResponseWriter{}
	h.Handle(context.Background(), writer, &cacheproto.Request{
		ID:      1,
		Command: cacheproto.CmdClose,
	})

	require.Len(t, writer.responses, 1)
	resp := writer.responses[0]
	assert.Equal(t, int64(1), resp.ID)
	assert.Empty(t, resp.Err)
}

func TestHandler_OnClose(t *testing.T) {
	t.Run("successful OnClose execution", func(t *testing.T) {
		var onCloseCalled atomic.Bool
		onClose := func(_ context.Context) error {
			onCloseCalled.Store(true)
			return nil
		}

		h := NewHandler(HandlerOptions{
			CloseTimeout: time.Second,
			OnClose:      onClose,
		})

		writer := &mockResponseWriter{}
		startTime := time.Now()

		h.Handle(context.Background(), writer, &cacheproto.Request{
			ID:      1,
			Command: cacheproto.CmdClose,
		})

		elapsed := time.Since(startTime)

		// OnClose should have been called
		assert.True(t, onCloseCalled.Load())

		// Response should be sent
		require.Len(t, writer.responses, 1)
		resp := writer.responses[0]
		assert.Equal(t, int64(1), resp.ID)
		assert.Empty(t, resp.Err)

		// Should not timeout
		assert.Less(t, elapsed, time.Second)
	})

	t.Run("OnClose with timeout", func(t *testing.T) {
		closeTimeout := 100 * time.Millisecond
		onCloseDelay := 200 * time.Millisecond

		var onCloseCalled atomic.Bool
		var onCloseTimedOut atomic.Bool
		onClose := func(ctx context.Context) error {
			onCloseCalled.Store(true)
			select {
			case <-time.After(onCloseDelay):
				return nil
			case <-ctx.Done():
				onCloseTimedOut.Store(true)
				return ctx.Err()
			}
		}

		h := NewHandler(HandlerOptions{
			CloseTimeout: closeTimeout,
			OnClose:      onClose,
		})

		writer := &mockResponseWriter{}
		startTime := time.Now()

		h.Handle(context.Background(), writer, &cacheproto.Request{
			ID:      1,
			Command: cacheproto.CmdClose,
		})

		elapsed := time.Since(startTime)

		// OnClose should have been called and timed out
		assert.True(t, onCloseCalled.Load())
		assert.True(t, onCloseTimedOut.Load())

		// Should respect timeout
		assert.GreaterOrEqual(t, elapsed, closeTimeout)
		assert.Less(t, elapsed, onCloseDelay)

		// Response should still be sent
		require.Len(t, writer.responses, 1)
		resp := writer.responses[0]
		assert.Equal(t, int64(1), resp.ID)
		assert.Empty(t, resp.Err)
	})

	t.Run("OnClose returns error", func(t *testing.T) {
		expectedErr := errors.New("close failed")
		onClose := func(_ context.Context) error {
			return expectedErr
		}

		h := NewHandler(HandlerOptions{
			CloseTimeout: time.Second,
			OnClose:      onClose,
		})

		writer := &mockResponseWriter{}

		h.Handle(context.Background(), writer, &cacheproto.Request{
			ID:      1,
			Command: cacheproto.CmdClose,
		})

		// Response should still be sent successfully even if OnClose fails
		require.Len(t, writer.responses, 1)
		resp := writer.responses[0]
		assert.Equal(t, int64(1), resp.ID)
		assert.Empty(t, resp.Err)
	})

	t.Run("OnClose without timeout waits indefinitely", func(t *testing.T) {
		onCloseDelay := 100 * time.Millisecond
		var onCloseCalled atomic.Bool
		var onCloseCompleted atomic.Bool

		onClose := func(_ context.Context) error {
			onCloseCalled.Store(true)
			time.Sleep(onCloseDelay)
			onCloseCompleted.Store(true)
			return nil
		}

		h := NewHandler(HandlerOptions{
			CloseTimeout: 0, // No timeout
			OnClose:      onClose,
		})

		writer := &mockResponseWriter{}
		startTime := time.Now()

		h.Handle(context.Background(), writer, &cacheproto.Request{
			ID:      1,
			Command: cacheproto.CmdClose,
		})

		elapsed := time.Since(startTime)

		// OnClose should have been called and completed
		assert.True(t, onCloseCalled.Load())
		assert.True(t, onCloseCompleted.Load())

		// Should wait for OnClose to complete
		assert.GreaterOrEqual(t, elapsed, onCloseDelay)

		// Response should be sent
		require.Len(t, writer.responses, 1)
		resp := writer.responses[0]
		assert.Equal(t, int64(1), resp.ID)
		assert.Empty(t, resp.Err)
	})

	t.Run("OnClose with background operations", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		localStorage := NewMockLocalStorage(ctrl)
		remoteStorage := NewMockRemoteStorage(ctrl)
		compressionCodec := NewMockCompressionCodec(ctrl)

		var onCloseCalled atomic.Bool
		onClose := func(_ context.Context) error {
			onCloseCalled.Store(true)
			return nil
		}

		h := NewHandler(HandlerOptions{
			RemoteStorage:    remoteStorage,
			LocalStorage:     localStorage,
			CompressionCodec: compressionCodec,
			CloseTimeout:     time.Second,
			OnClose:          onClose,
		})

		payload := []byte("data")
		body := &mockReadSeekCloser{bytes.NewReader(payload)}

		// Set up mocks for a PUT operation
		localStorage.EXPECT().
			PutLocal(gomock.Any(), gomock.Any()).
			Return(&LocalPutResponse{DiskPath: "/tmp/cache/test"}, nil)

		localStorage.EXPECT().
			GetLocalObject(gomock.Any(), gomock.Any()).
			Return(&LocalObjectGetResponse{
				ActionID: []byte("test"),
				OutputID: []byte("test"),
				Size:     int64(len(payload)),
				Body:     body,
			}, nil)

		compressionCodec.EXPECT().
			Compress(gomock.Any()).
			Return(&CompressResponse{
				Size:      int64(len("compressed-data")),
				Body:      &mockReadSeekCloser{bytes.NewReader([]byte("compressed-data"))},
				Algorithm: "zstd",
			}, nil)

		// Make remote put slow to ensure it's still running when close is called
		var remotePutStarted sync.WaitGroup
		remotePutStarted.Add(1)
		remoteStorage.EXPECT().
			Put(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, *PutRequest) (*PutResponse, error) {
				remotePutStarted.Done()
				time.Sleep(50 * time.Millisecond)
				return &PutResponse{}, nil
			})

		writer := &mockResponseWriter{}

		// Start a PUT operation
		h.Handle(context.Background(), writer, &cacheproto.Request{
			ID:       1,
			Command:  cacheproto.CmdPut,
			ActionID: []byte("test"),
			OutputID: []byte("test"),
			BodySize: int64(len(payload)),
			Body:     bytes.NewReader(payload),
		})

		// Wait for background operation to start
		remotePutStarted.Wait()

		// Now close the handler
		h.Handle(context.Background(), writer, &cacheproto.Request{
			ID:      2,
			Command: cacheproto.CmdClose,
		})

		// OnClose should have been called
		assert.True(t, onCloseCalled.Load())

		// Both responses should be sent
		require.Len(t, writer.responses, 2)

		// PUT response
		putResp := writer.responses[0]
		assert.Equal(t, int64(1), putResp.ID)
		assert.Equal(t, "/tmp/cache/test", putResp.DiskPath)

		// CLOSE response
		closeResp := writer.responses[1]
		assert.Equal(t, int64(2), closeResp.ID)
		assert.Empty(t, closeResp.Err)
	})
}

func TestHandler_ConcurrentOperations(t *testing.T) {
	const maxConcurrentGets = 2

	t.Run("respects max concurrent remote gets", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		localStorage := NewMockLocalStorage(ctrl)
		remoteStorage := NewMockRemoteStorage(ctrl)
		compressionCodec := NewMockCompressionCodec(ctrl)
		writer := &mockResponseWriter{}

		h := NewHandler(HandlerOptions{
			RemoteStorage:           remoteStorage,
			MaxConcurrentRemoteGets: maxConcurrentGets,
			LocalStorage:            localStorage,
			CompressionCodec:        compressionCodec,
		})

		const numRequests = 5
		startTime := time.Now()

		// Each remote get will take 100ms
		remoteGetDelay := 100 * time.Millisecond

		compressedBody := &mockReadSeekCloser{bytes.NewReader([]byte("compressed-data"))}
		decompressedBody := &mockReadSeekCloser{bytes.NewReader([]byte("data"))}

		// Expect local misses for all requests
		localStorage.EXPECT().
			GetLocal(gomock.Any(), gomock.Any()).
			Return(nil, ErrNotFound).
			Times(numRequests)

		// Remote gets will be slow
		remoteStorage.EXPECT().
			Get(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, *GetRequest) (*GetResponse, error) {
				time.Sleep(remoteGetDelay)
				return &GetResponse{
					OutputID:             []byte("test"),
					ModTime:              time.Now(),
					Size:                 100,
					Body:                 compressedBody,
					CompressionAlgorithm: "zstd",
					UncompressedSize:     100,
				}, nil
			}).
			Times(numRequests)

		// Compression codec will be called for each request
		compressionCodec.EXPECT().
			Decompress(gomock.Any()).
			Return(&DecompressResponse{
				Body: decompressedBody,
			}, nil).
			Times(numRequests)

		// Local puts will be fast
		localStorage.EXPECT().
			PutLocal(gomock.Any(), gomock.Any()).
			Return(&LocalPutResponse{DiskPath: "/tmp/cache/test"}, nil).
			Times(numRequests)

		// Launch concurrent requests
		var wg sync.WaitGroup
		for i := 0; i < numRequests; i++ {
			wg.Add(1)
			go func(reqID int64) {
				defer wg.Done()
				h.Handle(context.Background(), writer, &cacheproto.Request{
					ID:       reqID,
					Command:  cacheproto.CmdGet,
					ActionID: []byte("test"),
				})
			}(int64(i + 1))
		}

		wg.Wait()
		elapsed := time.Since(startTime)

		// With 5 requests, 100ms each, and max 2 concurrent operations,
		// it should take at least 300ms (3 batches: 2 + 2 + 1 requests)
		assert.GreaterOrEqual(t, elapsed, 3*remoteGetDelay)

		// But it shouldn't take too long (add some buffer for scheduling)
		assert.Less(t, elapsed, 4*remoteGetDelay)

		// Verify all requests got responses
		assert.Equal(t, numRequests, len(writer.responses))
	})
}

func TestHandler_RateLimiting(t *testing.T) {
	const maxConcurrentPuts = 1

	t.Run("rate limits background puts", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		localStorage := NewMockLocalStorage(ctrl)
		remoteStorage := NewMockRemoteStorage(ctrl)
		compressionCodec := NewMockCompressionCodec(ctrl)
		writer := &mockResponseWriter{}

		h := NewHandler(HandlerOptions{
			RemoteStorage:           remoteStorage,
			MaxConcurrentRemotePuts: maxConcurrentPuts,
			LocalStorage:            localStorage,
			CompressionCodec:        compressionCodec,
			CloseTimeout:            time.Second,
		})

		payload := []byte("data")
		body := &mockReadSeekCloser{bytes.NewReader(payload)}
		compressedBody := &mockReadSeekCloser{bytes.NewReader([]byte("compressed-data"))}

		const numRequests = 3
		startTime := time.Now()

		// Local puts are immediate
		localStorage.EXPECT().
			PutLocal(gomock.Any(), gomock.Any()).
			Return(&LocalPutResponse{DiskPath: "/tmp/cache/test"}, nil).
			Times(numRequests)

		// GetLocalObject for background puts
		localStorage.EXPECT().
			GetLocalObject(gomock.Any(), gomock.Any()).
			Return(&LocalObjectGetResponse{
				ActionID: []byte("test"),
				OutputID: []byte("test"),
				Size:     int64(len(payload)),
				Body:     body,
			}, nil).
			Times(numRequests)

		// Compression codec will be called for each request
		compressionCodec.EXPECT().
			Compress(gomock.Any()).
			Return(&CompressResponse{
				Size:      int64(len("compressed-data")),
				Body:      compressedBody,
				Algorithm: "zstd",
			}, nil).
			Times(numRequests)

		putDelay := 100 * time.Millisecond
		remoteStorage.EXPECT().
			Put(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, *PutRequest) (*PutResponse, error) {
				time.Sleep(putDelay)
				return &PutResponse{}, nil
			}).
			Times(numRequests)

		// Launch concurrent put requests
		for i := 0; i < numRequests; i++ {
			h.Handle(context.Background(), writer, &cacheproto.Request{
				ID:       int64(i + 1),
				Command:  cacheproto.CmdPut,
				ActionID: []byte("test"),
				OutputID: []byte("test"),
				BodySize: 100,
				Body:     bytes.NewReader([]byte("data")),
			})
		}

		// All PUT responses should be immediate
		assert.Equal(t, numRequests, len(writer.responses))
		assert.Less(t, time.Since(startTime), putDelay)

		// Close handler and wait for background operations
		h.Handle(context.Background(), writer, &cacheproto.Request{
			ID:      999,
			Command: cacheproto.CmdClose,
		})

		elapsed := time.Since(startTime)

		// With rate limiting of 1 concurrent put, it should take at least
		// numRequests * putDelay to complete all background operations
		assert.GreaterOrEqual(t, elapsed, time.Duration(numRequests)*putDelay)
	})
}

func TestHandler_ConcurrentWithClose(t *testing.T) {
	closeTimeout := 200 * time.Millisecond

	t.Run("cancels background operations on close", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		localStorage := NewMockLocalStorage(ctrl)
		remoteStorage := NewMockRemoteStorage(ctrl)
		compressionCodec := NewMockCompressionCodec(ctrl)
		writer := &mockResponseWriter{}

		h := NewHandler(HandlerOptions{
			RemoteStorage:    remoteStorage,
			LocalStorage:     localStorage,
			CompressionCodec: compressionCodec,
			CloseTimeout:     closeTimeout,
		})
		payload := []byte("data")
		// Local operations succeed immediately
		localStorage.EXPECT().
			PutLocal(gomock.Any(), gomock.Any()).
			Return(&LocalPutResponse{DiskPath: "/tmp/cache/test"}, nil).
			Times(2)

		localStorage.EXPECT().
			GetLocalObject(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, *LocalObjectGetRequest) (*LocalObjectGetResponse, error) {
				return &LocalObjectGetResponse{
					ActionID: []byte("test"),
					OutputID: []byte("test"),
					Size:     int64(len(payload)),
					Body:     &mockReadSeekCloser{bytes.NewReader(payload)},
				}, nil
			}).
			Times(2)

		compressedPayload := []byte("compressed-data")
		compressionCodec.EXPECT().
			Compress(gomock.Any()).
			DoAndReturn(func(*CompressRequest) (*CompressResponse, error) {
				return &CompressResponse{
					Size:      int64(len(compressedPayload)),
					Body:      &mockReadSeekCloser{bytes.NewReader(compressedPayload)},
					Algorithm: "zstd",
				}, nil
			}).
			Times(2)

		// Remote puts will be slow
		remoteStorage.EXPECT().
			Put(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, *PutRequest) (*PutResponse, error) {
				// Wait longer than close timeout
				time.Sleep(closeTimeout * 2)
				return &PutResponse{}, nil
			}).
			AnyTimes() // May be called 0-2 times depending on timing

		// Launch two put requests
		for i := 0; i < 2; i++ {
			h.Handle(context.Background(), writer, &cacheproto.Request{
				ID:       int64(i + 1),
				Command:  cacheproto.CmdPut,
				ActionID: []byte("test"),
				OutputID: []byte("test"),
				BodySize: 100,
				Body:     bytes.NewReader([]byte("data")),
			})
		}

		// Immediately close
		startClose := time.Now()
		h.Handle(context.Background(), writer, &cacheproto.Request{
			ID:      999,
			Command: cacheproto.CmdClose,
		})
		closeTime := time.Since(startClose)

		// Close should not wait longer than timeout
		assert.LessOrEqual(t, closeTime, closeTimeout*3/2) // Allow some buffer
	})
}
