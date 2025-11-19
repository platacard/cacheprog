package cacheproto

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestServer_ParallelRequestsBlocking(t *testing.T) {
	ctrl := gomock.NewController(t)

	handler := NewMockHandler(ctrl)
	var inBuf, outBuf bytes.Buffer

	// Create a server
	s := NewServer(ServerOptions{
		Reader:  &inBuf,
		Writer:  &outBuf,
		Handler: handler,
	})

	// Setup handler expectations
	handler.EXPECT().Supports(gomock.Any()).Return(true).AnyTimes()

	var handlerWg sync.WaitGroup
	handlerWg.Add(3)

	handler.EXPECT().Handle(gomock.Any(), gomock.Any(), gomock.Any()).
		Times(3).
		Do(func(_ context.Context, _ ResponseWriter, req *Request) {
			// Simulate slow processing of request body
			if req.Body != nil {
				_, err := io.Copy(io.Discard, req.Body)
				require.NoError(t, err)
			}
			handlerWg.Done()
		})

	// Write two requests to input buffer
	// First request with a body that will be read slowly
	req1Body := "hello world"
	req1BodyBase64 := base64.StdEncoding.EncodeToString([]byte(req1Body))
	writeRequest(&inBuf, &Request{
		ID:       1,
		Command:  CmdPut,
		BodySize: int64(len(req1Body)),
	})
	fmt.Fprintf(&inBuf, "\"%s\"\n", req1BodyBase64)

	// Second request without body
	writeRequest(&inBuf, &Request{
		ID:      2,
		Command: CmdGet,
	})

	// Start server in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run()
	}()

	// Wait for both handlers to complete
	handlerWg.Wait()

	// Stop server
	s.Stop()

	// Check for server errors
	require.NoError(t, <-errCh)
}

func writeRequest(w io.Writer, req *Request) {
	data, _ := json.Marshal(req)
	w.Write(data)
	w.Write([]byte("\n\n"))
}

func TestServer_Stop(t *testing.T) {
	ctrl := gomock.NewController(t)

	handler := NewMockHandler(ctrl)
	var inBuf, outBuf bytes.Buffer

	s := NewServer(ServerOptions{
		Reader:  &inBuf,
		Writer:  &outBuf,
		Handler: handler,
	})

	handler.EXPECT().Supports(gomock.Any()).Return(true).AnyTimes()

	// Start a long-running handler
	var onceHandlerStarted sync.Once
	handlerStarted := make(chan struct{})

	handler.EXPECT().Handle(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(context.Context, ResponseWriter, *Request) {
			onceHandlerStarted.Do(func() {
				close(handlerStarted)
			})
			time.Sleep(100 * time.Millisecond)
		}).Times(2)

	// Write a request
	writeRequest(&inBuf, &Request{
		ID:      1,
		Command: CmdGet,
	})

	// Run server in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run()
	}()

	// Wait for handler to start
	<-handlerStarted

	// Stop server - this should wait for handler to complete
	s.Stop()

	// Verify server stopped without errors
	require.NoError(t, <-errCh)
}

func TestServer_CloseOnEOF(t *testing.T) {
	ctrl := gomock.NewController(t)

	handler := NewMockHandler(ctrl)
	var inBuf, outBuf bytes.Buffer

	s := NewServer(ServerOptions{
		Reader:  &inBuf,
		Writer:  &outBuf,
		Handler: handler,
	})

	handler.EXPECT().Supports(gomock.Any()).Return(true).AnyTimes()

	// Expect Close command to be handled
	handler.EXPECT().Handle(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ ResponseWriter, req *Request) {
			require.Equal(t, CmdClose, req.Command)
		})

	// Write nothing to input buffer - this will cause EOF on read

	// Run server
	err := s.Run()
	require.NoError(t, err)
}
