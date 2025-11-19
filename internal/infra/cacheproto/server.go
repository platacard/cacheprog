package cacheproto

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/platacard/cacheprog/internal/infra/logging"
)

//go:generate go tool go.uber.org/mock/mockgen -destination=mocks_world_test.go -package=$GOPACKAGE -source=$GOFILE

type ResponseWriter interface {
	WriteResponse(*Response) error
}

type Handler interface {
	Handle(ctx context.Context, writer ResponseWriter, req *Request)

	// Supports used to indicate supported features by the handler. Needed to negotiate supported commands.
	Supports(cmd Cmd) bool
}

type ServerOptions struct {
	Reader  io.Reader
	Writer  io.Writer
	Handler Handler
}

type Server struct {
	reader  *Reader
	writer  *synchronizedWriter
	handler Handler

	stop        atomic.Bool
	canStopWait chan struct{} // closing this guarantees that we can safely call stopWait.Wait
	stopWait    sync.WaitGroup
}

func NewServer(opts ServerOptions) *Server {
	return &Server{
		reader: NewReader(opts.Reader),
		writer: &synchronizedWriter{
			writer: NewWriter(opts.Writer),
		},
		handler:     opts.Handler,
		canStopWait: make(chan struct{}),
	}
}

func (s *Server) Run() error {
	ctx := context.Background()

	if err := s.handshake(ctx); err != nil {
		return fmt.Errorf("handshake failure: %w", err)
	}

	sem := make(chan struct{}, 1)
	var seenClose bool
	for !s.stop.Load() {
		sem <- struct{}{}
		slog.DebugContext(ctx, "Waiting for request")
		request, err := s.reader.ReadRequest()
		if errors.Is(err, io.EOF) {
			slog.InfoContext(ctx, "Stopping server")
			s.stop.Store(true)
			close(s.canStopWait)
			break
		}
		if err != nil {
			return fmt.Errorf("read request: %w", err)
		}

		ctx := logging.AttachArgs(ctx,
			"command", request.Command,
			"id", request.ID,
			"action_id", logging.Bytes(request.ActionID),
		)
		slog.DebugContext(ctx, "Request received")

		s.stopWait.Add(1)
		if s.stop.Load() {
			close(s.canStopWait)
		}

		if !seenClose && request.Command == CmdClose {
			seenClose = true
		}

		// this needed to ensure that entire body is read before we start reading next request
		reqBody := request.Body
		if reqBody != nil {
			request.Body = &unblockingReader{r: reqBody, sem: sem}
		} else {
			<-sem // release semaphore, we can read next request
		}

		go func() {
			defer s.stopWait.Done()
			s.handler.Handle(ctx, s.writer, request)
		}()
	}

	<-s.canStopWait
	s.stopWait.Wait()
	// sometimes client doesn't send close command, so we need to send it ourselves
	if !seenClose && s.handler.Supports(CmdClose) {
		slog.InfoContext(ctx, "Client didn't send close command, simulating it")
		s.handler.Handle(ctx, &noopWriter{ctx: ctx}, &Request{
			Command: CmdClose,
		})
	}
	slog.InfoContext(ctx, "Server stopped")
	return nil
}

func (s *Server) Stop() {
	s.stop.Store(true)
	<-s.canStopWait
	s.stopWait.Wait()
}

func (s *Server) handshake(ctx context.Context) error {
	var supportedCmds []Cmd

	if s.handler.Supports(CmdClose) {
		supportedCmds = append(supportedCmds, CmdClose)
	}

	if s.handler.Supports(CmdGet) {
		supportedCmds = append(supportedCmds, CmdGet)
	}

	if s.handler.Supports(CmdPut) {
		supportedCmds = append(supportedCmds, CmdPut)
	}

	err := s.writer.WriteResponse(&Response{
		ID:            0,
		KnownCommands: supportedCmds,
	})
	if err != nil {
		return fmt.Errorf("write hello: %w", err)
	}

	slog.DebugContext(ctx, "Handshake done", "supported_commands", supportedCmds)

	return nil
}

type synchronizedWriter struct {
	mu     sync.Mutex
	writer ResponseWriter
}

func (w *synchronizedWriter) WriteResponse(response *Response) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.writer.WriteResponse(response)
}

type unblockingReader struct {
	r   io.Reader
	sem <-chan struct{}
}

func (u *unblockingReader) Read(p []byte) (n int, err error) {
	n, err = u.r.Read(p)
	if err != nil {
		<-u.sem
	}
	return n, err
}
