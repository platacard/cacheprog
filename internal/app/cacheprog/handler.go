package cacheprog

import (
	"context"
	"crypto/md5" //nolint:gosec // this used only for object presence detection by storage implementation
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/platacard/cacheprog/internal/infra/cacheproto"
	"github.com/platacard/cacheprog/internal/infra/logging"
)

//go:generate go tool go.uber.org/mock/mockgen -destination=mocks_world_test.go -package=$GOPACKAGE -source=$GOFILE

type (
	GetRequest struct {
		ActionID []byte
	}

	GetResponse struct {
		OutputID             []byte
		ModTime              time.Time
		Size                 int64
		Body                 io.ReadCloser
		CompressionAlgorithm string
		UncompressedSize     int64
	}

	PutRequest struct {
		ActionID             []byte
		OutputID             []byte
		Size                 int64
		Body                 io.Reader
		MD5Sum               []byte
		Sha256Sum            []byte
		CompressionAlgorithm string
		UncompressedSize     int64
	}

	PutResponse struct {
	}

	LocalGetRequest struct {
		ActionID []byte
	}

	LocalGetResponse struct {
		OutputID []byte
		ModTime  time.Time
		Size     int64
		DiskPath string
	}

	LocalObjectGetRequest struct {
		ActionID []byte
	}

	LocalObjectGetResponse struct {
		ActionID []byte
		OutputID []byte
		Size     int64
		Body     io.ReadSeekCloser
	}

	LocalPutRequest struct {
		ActionID []byte
		OutputID []byte
		Size     int64
		Body     io.Reader
	}

	LocalPutResponse struct {
		DiskPath string
	}

	CompressRequest struct {
		Body io.ReadSeeker
		Size int64
	}

	CompressResponse struct {
		Size      int64
		Body      io.ReadSeekCloser
		Algorithm string
	}

	DecompressRequest struct {
		Body      io.ReadCloser
		Algorithm string
	}

	DecompressResponse struct {
		Body io.ReadCloser
	}

	Statistics struct {
		GetCalls int64
		GetHits  int64
		PutCalls int64
	}
)

var ErrNotFound = errors.New("not found")

type (
	RemoteStorage interface {
		// Get fetches object from remote storage. Returns ErrNotFound if object was not found.
		Get(ctx context.Context, request *GetRequest) (*GetResponse, error)

		// Put pushes object to remote storage.
		Put(ctx context.Context, request *PutRequest) (*PutResponse, error)
	}
)

type (
	LocalStorage interface {
		// GetLocal returns object path on disk. Returns ErrNotFound if object was not found.
		GetLocal(ctx context.Context, request *LocalGetRequest) (*LocalGetResponse, error)

		// GetLocalObject returns object from disk. Returns ErrNotFound if object was not found.
		GetLocalObject(ctx context.Context, request *LocalObjectGetRequest) (*LocalObjectGetResponse, error)

		// PutLocal writes object onto disk and returns it path.
		PutLocal(ctx context.Context, request *LocalPutRequest) (*LocalPutResponse, error)
	}
)

type (
	CompressionCodec interface {
		// Compress compresses content and returns compressed result.
		Compress(req *CompressRequest) (*CompressResponse, error)

		// Decompress decompresses content and returns decompressed result.
		Decompress(req *DecompressRequest) (*DecompressResponse, error)
	}
)

type Handler struct {
	remoteStorage    RemoteStorage
	remoteGetSema    chan struct{}
	remotePutSema    chan struct{}
	minRemotePutSize int64
	compressionCodec CompressionCodec

	localStorage LocalStorage
	closeTimeout time.Duration
	closeChan    chan struct{} // closed on "close" command
	closeWG      sync.WaitGroup
	onClose      func(ctx context.Context) error

	disableGet bool

	// counters for statistic output
	getCalls atomic.Int64
	getHits  atomic.Int64
	putCalls atomic.Int64
}

type HandlerOptions struct {
	RemoteStorage           RemoteStorage    // if provided - remote storage will be used
	MaxConcurrentRemoteGets int              // max number of concurrent get requests to remote storage, 0 - unlimited
	MaxConcurrentRemotePuts int              // max number of concurrent put requests to remote storage, 0 - unlimited
	MinRemotePutSize        int64            // min size of object to push to remote storage, 0 - no limit
	LocalStorage            LocalStorage     // local storage getter and pusher
	CompressionCodec        CompressionCodec // compression codec to use on remote storage, if not provided - no compression will be used
	DisableGet              bool             // disable getting objects from any storage, useful to force rebuild of the project and rewrite cache

	CloseTimeout time.Duration                   // max time to wait for handler to close, 0 - no timeout
	OnClose      func(ctx context.Context) error // if provided - expected to be blocking, called on close command
}

func NewHandler(opts HandlerOptions) *Handler {
	var remoteGetSema chan struct{}
	if opts.MaxConcurrentRemoteGets > 0 {
		remoteGetSema = make(chan struct{}, opts.MaxConcurrentRemoteGets)
	}

	var remotePutSema chan struct{}
	if opts.MaxConcurrentRemotePuts > 0 {
		remotePutSema = make(chan struct{}, opts.MaxConcurrentRemotePuts)
	}

	return &Handler{
		remoteStorage:    opts.RemoteStorage,
		remoteGetSema:    remoteGetSema,
		remotePutSema:    remotePutSema,
		minRemotePutSize: opts.MinRemotePutSize,
		compressionCodec: opts.CompressionCodec,
		localStorage:     opts.LocalStorage,
		closeTimeout:     opts.CloseTimeout,
		onClose:          opts.OnClose,
		closeChan:        make(chan struct{}),
		disableGet:       opts.DisableGet,
	}
}

func (h *Handler) Handle(ctx context.Context, writer cacheproto.ResponseWriter, req *cacheproto.Request) {
	switch req.Command {
	case cacheproto.CmdClose:
		h.handleClose(ctx, writer, req)
	case cacheproto.CmdGet:
		h.getCalls.Add(1)
		h.handleGet(ctx, writer, req)
	case cacheproto.CmdPut:
		h.putCalls.Add(1)
		h.handlePut(ctx, writer, req)
	default:
		h.writeResponse(ctx, writer, &cacheproto.Response{
			ID:  req.ID,
			Err: fmt.Sprintf("unsupported command %q", req.Command),
		})
	}
}

func (h *Handler) handleGet(ctx context.Context, writer cacheproto.ResponseWriter, req *cacheproto.Request) {
	if h.disableGet {
		// this should never happen because compiler must not call this method if we announced disabled 'get' support
		h.writeResponse(ctx, writer, &cacheproto.Response{
			ID:  req.ID,
			Err: "getting objects from any storage is disabled",
		})
		return
	}

	// try to get object from local storage
	localObj, err := h.localStorage.GetLocal(ctx, &LocalGetRequest{
		ActionID: req.ActionID,
	})
	switch {
	case errors.Is(err, nil):
		h.getHits.Add(1)
		h.writeResponse(ctx, writer, &cacheproto.Response{
			ID:       req.ID,
			OutputID: localObj.OutputID,
			Time:     &localObj.ModTime,
			Size:     localObj.Size,
			DiskPath: localObj.DiskPath,
		})
		return
	case errors.Is(err, ErrNotFound):
		// if local object was not found - try to get it from remote storage
	default:
		h.writeResponse(ctx, writer, &cacheproto.Response{
			ID:  req.ID,
			Err: fmt.Sprintf("failed to get object: %v", err),
		})
		return
	}

	if h.remoteStorage == nil {
		slog.DebugContext(ctx, "Remote storage is not provided, report miss")
		h.writeResponse(ctx, writer, &cacheproto.Response{
			ID:   req.ID,
			Miss: true,
		})
		return
	}

	defer h.enterGetRemote()()

	remoteObj, err := h.remoteStorage.Get(ctx, &GetRequest{
		ActionID: req.ActionID,
	})
	switch {
	case errors.Is(err, nil):
		// store object in local storage and return path
		defer remoteObj.Body.Close()
	case errors.Is(err, ErrNotFound):
		h.writeResponse(ctx, writer, &cacheproto.Response{
			ID:   req.ID,
			Miss: true,
		})
		return
	default:
		h.writeResponse(ctx, writer, &cacheproto.Response{
			ID:  req.ID,
			Err: fmt.Sprintf("failed to get object: %v", err),
		})
		return
	}

	decompressedObj, err := h.compressionCodec.Decompress(&DecompressRequest{
		Body:      remoteObj.Body,
		Algorithm: remoteObj.CompressionAlgorithm,
	})
	if err != nil {
		h.writeResponse(ctx, writer, &cacheproto.Response{
			ID:  req.ID,
			Err: fmt.Sprintf("failed to decompress object: %v", err),
		})
		return
	}

	defer decompressedObj.Body.Close()

	newLocalObj, err := h.localStorage.PutLocal(ctx, &LocalPutRequest{
		ActionID: req.ActionID,
		OutputID: remoteObj.OutputID,
		Size:     remoteObj.UncompressedSize,
		Body:     decompressedObj.Body,
	})
	if err != nil {
		h.writeResponse(ctx, writer, &cacheproto.Response{
			ID:  req.ID,
			Err: fmt.Sprintf("failed to put object: %v", err),
		})
		return
	}

	h.getHits.Add(1)
	h.writeResponse(ctx, writer, &cacheproto.Response{
		ID:       req.ID,
		OutputID: remoteObj.OutputID,
		Time:     &remoteObj.ModTime,
		Size:     remoteObj.UncompressedSize,
		DiskPath: newLocalObj.DiskPath,
	})
}

func (h *Handler) handlePut(ctx context.Context, writer cacheproto.ResponseWriter, req *cacheproto.Request) {
	localObj, err := h.localStorage.PutLocal(ctx, &LocalPutRequest{
		ActionID: req.ActionID,
		OutputID: outputID(req),
		Size:     req.BodySize,
		Body:     req.Body,
	})
	if err != nil {
		h.writeResponse(ctx, writer, &cacheproto.Response{
			ID:  req.ID,
			Err: fmt.Sprintf("failed to put object: %v", err),
		})
		return
	}

	if h.minRemotePutSize > 0 && req.BodySize < h.minRemotePutSize {
		slog.DebugContext(ctx, "Object is too small to push to remote storage, skip", "size", req.BodySize, "min_size", h.minRemotePutSize)
		h.writeResponse(ctx, writer, &cacheproto.Response{
			ID:       req.ID,
			DiskPath: localObj.DiskPath,
		})
		return
	}

	// perform remote storage upload in background to avoid response blocking
	// upload failure is not critical, we already stored object in local storage
	if h.remoteStorage != nil {
		h.closeWG.Go(func() { h.putToRemote(req.ActionID) })
	}

	h.writeResponse(ctx, writer, &cacheproto.Response{
		ID:       req.ID,
		DiskPath: localObj.DiskPath,
	})
}

func (h *Handler) putToRemote(actionID []byte) {
	ctx := logging.AttachArgs(context.Background(), "action_id", logging.Bytes(actionID))

	ctx, exit := h.enterPutToRemote(ctx)
	defer exit()

	if ctx.Err() != nil {
		slog.WarnContext(ctx, "Put to remote storage cancelled")
		return
	}

	localObjStream, err := h.localStorage.GetLocalObject(ctx, &LocalObjectGetRequest{
		ActionID: actionID,
	})
	if err != nil {
		return
	}

	body := localObjStream.Body
	defer body.Close()

	compressResult, err := h.compressionCodec.Compress(&CompressRequest{
		Body: body,
		Size: localObjStream.Size,
	})
	if err != nil {
		slog.ErrorContext(ctx, "Failed to compress object", logging.Error(err))
		return
	}

	defer compressResult.Body.Close()

	md5sum := md5.New() //nolint:gosec // this used only for object presence detection by storage implementation
	sha256sum := sha256.New()

	_, err = io.Copy(io.MultiWriter(md5sum, sha256sum), compressResult.Body)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to calculate MD5 and SHA256 sums", logging.Error(err))
		return
	}

	_, err = compressResult.Body.Seek(0, io.SeekStart)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to seek local object body", logging.Error(err))
		return
	}

	_, err = h.remoteStorage.Put(ctx, &PutRequest{
		ActionID:             localObjStream.ActionID,
		OutputID:             localObjStream.OutputID,
		Size:                 compressResult.Size,
		Body:                 compressResult.Body,
		MD5Sum:               md5sum.Sum(nil),
		Sha256Sum:            sha256sum.Sum(nil),
		CompressionAlgorithm: compressResult.Algorithm,
		UncompressedSize:     localObjStream.Size,
	})
	if err != nil {
		slog.ErrorContext(ctx, "Failed to push object to remote storage", logging.Error(err))
		return
	}
}

func (h *Handler) handleClose(ctx context.Context, writer cacheproto.ResponseWriter, req *cacheproto.Request) {
	slog.DebugContext(ctx, "Close handler called")

	close(h.closeChan)
	if h.onClose != nil {
		h.closeWG.Go(func() { h.handleOnClose(ctx) })
	}
	h.closeWait()

	slog.DebugContext(ctx, "Close handler finished")
	h.writeResponse(ctx, writer, &cacheproto.Response{
		ID: req.ID,
	})
}

func (h *Handler) writeResponse(ctx context.Context, writer cacheproto.ResponseWriter, resp *cacheproto.Response) {
	if err := writer.WriteResponse(resp); err != nil {
		slog.ErrorContext(ctx, "Write response failed", logging.Error(err))
	}
}

func (h *Handler) enterGetRemote() (exit func()) {
	if h.remoteGetSema != nil {
		h.remoteGetSema <- struct{}{}
		return func() {
			<-h.remoteGetSema
		}
	}
	return func() {}
}

func (h *Handler) enterPutToRemote(ctx context.Context) (outCtx context.Context, exit func()) {
	outCtx = ctx
	outCtxCancel := func() {}

	// if close command was received - we should return timeout context
	select {
	case _, ok := <-h.closeChan:
		if !ok {
			outCtx, outCtxCancel = context.WithTimeout(ctx, h.closeTimeout)
		}
	default:
	}

	if h.remotePutSema != nil {
		select {
		case h.remotePutSema <- struct{}{}:
			return outCtx, func() {
				<-h.remotePutSema
				outCtxCancel()
			}
		case <-outCtx.Done():
		}
	}

	return outCtx, outCtxCancel
}

func (h *Handler) handleOnClose(ctx context.Context) {
	if h.onClose == nil {
		return
	}

	if h.closeTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.closeTimeout)
		defer cancel()
	}

	if err := h.onClose(ctx); err != nil {
		slog.ErrorContext(ctx, "OnClose handler failed", logging.Error(err))
	}
}

func (h *Handler) closeWait() {
	// we can simply use waitgroup here, because no new put commands will arrive after close command

	if h.closeTimeout == 0 {
		slog.Debug("waiting for background jobs to finish")
		h.closeWG.Wait()
		return
	}

	slog.Debug("waiting for background jobs to finish with timeout", "timeout", h.closeTimeout)
	logTicker := time.NewTicker(5 * time.Second)
	defer logTicker.Stop()

	allDone := make(chan struct{})
	go func() {
		h.closeWG.Wait()
		close(allDone)
	}()

	for {
		select {
		case <-allDone:
			return
		case <-logTicker.C:
			slog.Debug("still waiting for background jobs to finish")
		case <-time.After(h.closeTimeout):
			slog.Warn("background jobs did not finish in time")
			return
		}
	}
}

func (h *Handler) Supports(cmd cacheproto.Cmd) bool {
	switch cmd {
	case cacheproto.CmdClose:
		return true
	case cacheproto.CmdGet:
		return !h.disableGet
	case cacheproto.CmdPut:
		return true
	default:
		return false
	}
}

func (h *Handler) GetStatistics() Statistics {
	return Statistics{
		GetCalls: h.getCalls.Load(),
		GetHits:  h.getHits.Load(),
		PutCalls: h.putCalls.Load(),
	}
}

// outputID returns object id from request.
// It may appear either in OutputID (go compiler) or ObjectID (golangci-lint, legacy implementation) field.
func outputID(req *cacheproto.Request) []byte {
	if len(req.OutputID) > 0 {
		return req.OutputID
	}
	return req.ObjectID
}
