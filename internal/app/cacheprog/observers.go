package cacheprog

import (
	"context"
	"errors"
	"log/slog"

	"github.com/platacard/cacheprog/internal/infra/logging"
	"github.com/platacard/cacheprog/internal/infra/metrics"
)

type ObservingLocalStorage struct {
	LocalStorage
}

func (o ObservingLocalStorage) GetLocal(ctx context.Context, request *LocalGetRequest) (*LocalGetResponse, error) {
	slog.DebugContext(ctx, "Get object from local storage")
	defer metrics.ObserveObjectDuration("local", "get")()

	localObj, err := o.LocalStorage.GetLocal(ctx, request)
	switch {
	case errors.Is(err, nil):
		slog.DebugContext(ctx, "Local object found", "disk_path", localObj.DiskPath)
		metrics.ObserveObject("local", "get", localObj.Size)
		return localObj, nil
	case errors.Is(err, ErrNotFound):
		slog.DebugContext(ctx, "Local object not found")
		metrics.ObserveObjectMiss("local")
		return nil, err
	default:
		slog.ErrorContext(ctx, "Failed to get from local storage", logging.Error(err))
		metrics.ObserveStorageError("local", "get")
		return nil, err
	}
}

func (o ObservingLocalStorage) GetLocalObject(ctx context.Context, request *LocalObjectGetRequest) (*LocalObjectGetResponse, error) {
	slog.DebugContext(ctx, "Get object stream from local storage")
	defer metrics.ObserveObjectDuration("local", "get")()

	localObj, err := o.LocalStorage.GetLocalObject(ctx, request)
	switch {
	case errors.Is(err, nil):
		slog.DebugContext(ctx, "Local object stream found")
		metrics.ObserveObject("local", "get", localObj.Size)
		return localObj, nil
	case errors.Is(err, ErrNotFound):
		slog.DebugContext(ctx, "Local object stream not found")
		metrics.ObserveObjectMiss("local")
		return nil, err
	default:
		slog.ErrorContext(ctx, "Failed to get from local storage", logging.Error(err))
		metrics.ObserveStorageError("local", "get")
		return nil, err
	}
}

func (o ObservingLocalStorage) PutLocal(ctx context.Context, request *LocalPutRequest) (*LocalPutResponse, error) {
	ctx = logging.AttachArgs(ctx, "output_id", logging.Bytes(request.OutputID), "size", request.Size)
	slog.DebugContext(ctx, "Put object to local storage")
	defer metrics.ObserveObjectDuration("local", "put")()

	localObj, err := o.LocalStorage.PutLocal(ctx, request)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to put to local storage", logging.Error(err))
		metrics.ObserveStorageError("local", "put")
		return nil, err
	}

	slog.DebugContext(ctx, "Object stored in local storage", "disk_path", localObj.DiskPath)
	metrics.ObserveObject("local", "put", request.Size)
	return localObj, nil
}

type ObservingRemoteStorage struct {
	RemoteStorage
}

func (o ObservingRemoteStorage) Get(ctx context.Context, request *GetRequest) (*GetResponse, error) {
	slog.DebugContext(ctx, "Get object from remote storage")
	defer metrics.ObserveObjectDuration("remote", "get")()

	remoteObj, err := o.RemoteStorage.Get(ctx, request)
	switch {
	case errors.Is(err, nil):
		slog.DebugContext(ctx, "Remote object found",
			"output_id", logging.Bytes(remoteObj.OutputID),
			"mod_time", remoteObj.ModTime,
			"size", remoteObj.Size,
		)
		metrics.ObserveObject("remote", "get", remoteObj.Size)
		return remoteObj, nil
	case errors.Is(err, ErrNotFound):
		slog.DebugContext(ctx, "Remote object not found")
		metrics.ObserveObjectMiss("remote")
		return nil, err
	default:
		slog.ErrorContext(ctx, "Failed to get from remote storage", logging.Error(err))
		metrics.ObserveStorageError("remote", "get")
		return nil, err
	}
}

func (o ObservingRemoteStorage) Put(ctx context.Context, request *PutRequest) (*PutResponse, error) {
	ctx = logging.AttachArgs(ctx, "output_id", logging.Bytes(request.OutputID), "size", request.Size)
	slog.DebugContext(ctx, "Put object to remote storage")
	defer metrics.ObserveObjectDuration("remote", "put")()

	if _, err := o.RemoteStorage.Put(ctx, request); err != nil {
		slog.ErrorContext(ctx, "Failed to put to remote storage", logging.Error(err))
		metrics.ObserveStorageError("remote", "put")
		return nil, err
	}

	slog.DebugContext(ctx, "Object stored in remote storage")
	metrics.ObserveObject("remote", "put", request.Size)
	return &PutResponse{}, nil
}
