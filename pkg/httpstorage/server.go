package httpstorage

import (
	"context"
	"errors"
	"net/http"

	"github.com/platacard/cacheprog/internal/app/cacheprog"
	"github.com/platacard/cacheprog/internal/infra/storage"
)

//go:generate go tool go.uber.org/mock/mockgen -destination=mocks_world_test.go -package=$GOPACKAGE -source=$GOFILE

type (
	GetRequest  = cacheprog.GetRequest
	GetResponse = cacheprog.GetResponse
	PutRequest  = cacheprog.PutRequest
	PutResponse = cacheprog.PutResponse

	RemoteStorage interface {
		// Get fetches object from remote storage. Returns [ErrNotFound] if object was not found.
		Get(ctx context.Context, request *GetRequest) (*GetResponse, error)

		// Put pushes object to remote storage.
		Put(ctx context.Context, request *PutRequest) (*PutResponse, error)
	}
)

var (
	// ErrNotFound is returned when object is not found in remote storage.
	ErrNotFound = cacheprog.ErrNotFound
)

// NewServer creates a http handler meant to be used as a 'http' remote storage for cacheprog.
// It has 2 endpoints:
// - PUT /cache/{actionID} - pushes object to remote storage, returns 200 on success, 400 on bad request, 500 on error
// - GET /cache/{actionID} - fetches object from remote storage, returns 200 on success, 400 on bad request, 404 on not found object, 500 on error
func NewServer(remoteStorage RemoteStorage) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /cache/{actionID}", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		putRequest, err := storage.ParsePutRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if remoteStorage == nil {
			w.WriteHeader(http.StatusOK)
			return
		}

		_, err = remoteStorage.Put(ctx, putRequest)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /cache/{actionID}", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		getRequest, err := storage.ParseGetRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if remoteStorage == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		getResponse, err := remoteStorage.Get(ctx, getRequest)
		switch {
		case errors.Is(err, nil):
		case errors.Is(err, ErrNotFound):
			w.WriteHeader(http.StatusNotFound)
			return
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err = storage.WriteGetResponse(w, getResponse); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	return mux
}
