package env

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/url"
	"sync"
)

type recordingServer struct {
	mu          sync.RWMutex
	rawRequests map[string]recordedRequest
}

type recordedRequest struct {
	Method  string
	Path    string
	Query   url.Values
	Headers http.Header
	Body    []byte
}

func newRecordingServer() *recordingServer {
	return &recordingServer{
		rawRequests: make(map[string]recordedRequest),
	}
}

func (s *recordingServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	recordKey := r.Header.Get("X-Test-Record-Key")
	if recordKey == "" {
		http.Error(w, "X-Test-Record-Key header is required", http.StatusBadRequest)
		return
	}

	bodyReader := r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		var err error
		bodyReader, err = gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, "failed to create gzip reader", http.StatusInternalServerError)
			return
		}
	}

	body, err := io.ReadAll(bodyReader)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.rawRequests[recordKey] = recordedRequest{
		Method:  r.Method,
		Path:    r.URL.Path,
		Query:   r.URL.Query(),
		Headers: r.Header,
		Body:    body,
	}
}

func (s *recordingServer) GetRequest(recordKey string) (recordedRequest, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	request, ok := s.rawRequests[recordKey]
	return request, ok
}
