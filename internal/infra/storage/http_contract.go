package storage

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/platacard/cacheprog/internal/app/cacheprog"
)

const (
	ActionIDKey = "actionID"

	CachePath = "cache"

	HeaderPrefix               = "X-Cacheprog-"
	OutputIDHeader             = HeaderPrefix + "OutputID"
	MD5SumHeader               = HeaderPrefix + "MD5Sum"
	Sha256SumHeader            = HeaderPrefix + "Sha256Sum"
	CompressionAlgorithmHeader = HeaderPrefix + "CompressionAlgorithm"
	UncompressedSizeHeader     = HeaderPrefix + "UncompressedSize"
)

func ParsePutRequest(r *http.Request) (*cacheprog.PutRequest, error) {
	actionID := r.PathValue(ActionIDKey)
	if actionID == "" {
		return nil, errors.New("actionID is required")
	}

	actionIDBytes, err := hex.DecodeString(actionID)
	if err != nil {
		return nil, fmt.Errorf("failed to decode actionID: %w", err)
	}

	putRequest := &cacheprog.PutRequest{
		ActionID: actionIDBytes,
	}

	putRequest.Size = r.ContentLength
	if putRequest.Size < 0 {
		return nil, errors.New("content-length is unknown")
	}

	putRequest.OutputID, err = hex.DecodeString(r.Header.Get(OutputIDHeader))
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s header: %w", OutputIDHeader, err)
	}
	putRequest.MD5Sum, err = hex.DecodeString(r.Header.Get(MD5SumHeader))
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s header: %w", MD5SumHeader, err)
	}
	putRequest.Sha256Sum, err = hex.DecodeString(r.Header.Get(Sha256SumHeader))
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s header: %w", Sha256SumHeader, err)
	}
	putRequest.CompressionAlgorithm = r.Header.Get(CompressionAlgorithmHeader)
	putRequest.UncompressedSize, err = strconv.ParseInt(r.Header.Get(UncompressedSizeHeader), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s header: %w", UncompressedSizeHeader, err)
	}

	putRequest.Body = r.Body

	return putRequest, nil
}

func NewPutRequest(ctx context.Context, baseURL string, putRequest *cacheprog.PutRequest) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, addPathToURL(baseURL, CachePath, hex.EncodeToString(putRequest.ActionID)), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.ContentLength = putRequest.Size
	if putRequest.Size == 0 {
		req.Body = http.NoBody
	} else {
		req.Body = io.NopCloser(putRequest.Body)
	}

	req.Header.Set(OutputIDHeader, hex.EncodeToString(putRequest.OutputID))
	req.Header.Set(MD5SumHeader, hex.EncodeToString(putRequest.MD5Sum))
	req.Header.Set(Sha256SumHeader, hex.EncodeToString(putRequest.Sha256Sum))
	req.Header.Set(CompressionAlgorithmHeader, putRequest.CompressionAlgorithm)
	req.Header.Set(UncompressedSizeHeader, strconv.FormatInt(putRequest.UncompressedSize, 10))

	return req, nil
}

func ParseGetRequest(r *http.Request) (*cacheprog.GetRequest, error) {
	actionID := r.PathValue(ActionIDKey)
	if actionID == "" {
		return nil, errors.New("actionID is required")
	}

	actionIDBytes, err := hex.DecodeString(actionID)
	if err != nil {
		return nil, fmt.Errorf("failed to decode actionID: %w", err)
	}

	return &cacheprog.GetRequest{ActionID: actionIDBytes}, nil
}

func NewGetRequest(ctx context.Context, baseURL string, getRequest *cacheprog.GetRequest) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, addPathToURL(baseURL, CachePath, hex.EncodeToString(getRequest.ActionID)), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	return req, nil
}

func WriteGetResponse(w http.ResponseWriter, getResponse *cacheprog.GetResponse) error {
	defer getResponse.Body.Close()
	w.Header().Set("Content-Length", strconv.FormatInt(getResponse.Size, 10))
	w.Header().Set("Last-Modified", getResponse.ModTime.Format(http.TimeFormat))
	w.Header().Set(OutputIDHeader, hex.EncodeToString(getResponse.OutputID))
	w.Header().Set(CompressionAlgorithmHeader, getResponse.CompressionAlgorithm)
	w.Header().Set(UncompressedSizeHeader, strconv.FormatInt(getResponse.UncompressedSize, 10))
	w.WriteHeader(http.StatusOK)
	_, err := io.Copy(w, getResponse.Body)
	return err
}

func ParseGetResponse(r *http.Response) (*cacheprog.GetResponse, error) {
	if r.StatusCode == http.StatusNotFound {
		return nil, cacheprog.ErrNotFound
	}

	if r.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(r.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", r.StatusCode, string(body))
	}

	if r.ContentLength < 0 {
		return nil, errors.New("content-length is unknown")
	}

	var (
		getResponse cacheprog.GetResponse
		err         error
	)

	getResponse.OutputID, err = hex.DecodeString(r.Header.Get(OutputIDHeader))
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s header: %w", OutputIDHeader, err)
	}
	getResponse.ModTime, err = time.Parse(http.TimeFormat, r.Header.Get("Last-Modified"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s header: %w", "Last-Modified", err)
	}
	getResponse.CompressionAlgorithm = r.Header.Get(CompressionAlgorithmHeader)
	getResponse.UncompressedSize, err = strconv.ParseInt(r.Header.Get(UncompressedSizeHeader), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s header: %w", UncompressedSizeHeader, err)
	}
	getResponse.Size = r.ContentLength

	getResponse.Body = r.Body

	return &getResponse, nil
}

func addPathToURL(baseURL string, pathItems ...string) string {
	return strings.TrimSuffix(baseURL, "/") + "/" + path.Join(pathItems...)
}
