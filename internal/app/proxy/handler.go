package proxy

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"

	"github.com/platacard/cacheprog/internal/app/cacheprog"
	"github.com/platacard/cacheprog/internal/infra/logging"
	"github.com/platacard/cacheprog/pkg/httpstorage"
)

type MetricsConfig struct {
	Endpoint     string            // metrics endpoint, metrics push proxy will be enabled if provided
	ExtraLabels  map[string]string // extra labels to be added to each metric
	ExtraHeaders http.Header       // extra headers to be added to each request
	Method       string            // HTTP method to use for sending metrics
}

// NewHandler creates a http handler meant to be used as a proxy for cacheprog.
// Among with remote storage it has 2 endpoints:
// - /health - returns 200
// - /metricsproxy - if [MetricsConfig.Endpoint] is provided - proxies metrics to the metrics endpoint, returns 200 on success, 500 on error
func NewHandler(remoteStorage cacheprog.RemoteStorage, metricsConfig MetricsConfig) (http.Handler, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	mux.Handle("/cache/", httpstorage.NewServer(remoteStorage))

	if metricsConfig.Endpoint != "" {
		metricsURL, err := makeMetricsPushURL(metricsConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to make metrics push URL: %w", err)
		}

		mux.Handle("/metricsproxy", http.StripPrefix("/metricsproxy", &httputil.ReverseProxy{
			Rewrite: func(r *httputil.ProxyRequest) {
				r.SetURL(metricsURL)
				for k := range metricsConfig.ExtraHeaders {
					for _, v := range metricsConfig.ExtraHeaders[k] {
						r.Out.Header.Add(k, v)
					}
				}
			},
			ErrorLog: slog.NewLogLogger(slog.Default().Handler(), slog.LevelError),
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
				slog.ErrorContext(r.Context(), "failed to proxy metrics", "error", err)
				http.Error(w, err.Error(), http.StatusBadGateway)
			},
		}))
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r.WithContext(logging.AttachArgs(r.Context(),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
		)))
	}), nil
}

func makeMetricsPushURL(metricsConfig MetricsConfig) (*url.URL, error) {
	u, err := url.Parse(metricsConfig.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metrics endpoint: %w", err)
	}

	if u.Path == "" {
		u.Path = "/" // make url absolute
	}

	if len(metricsConfig.ExtraLabels) == 0 {
		return u, nil
	}

	var pathElements []string
	pathElements = append(pathElements, "metrics")

	// metrics paths must start with job label
	var jobName string
	if jn, ok := metricsConfig.ExtraLabels["job"]; ok {
		jobName = jn
		delete(metricsConfig.ExtraLabels, "job")
	}
	if encodedJobName, isBase64 := encodeComponent(jobName); isBase64 {
		pathElements = append(pathElements, "job@base64", encodedJobName)
	} else {
		pathElements = append(pathElements, "job", jobName)
	}

	for _, k := range slices.Sorted(maps.Keys(metricsConfig.ExtraLabels)) {
		v := metricsConfig.ExtraLabels[k]
		if encoded, isBase64 := encodeComponent(v); isBase64 {
			pathElements = append(pathElements, k+"@base64", encoded)
		} else {
			pathElements = append(pathElements, k, encoded)
		}
	}

	return u.JoinPath(pathElements...), nil
}

// encodeComponent encodes the provided string with base64.RawURLEncoding in
// case it contains '/' and as "=" in case it is empty. If neither is the case,
// it uses url.QueryEscape instead. It returns true in the former two cases.
// This function was copied from prometheus client.
func encodeComponent(s string) (string, bool) {
	if s == "" {
		return "=", true
	}
	if strings.Contains(s, "/") {
		return base64.RawURLEncoding.EncodeToString([]byte(s)), true
	}
	return url.QueryEscape(s), false
}
