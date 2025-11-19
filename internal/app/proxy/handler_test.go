package proxy

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/metrics"
	"github.com/stretchr/testify/require"
)

func TestHandler_Metrics(t *testing.T) {
	const (
		emptyValueLabel = "empty_value"

		plainValueLabel = "plain_value"
		plainValue      = "value"

		encodedValueLabel = "encoded_value"
		encodedValue      = "encoded/value" // not path-safe

		extraHeader = "X-Extra-Header"
		extraValue  = "extra_value"
	)

	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "expected POST request", http.StatusMethodNotAllowed)
			return
		}

		if r.Header.Get(extraHeader) != extraValue {
			http.Error(w, "expected extra header", http.StatusBadRequest)
			return
		}

		if !strings.HasPrefix(r.URL.Path, "/metrics/job") {
			http.Error(w, "expected metrics path", http.StatusBadRequest)
			return
		}

		if !strings.Contains(r.URL.Path, fmt.Sprintf("/%s@base64/=", emptyValueLabel)) {
			http.Error(w, "expected empty value label", http.StatusBadRequest)
			return
		}

		if !strings.Contains(r.URL.Path, fmt.Sprintf("/%s/%s", plainValueLabel, plainValue)) {
			http.Error(w, "expected plain value label", http.StatusBadRequest)
			return
		}

		if !strings.Contains(r.URL.Path, fmt.Sprintf("/%s@base64/%s", encodedValueLabel, base64.RawURLEncoding.EncodeToString([]byte(encodedValue)))) {
			http.Error(w, "expected encoded value label", http.StatusBadRequest)
			return
		}

		_, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(targetServer.Close)

	handler, err := NewHandler(nil, MetricsConfig{
		Endpoint: targetServer.URL,
		ExtraLabels: map[string]string{
			emptyValueLabel:   "",
			plainValueLabel:   plainValue,
			encodedValueLabel: encodedValue,
		},
		ExtraHeaders: http.Header{
			extraHeader: {extraValue},
		},
	})
	require.NoError(t, err)

	metricServer := httptest.NewServer(handler)
	t.Cleanup(metricServer.Close)

	require.NoError(t, metrics.PushMetrics(context.Background(), metricServer.URL+"/metricsproxy", true, &metrics.PushOptions{
		Method: "POST",
	}))
}
