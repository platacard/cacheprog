package metrics

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

const metricPrefix = "cacheprog_"

var (
	enableVMHistograms atomic.Bool
)

type PushConfig struct {
	Endpoint     string            // metrics endpoint, metrics will be pushed if provided
	ExtraLabels  map[string]string // extra labels to be added to each metric
	ExtraHeaders http.Header       // extra headers to be added to each request
	Method       string            // HTTP method to use for sending metrics
}

// EnableVMHistograms enables victoriametrics-style histograms instead of prometheus-style if 'true' is passed.
func EnableVMHistograms(enable bool) {
	enableVMHistograms.Store(enable)
}

type histogram interface {
	Update(float64)
	UpdateDuration(time.Time)
}

func getOrCreateHistogram(name string) histogram {
	if enableVMHistograms.Load() {
		return metrics.GetOrCreateHistogram(name)
	}
	return metrics.GetOrCreatePrometheusHistogram(name)
}

func ObserveOverallRunTime() func() {
	start := time.Now()
	return func() {
		metrics.GetOrCreateCounter(metricPrefix + "overall_run_time_ms").
			AddInt64(time.Since(start).Milliseconds())
	}
}

func ObserveObject(storageType, operation string, size int64) {
	metrics.GetOrCreateCounter(fmt.Sprintf("%sobject_get_size_total{storage_type=%q,operation=%q}",
		metricPrefix, storageType, operation)).Add(1)
	metrics.GetOrCreateCounter(fmt.Sprintf("%sobject_get_size_bytes_total{storage_type=%q,operation=%q}",
		metricPrefix, storageType, operation)).AddInt64(size)
	getOrCreateHistogram(fmt.Sprintf("%sobject_get_size_bytes{storage_type=%q,operation=%q}",
		metricPrefix, storageType, operation)).Update(float64(size))
}

func ObserveObjectMiss(storageType string) {
	metrics.GetOrCreateCounter(fmt.Sprintf("%sobject_miss_total{storage_type=%q}", metricPrefix, storageType)).
		Add(1)
}

func ObserveObjectDuration(storageType, operation string) func() {
	start := time.Now()
	return func() {
		getOrCreateHistogram(fmt.Sprintf("%sobject_get_duration_seconds{storage_type=%q,operation=%q}",
			metricPrefix, storageType, operation)).UpdateDuration(start)
	}
}

func ObserveStorageError(storageType, operation string) {
	metrics.GetOrCreateCounter(fmt.Sprintf("%sstorage_error_total{storage_type=%q,operation=%q}",
		metricPrefix, storageType, operation)).Add(1)
}

func PushMetrics(ctx context.Context, cfg PushConfig) error {
	if cfg.Endpoint == "" {
		return nil
	}

	extraLabels := make([]string, 0, len(cfg.ExtraLabels))
	for k, v := range cfg.ExtraLabels {
		extraLabels = append(extraLabels, fmt.Sprintf("%s=%q", k, v))
	}

	extraHeaders := make([]string, 0, len(cfg.ExtraHeaders))
	for k, vs := range cfg.ExtraHeaders {
		for _, v := range vs {
			extraHeaders = append(extraHeaders, fmt.Sprintf("%s: %s", k, v))
		}
	}

	return metrics.PushMetrics(ctx, cfg.Endpoint, true, &metrics.PushOptions{
		ExtraLabels: strings.Join(extraLabels, ","),
		Headers:     extraHeaders,
		Method:      cfg.Method,
	})
}
