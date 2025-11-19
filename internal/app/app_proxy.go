package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/platacard/cacheprog/internal/app/proxy"
)

type ProxyAppArgs struct {
	MetricsProxyArgs
	RemoteStorageArgs

	ListenAddress string `arg:"--listen-address,env:PROXY_LISTEN_ADDRESS" placeholder:"ADDR" default:":8080" help:"Listen address"`
}

type MetricsProxyArgs struct {
	Endpoint     *url.URL          `arg:"--metrics-proxy-endpoint,env:METRICS_PROXY_ENDPOINT" placeholder:"URL" help:"Metrics endpoint, metrics push proxy endpoint will be enabled if provided"`
	ExtraLabels  map[string]string `arg:"--metrics-proxy-extra-labels,env:METRICS_PROXY_EXTRA_LABELS" placeholder:"[key=value]" help:"Extra labels to be added to each metric, format: key=value"`
	ExtraHeaders []httpHeader      `arg:"--metrics-proxy-extra-headers,env:METRICS_PROXY_EXTRA_HEADERS" placeholder:"[key:value]" help:"Extra headers to be added to each request."`
}

func (a *ProxyAppArgs) Run(ctx context.Context) error {
	remoteStorage, err := a.configureRemoteStorage()
	if err != nil {
		return fmt.Errorf("failed to configure remote storage: %w", err)
	}

	handler, err := proxy.NewHandler(remoteStorage, proxy.MetricsConfig{
		Endpoint:     urlOrEmpty(a.Endpoint),
		ExtraLabels:  a.ExtraLabels,
		ExtraHeaders: headerValuesToHTTP(a.ExtraHeaders),
	})
	if err != nil {
		return fmt.Errorf("failed to configure proxy handler: %w", err)
	}

	srv := &http.Server{
		Addr:              a.ListenAddress,
		Handler:           handler,
		ReadHeaderTimeout: time.Minute,
	}
	defer context.AfterFunc(ctx, func() {
		_ = srv.Close()
	})()

	slog.Info("Listening", "address", a.ListenAddress)

	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("failed to serve: %w", err)
	}

	return nil
}
