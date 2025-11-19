package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"time"

	"github.com/platacard/cacheprog/internal/app/cacheprog"
	"github.com/platacard/cacheprog/internal/infra/cacheproto"
	"github.com/platacard/cacheprog/internal/infra/compression"
	"github.com/platacard/cacheprog/internal/infra/metrics"
	"github.com/platacard/cacheprog/internal/infra/storage"
)

type CacheprogAppArgs struct {
	RemoteStorageArgs
	MetricsPushArgs

	RootDirectory           string        `arg:"--root-directory,env:ROOT_DIRECTORY" placeholder:"PATH" help:"Root directory to local storage of objects. Must be read-write accessible by user and read-accessible by 'go' compiler. If not provided, subdirectory in system temporary directory will be used."`
	MaxConcurrentRemoteGets int           `arg:"--max-concurrent-remote-gets,env:MAX_CONCURRENT_REMOTE_GETS" placeholder:"NUM" help:"Max number of concurrent remote gets, unlimited if not provided"`
	MaxConcurrentRemotePuts int           `arg:"--max-concurrent-remote-puts,env:MAX_CONCURRENT_REMOTE_PUTS" placeholder:"NUM" help:"Max number of concurrent remote puts, unlimited if not provided"`
	MaxBackgroundWait       time.Duration `arg:"--max-background-wait,env:MAX_BACKGROUND_WAIT" placeholder:"DURATION" default:"10s" help:"Max time to wait for waiting of background operations to complete"`
	MinRemotePutSize        int64         `arg:"--min-remote-put-size,env:MIN_REMOTE_PUT_SIZE" placeholder:"SIZE" help:"Min size of object to push to remote storage, no size limit if not provided"`
	DisableGet              bool          `arg:"--disable-get,env:DISABLE_GET" help:"Disable getting objects from any storage, useful to force rebuild of the project and rewrite cache"`
}

type RemoteStorageArgs struct {
	S3Args
	HTTPStorageArgs

	RemoteStorageType string `arg:"--remote-storage-type,env:REMOTE_STORAGE_TYPE" placeholder:"TYPE" default:"disabled" help:"Remote storage type. Available: s3, http, disabled"`
}

type S3Args struct {
	Endpoint            *url.URL      `arg:"--s3-endpoint,env:S3_ENDPOINT" placeholder:"URL" help:"Endpoint for S3-compatible storages, use schemes minio+http://, minio+https://, etc. for minio compatibility."`
	ForcePathStyle      bool          `arg:"--s3-force-path-style,env:S3_FORCE_PATH_STYLE" placeholder:"true/false" help:"Forces path style endpoints, useful for some S3-compatible storages."`
	Region              string        `arg:"--s3-region,env:S3_REGION" placeholder:"REGION" help:"S3 region name. If not provided will be detected automatically via GetBucketLocation API."`
	Bucket              string        `arg:"--s3-bucket,env:S3_BUCKET" placeholder:"BUCKET" help:"S3 bucket name."`
	Prefix              string        `arg:"--s3-prefix,env:S3_PREFIX" placeholder:"PREFIX" help:"Prefix for S3 keys, useful to run multiple apps on same bucket. Templated, GOOS, GOARCH and env.<env var> are available. Template format: {% GOOS %}"`
	Expiration          time.Duration `arg:"--s3-expiration,env:S3_EXPIRATION" placeholder:"DURATION" help:"Sets expiration for each S3 object during Put, 0 - no expiration."`
	CredentialsEndpoint string        `arg:"--s3-credentials-endpoint,env:S3_CREDENTIALS_ENDPOINT" placeholder:"URL" help:"Credentials endpoint for S3-compatible storages."`
	AccessKeyID         string        `arg:"--s3-access-key-id,env:S3_ACCESS_KEY_ID" placeholder:"ID" help:"S3 access key id."`
	AccessKeySecret     string        `arg:"--s3-access-key-secret,env:S3_ACCESS_KEY_SECRET" placeholder:"SECRET" help:"S3 access key secret."`
	SessionToken        string        `arg:"--s3-session-token,env:S3_SESSION_TOKEN" placeholder:"TOKEN" help:"S3 session token."`
}

type HTTPStorageArgs struct {
	BaseURL      *url.URL     `arg:"--http-storage-base-url,env:HTTP_STORAGE_BASE_URL" placeholder:"URL" help:"Base URL for HTTP storage."`
	ExtraHeaders []httpHeader `arg:"--http-storage-extra-headers,env:HTTP_STORAGE_EXTRA_HEADERS" placeholder:"[key:value]" help:"Extra headers to be added to each request."`
}

type MetricsPushArgs struct {
	Endpoint     *url.URL          `arg:"--metrics-push-endpoint,env:METRICS_PUSH_ENDPOINT" placeholder:"URL" help:"Metrics endpoint, metrics will be pushed if provided"`
	Method       string            `arg:"--metrics-push-method,env:METRICS_PUSH_METHOD" placeholder:"METHOD" default:"GET" help:"HTTP method to use for sending metrics"`
	ExtraLabels  map[string]string `arg:"--metrics-push-extra-labels,env:METRICS_PUSH_EXTRA_LABELS" placeholder:"[key=value]" help:"Extra labels to be added to each metric, format: key=value"`
	ExtraHeaders []httpHeader      `arg:"--metrics-push-extra-headers,env:METRICS_PUSH_EXTRA_HEADERS" placeholder:"[key:value]" help:"Extra headers to be added to each request."`
}

func (r *RemoteStorageArgs) configureRemoteStorage() (cacheprog.RemoteStorage, error) {
	switch r.RemoteStorageType {
	case "s3":
		return storage.ConfigureS3(storage.S3Config{
			KeyPrefix:           r.Prefix,
			Expiration:          r.Expiration,
			Bucket:              r.Bucket,
			Region:              r.Region,
			Endpoint:            urlOrEmpty(r.Endpoint),
			ForcePathStyle:      r.ForcePathStyle,
			CredentialsEndpoint: r.CredentialsEndpoint,
			AccessKeyID:         r.AccessKeyID,
			AccessKeySecret:     r.AccessKeySecret,
			SessionToken:        r.SessionToken,
		})
	case "http":
		return storage.ConfigureHTTP(urlOrEmpty(r.BaseURL), headerValuesToHTTP(r.ExtraHeaders))
	case "disabled":
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid remote storage type: %s", r.RemoteStorageType)
	}
}

func (a *CacheprogAppArgs) Run(ctx context.Context) error {
	defer func() {
		if err := metrics.PushMetrics(ctx, metrics.PushConfig{
			Endpoint:     urlOrEmpty(a.Endpoint),
			ExtraLabels:  a.ExtraLabels,
			ExtraHeaders: headerValuesToHTTP(a.ExtraHeaders),
			Method:       a.Method,
		}); err != nil {
			slog.Warn("Failed to push metrics", "error", err)
		}
	}()

	defer metrics.ObserveOverallRunTime()()

	remoteStorage, err := a.configureRemoteStorage()
	if err != nil {
		return fmt.Errorf("failed to configure remote storage: %w", err)
	}

	if remoteStorage != nil {
		remoteStorage = cacheprog.ObservingRemoteStorage{RemoteStorage: remoteStorage}
	}

	diskStorage, err := storage.ConfigureDisk(a.RootDirectory)
	if err != nil {
		return fmt.Errorf("failed to configure disk storage: %w", err)
	}

	h := cacheprog.NewHandler(cacheprog.HandlerOptions{
		RemoteStorage:           remoteStorage,
		MaxConcurrentRemoteGets: a.MaxConcurrentRemoteGets,
		MaxConcurrentRemotePuts: a.MaxConcurrentRemotePuts,
		LocalStorage:            cacheprog.ObservingLocalStorage{LocalStorage: diskStorage},
		CloseTimeout:            a.MaxBackgroundWait,
		CompressionCodec:        compression.NewCodec(),
		DisableGet:              a.DisableGet,
	})
	defer func() {
		statistics := h.GetStatistics()
		slog.Info("cacheprog statistics",
			"get_calls", statistics.GetCalls,
			"get_hits", statistics.GetHits,
			"get_hit_ratio", fmt.Sprintf("%.2f", float64(statistics.GetHits)/float64(statistics.GetCalls)),
			"put_calls", statistics.PutCalls,
		)
	}()

	server := cacheproto.NewServer(cacheproto.ServerOptions{
		Reader:  os.Stdin,
		Writer:  os.Stdout,
		Handler: h,
	})

	defer context.AfterFunc(ctx, server.Stop)()

	slog.Info("Starting cacheprog")

	return server.Run()
}
