package storage

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/url"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	signer "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/endpointcreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	transport "github.com/aws/smithy-go/endpoints"
	"github.com/aws/smithy-go/middleware"
	"github.com/valyala/fasttemplate"

	"github.com/platacard/cacheprog/internal/app/cacheprog"
	"github.com/platacard/cacheprog/internal/infra/logging"
)

const (
	outputMetadataKey               = "output_id"
	compressionAlgorithmMetadataKey = "compression_algorithm"
	uncompressedSizeMetadataKey     = "uncompressed_size"
)

type S3Params struct {
	Client   S3Client
	Bucket   string
	Prefix   string
	Lifetime time.Duration // if provided, expiration will be set to each object during Put
}

type S3 struct {
	client   S3Client
	bucket   string
	prefix   string
	lifetime time.Duration
}

func NewS3(params S3Params) *S3 {
	return &S3{
		client:   params.Client,
		bucket:   params.Bucket,
		prefix:   params.Prefix,
		lifetime: params.Lifetime,
	}
}

type S3Config struct {
	// app-level settings

	KeyPrefix  string        // bucket key prefix, templated, GOOS and GOARCH are available
	Expiration time.Duration // sets expiration for each object during Put, 0 - no expiration

	// client-level basic settings

	Bucket string // required
	Region string

	// client-level path resolver settings

	Endpoint       string // optional, if provided, will be used to configure the client, supports schemes minio+http://, minio+https://, etc. for minio compatibility
	ForcePathStyle bool   // forces path style endpoints, useful for some S3-compatible storages

	// client-level credentials by endpoint settings, optional

	CredentialsEndpoint string

	// client-level static credentials settings, optional

	AccessKeyID     string
	AccessKeySecret string
	SessionToken    string
}

func ConfigureS3(cfg S3Config) (*S3, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}

	prefix, err := templatePrefix(cfg.KeyPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to template prefix: %w", err)
	}

	slog.Info("Configuring S3 storage",
		"region", cfg.Region,
		"forcePathStyle", cfg.ForcePathStyle,
		"bucket", cfg.Bucket,
		"prefix", prefix,
		"expiration", cfg.Expiration,
		"endpoint", cfg.Endpoint,
		"credentialsEndpoint", cfg.CredentialsEndpoint,
	)

	var (
		awsOptions []func(*awsConfig.LoadOptions) error
		s3Options  []func(*s3.Options)
	)

	s3Options = append(s3Options, func(options *s3.Options) {
		options.UsePathStyle = cfg.ForcePathStyle
		options.DisableLogOutputChecksumValidationSkipped = true
	})

	if logger := slog.Default(); logger != nil {
		awsOptions = append(awsOptions,
			awsConfig.WithLogger(&logging.SmithyLogger{Logger: logger}),
			awsConfig.WithClientLogMode(aws.LogDeprecatedUsage),
		)
	}
	if cfg.Endpoint != "" {
		u, err := url.Parse(cfg.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("failed to parse endpoint URL: %w", err)
		}

		switch u.Scheme {
		case "http", "https":
			awsOptions = append(awsOptions, awsConfig.WithBaseEndpoint(u.String()))
		case "minio+http", "minio+https":
			u.Scheme = strings.TrimPrefix(u.Scheme, "minio+")
			s3Options = append(s3Options, s3.WithEndpointResolverV2(&minioEndpointResolver{url: u}))
		default:
			return nil, fmt.Errorf("unsupported endpoint scheme: %s", u.Scheme)
		}
	}

	var credsProviders []aws.CredentialsProvider
	if cfg.CredentialsEndpoint != "" {
		credsProviders = append(credsProviders, endpointcreds.New(cfg.CredentialsEndpoint))
	}
	if cfg.AccessKeyID != "" {
		credsProviders = append(credsProviders, credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.AccessKeySecret, cfg.SessionToken))
	}
	if len(credsProviders) > 0 {
		awsOptions = append(awsOptions, awsConfig.WithCredentialsProvider(
			aws.NewCredentialsCache(chainCredentialsProviders(credsProviders...)),
		))
	}

	// try to figure out region if not provided
	region := cfg.Region
	if region == "" {
		// propagate gathered options to use custom endpoint if specified
		region, err = GetBucketRegion(context.Background(), cfg.Bucket, awsOptions...)
		if err == nil {
			slog.Info("Bucket region detected", "region", region)
		}
		// ignore error because s3-compatible storages may not support this API
	}
	if region != "" {
		awsOptions = append(awsOptions, awsConfig.WithRegion(region))
	}

	awsCfg, err := awsConfig.LoadDefaultConfig(context.Background(), awsOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to load aws config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, s3Options...)

	return NewS3(S3Params{
		Client:   s3Client,
		Bucket:   cfg.Bucket,
		Prefix:   prefix,
		Lifetime: cfg.Expiration,
	}), nil
}

// GetBucketRegion attempts to detrmine bucket region by querying GetBucketLocation API.
// This needed to reduce requests latency by preventing extra hops via default region if bucket region was not provided
func GetBucketRegion(ctx context.Context, bucket string, awsOptions ...func(*awsConfig.LoadOptions) error) (string, error) {
	// default region to retrieve real bucket region
	const defaultRegion = "us-east-1"

	var loadOptions []func(*awsConfig.LoadOptions) error
	loadOptions = append(loadOptions, awsConfig.WithRegion(defaultRegion))
	loadOptions = append(loadOptions, awsOptions...)

	awsClient, err := awsConfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return "", fmt.Errorf("failed to load aws config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsClient)

	location, err := s3Client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get bucket location: %w", err)
	}

	// for historical reasons, us-east-1 has no location constraint so return it as default
	return cmp.Or(string(location.LocationConstraint), defaultRegion), nil
}

func (s *S3) Get(ctx context.Context, request *cacheprog.GetRequest) (*cacheprog.GetResponse, error) {
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(request.ActionID)),
	})
	switch {
	case errors.Is(err, nil):
	case isNotFound(err):
		return nil, cacheprog.ErrNotFound
	default:
		return nil, fmt.Errorf("get object: %w", err)
	}

	outputID, err := s.getOutputID(object)
	if err != nil {
		return nil, fmt.Errorf("get outputID: %w", err)
	}

	uncompressedSize, err := s.getUncompressedSize(object)
	if err != nil {
		return nil, fmt.Errorf("get uncompressed size: %w", err)
	}

	return &cacheprog.GetResponse{
		OutputID:             outputID,
		ModTime:              *object.LastModified,
		Size:                 *object.ContentLength,
		Body:                 object.Body,
		CompressionAlgorithm: object.Metadata[compressionAlgorithmMetadataKey],
		UncompressedSize:     cmp.Or(uncompressedSize, *object.ContentLength),
	}, nil
}

func (s *S3) Put(ctx context.Context, request *cacheprog.PutRequest) (*cacheprog.PutResponse, error) {
	// check if object already exists
	// head requests are cheaper than put requests
	headResp, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:  aws.String(s.bucket),
		Key:     aws.String(s.key(request.ActionID)),
		IfMatch: aws.String(hex.EncodeToString(request.MD5Sum)),
	})
	if err == nil && maps.Equal(headResp.Metadata, s.makeMetadata(request)) {
		slog.Debug("Object already exists in S3", "key", s.key(request.ActionID))
		_, err = io.Copy(io.Discard, request.Body)
		return &cacheprog.PutResponse{}, err
	}

	_, err = s.client.PutObject(
		ctx,
		&s3.PutObjectInput{
			Bucket:            aws.String(s.bucket),
			Key:               aws.String(s.key(request.ActionID)),
			Body:              request.Body,
			ContentLength:     aws.Int64(request.Size),
			Expires:           s.objectExpiration(),
			Metadata:          s.makeMetadata(request),
			ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
			ChecksumSHA256:    aws.String(base64.StdEncoding.EncodeToString(request.Sha256Sum)),
		},
		s3.WithAPIOptions(func(s *middleware.Stack) error {
			// Trick signing middleware to use precalculated SHA256 checksum
			// This needed because sometimes body is not seekable and client fails to calculate checksum
			return addPropagateSHA256Middleware(s, request.Sha256Sum)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("put object: %w", err)
	}

	return &cacheprog.PutResponse{}, nil
}

func (s *S3) key(actionID []byte) string {
	return path.Join(s.prefix, hex.EncodeToString(actionID))
}

func isNotFound(err error) bool {
	var se smithy.APIError
	if errors.As(err, &se) {
		code := se.ErrorCode()
		if code == "AccessDenied" || code == "NoSuchKey" {
			return true
		}
	}

	return false
}

func (*S3) getOutputID(object *s3.GetObjectOutput) ([]byte, error) {
	rawID, ok := object.Metadata[outputMetadataKey]
	if !ok {
		return nil, fmt.Errorf("metadata %q not found", outputMetadataKey)
	}

	return base64.StdEncoding.DecodeString(rawID)
}

func (*S3) getUncompressedSize(object *s3.GetObjectOutput) (int64, error) {
	size, ok := object.Metadata[uncompressedSizeMetadataKey]
	if !ok {
		return 0, nil
	}
	return strconv.ParseInt(size, 10, 64)
}

func (s *S3) objectExpiration() *time.Time {
	if s.lifetime <= 0 {
		return nil
	}

	return aws.Time(time.Now().Add(s.lifetime))
}

func (*S3) makeMetadata(request *cacheprog.PutRequest) map[string]string {
	ret := map[string]string{
		outputMetadataKey:               base64.StdEncoding.EncodeToString(request.OutputID),
		compressionAlgorithmMetadataKey: request.CompressionAlgorithm,
	}
	if request.UncompressedSize > 0 {
		ret[uncompressedSizeMetadataKey] = fmt.Sprintf("%d", request.UncompressedSize)
	}
	return ret
}

type minioEndpointResolver struct {
	url *url.URL
}

func (m *minioEndpointResolver) ResolveEndpoint(_ context.Context, params s3.EndpointParameters) (transport.Endpoint, error) {
	u := *m.url
	u.Path = path.Join(u.Path, *params.Bucket)
	return transport.Endpoint{URI: u}, nil
}

type propagateSHA256Middleware struct {
	sha256Sum []byte
}

func (m *propagateSHA256Middleware) ID() string {
	return "PropagateSHA256Middleware"
}

func (m *propagateSHA256Middleware) HandleInitialize(ctx context.Context, input middleware.InitializeInput, next middleware.InitializeHandler) (
	out middleware.InitializeOutput, metadata middleware.Metadata, err error,
) {
	return next.HandleInitialize(signer.SetPayloadHash(ctx, hex.EncodeToString(m.sha256Sum)), input)
}

func addPropagateSHA256Middleware(stack *middleware.Stack, sha256Sum []byte) error {
	return stack.Initialize.Add(&propagateSHA256Middleware{sha256Sum: sha256Sum}, middleware.Before)
}

func templatePrefix(prefix string) (string, error) {
	return fasttemplate.ExecuteFuncStringWithErr(prefix, "{%", "%}",
		func(w io.Writer, tag string) (int, error) {
			tag = strings.TrimSpace(tag)

			envName, isEnv := strings.CutPrefix("env.", tag)
			if isEnv {
				return io.WriteString(w, os.Getenv(envName))
			}

			switch tag {
			case "GOOS":
				return io.WriteString(w, cmp.Or(os.Getenv("GOOS"), runtime.GOOS))
			case "GOARCH":
				return io.WriteString(w, cmp.Or(os.Getenv("GOARCH"), runtime.GOARCH))
			default:
				return 0, fmt.Errorf("unknown template tag: %s", tag)
			}
		})
}

func chainCredentialsProviders(providers ...aws.CredentialsProvider) aws.CredentialsProvider {
	return aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		var errs []error

		for _, provider := range providers {
			creds, err := provider.Retrieve(ctx)
			if err == nil {
				return creds, nil
			}
			errs = append(errs, err)
		}

		return aws.Credentials{}, fmt.Errorf("failed to retrieve credentials: %w", errors.Join(errs...))
	})
}
