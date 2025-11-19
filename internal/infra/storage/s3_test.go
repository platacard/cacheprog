package storage_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/platacard/cacheprog/internal/app/cacheprog"
	"github.com/platacard/cacheprog/internal/infra/storage"
)

type putObjectInputMatcher struct {
	bucket            string
	key               string
	content           []byte
	contentLength     int64
	checksumAlgorithm types.ChecksumAlgorithm
	checksumSHA256    []byte
	metadata          map[string]string
}

func (m putObjectInputMatcher) Matches(x any) bool {
	input, ok := x.(*s3.PutObjectInput)
	if !ok {
		return false
	}

	// Read the body content
	content, err := io.ReadAll(input.Body)
	if err != nil {
		return false
	}
	// Reset the reader for subsequent reads
	input.Body = bytes.NewReader(content)

	// Check all required fields
	if aws.ToString(input.Bucket) != m.bucket ||
		aws.ToString(input.Key) != m.key ||
		!bytes.Equal(content, m.content) ||
		aws.ToInt64(input.ContentLength) != m.contentLength ||
		input.ChecksumAlgorithm != m.checksumAlgorithm ||
		aws.ToString(input.ChecksumSHA256) != base64.StdEncoding.EncodeToString(m.checksumSHA256) {
		return false
	}

	// Check metadata
	if !maps.Equal(input.Metadata, m.metadata) {
		return false
	}

	return true
}

func (m putObjectInputMatcher) String() string {
	return fmt.Sprintf("PutObjectInput with bucket=%q, key=%q, metadata=%v", m.bucket, m.key, m.metadata)
}

func TestS3(t *testing.T) {
	t.Run("Get", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			ctrl := gomock.NewController(t)

			mockClient := storage.NewMockS3Client(ctrl)
			s3Storage := storage.NewS3(storage.S3Params{
				Client:   mockClient,
				Bucket:   "test-bucket",
				Prefix:   "test-prefix",
				Lifetime: time.Hour,
			})

			actionID := []byte("test-action-id")
			outputID := []byte("test-output-id")
			lastModified := time.Now()
			contentLength := int64(100)
			body := io.NopCloser(bytes.NewReader([]byte("test-content")))
			metadata := map[string]string{
				"output_id":             base64.StdEncoding.EncodeToString(outputID),
				"compression_algorithm": "test-algorithm",
				"uncompressed_size":     "100",
			}

			mockClient.EXPECT().GetObject(gomock.Any(), &s3.GetObjectInput{
				Bucket: aws.String("test-bucket"),
				Key:    aws.String("test-prefix/" + hex.EncodeToString(actionID)),
			}).Return(&s3.GetObjectOutput{
				LastModified:  &lastModified,
				ContentLength: aws.Int64(contentLength),
				Body:          body,
				Metadata:      metadata,
			}, nil)

			resp, err := s3Storage.Get(context.Background(), &cacheprog.GetRequest{
				ActionID: actionID,
			})

			require.NoError(t, err)
			assert.Equal(t, outputID, resp.OutputID)
			assert.Equal(t, lastModified, resp.ModTime)
			assert.Equal(t, contentLength, resp.Size)
			assert.Equal(t, body, resp.Body)
			assert.Equal(t, "test-algorithm", resp.CompressionAlgorithm)
			assert.Equal(t, contentLength, resp.UncompressedSize)
		})

		t.Run("success, not compressed", func(t *testing.T) {
			ctrl := gomock.NewController(t)

			mockClient := storage.NewMockS3Client(ctrl)
			s3Storage := storage.NewS3(storage.S3Params{
				Client:   mockClient,
				Bucket:   "test-bucket",
				Prefix:   "test-prefix",
				Lifetime: time.Hour,
			})

			actionID := []byte("test-action-id")
			outputID := []byte("test-output-id")
			lastModified := time.Now()
			contentLength := int64(100)
			body := io.NopCloser(bytes.NewReader([]byte("test-content")))
			metadata := map[string]string{
				"output_id": base64.StdEncoding.EncodeToString(outputID),
			}

			mockClient.EXPECT().GetObject(gomock.Any(), &s3.GetObjectInput{
				Bucket: aws.String("test-bucket"),
				Key:    aws.String("test-prefix/" + hex.EncodeToString(actionID)),
			}).Return(&s3.GetObjectOutput{
				LastModified:  &lastModified,
				ContentLength: aws.Int64(contentLength),
				Body:          body,
				Metadata:      metadata,
			}, nil)

			resp, err := s3Storage.Get(context.Background(), &cacheprog.GetRequest{
				ActionID: actionID,
			})

			require.NoError(t, err)
			assert.Equal(t, outputID, resp.OutputID)
			assert.Equal(t, lastModified, resp.ModTime)
			assert.Equal(t, contentLength, resp.Size)
			assert.Equal(t, body, resp.Body)
			assert.Empty(t, resp.CompressionAlgorithm)
			assert.Equal(t, contentLength, resp.UncompressedSize)
		})

		t.Run("not found", func(t *testing.T) {
			ctrl := gomock.NewController(t)

			mockClient := storage.NewMockS3Client(ctrl)
			s3Storage := storage.NewS3(storage.S3Params{
				Client:   mockClient,
				Bucket:   "test-bucket",
				Prefix:   "test-prefix",
				Lifetime: time.Hour,
			})

			actionID := []byte("test-action-id")

			mockClient.EXPECT().GetObject(gomock.Any(), gomock.Any()).Return(nil, &smithy.GenericAPIError{
				Code: "NoSuchKey",
			})

			_, err := s3Storage.Get(context.Background(), &cacheprog.GetRequest{
				ActionID: actionID,
			})

			assert.ErrorIs(t, err, cacheprog.ErrNotFound)
		})

		t.Run("missing output ID", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			mockClient := storage.NewMockS3Client(ctrl)
			s3Storage := storage.NewS3(storage.S3Params{
				Client:   mockClient,
				Bucket:   "test-bucket",
				Prefix:   "test-prefix",
				Lifetime: time.Hour,
			})

			actionID := []byte("test-action-id")

			mockClient.EXPECT().GetObject(gomock.Any(), gomock.Any()).Return(&s3.GetObjectOutput{
				Metadata: map[string]string{},
			}, nil)

			_, err := s3Storage.Get(context.Background(), &cacheprog.GetRequest{
				ActionID: actionID,
			})

			assert.Error(t, err)
		})
	})

	t.Run("Put", func(t *testing.T) {
		t.Run("success new object", func(t *testing.T) {
			ctrl := gomock.NewController(t)

			mockClient := storage.NewMockS3Client(ctrl)
			s3Storage := storage.NewS3(storage.S3Params{
				Client:   mockClient,
				Bucket:   "test-bucket",
				Prefix:   "test-prefix",
				Lifetime: time.Hour,
			})

			actionID := []byte("test-action-id")
			outputID := []byte("test-output-id")
			content := []byte("test-content")
			md5Sum := []byte("test-md5")
			sha256Sum := []byte("test-sha256")

			mockClient.EXPECT().HeadObject(gomock.Any(), &s3.HeadObjectInput{
				Bucket:  aws.String("test-bucket"),
				Key:     aws.String("test-prefix/" + hex.EncodeToString(actionID)),
				IfMatch: aws.String(hex.EncodeToString(md5Sum)),
			}).Return(nil, &smithy.GenericAPIError{Code: "NoSuchKey"})

			mockClient.EXPECT().PutObject(
				gomock.Any(),
				putObjectInputMatcher{
					bucket:            "test-bucket",
					key:               "test-prefix/" + hex.EncodeToString(actionID),
					content:           content,
					contentLength:     int64(len(content)),
					checksumAlgorithm: types.ChecksumAlgorithmSha256,
					checksumSHA256:    sha256Sum,
					metadata: map[string]string{
						"output_id":             base64.StdEncoding.EncodeToString(outputID),
						"compression_algorithm": "test-algorithm",
						"uncompressed_size":     "100",
					},
				},
				gomock.Any(),
			).Return(&s3.PutObjectOutput{}, nil)

			_, err := s3Storage.Put(context.Background(), &cacheprog.PutRequest{
				ActionID:             actionID,
				OutputID:             outputID,
				Body:                 bytes.NewReader(content),
				Size:                 int64(len(content)),
				MD5Sum:               md5Sum,
				Sha256Sum:            sha256Sum,
				CompressionAlgorithm: "test-algorithm",
				UncompressedSize:     100,
			})

			require.NoError(t, err)
		})

		t.Run("object exists with same metadata", func(t *testing.T) {
			ctrl := gomock.NewController(t)

			mockClient := storage.NewMockS3Client(ctrl)
			s3Storage := storage.NewS3(storage.S3Params{
				Client:   mockClient,
				Bucket:   "test-bucket",
				Prefix:   "test-prefix",
				Lifetime: time.Hour,
			})

			actionID := []byte("test-action-id")
			outputID := []byte("test-output-id")
			content := []byte("test-content")
			md5Sum := []byte("test-md5")
			sha256Sum := []byte("test-sha256")
			metadata := map[string]string{
				"output_id":             base64.StdEncoding.EncodeToString(outputID),
				"compression_algorithm": "test-algorithm",
				"uncompressed_size":     "100",
			}

			mockClient.EXPECT().HeadObject(gomock.Any(), gomock.Any()).Return(&s3.HeadObjectOutput{
				Metadata: metadata,
			}, nil)

			_, err := s3Storage.Put(context.Background(), &cacheprog.PutRequest{
				ActionID:             actionID,
				OutputID:             outputID,
				Body:                 bytes.NewReader(content),
				Size:                 int64(len(content)),
				MD5Sum:               md5Sum,
				Sha256Sum:            sha256Sum,
				CompressionAlgorithm: "test-algorithm",
				UncompressedSize:     100,
			})

			require.NoError(t, err)
		})

		t.Run("put error", func(t *testing.T) {
			ctrl := gomock.NewController(t)

			mockClient := storage.NewMockS3Client(ctrl)
			s3Storage := storage.NewS3(storage.S3Params{
				Client:   mockClient,
				Bucket:   "test-bucket",
				Prefix:   "test-prefix",
				Lifetime: time.Hour,
			})

			actionID := []byte("test-action-id")
			outputID := []byte("test-output-id")
			content := []byte("test-content")
			md5Sum := []byte("test-md5")
			sha256Sum := []byte("test-sha256")

			mockClient.EXPECT().HeadObject(gomock.Any(), gomock.Any()).Return(nil, errors.New("head error"))
			mockClient.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("put error"))

			_, err := s3Storage.Put(context.Background(), &cacheprog.PutRequest{
				ActionID:             actionID,
				OutputID:             outputID,
				Body:                 bytes.NewReader(content),
				Size:                 int64(len(content)),
				MD5Sum:               md5Sum,
				Sha256Sum:            sha256Sum,
				CompressionAlgorithm: "test-algorithm",
				UncompressedSize:     100,
			})

			assert.Error(t, err)
		})
	})
}
