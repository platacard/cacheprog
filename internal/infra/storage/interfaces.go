package storage

import (
	"context"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

//go:generate go tool go.uber.org/mock/mockgen -destination=mocks_world_test.go -package=$GOPACKAGE -source=$GOFILE

type S3Client interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type (
	DiskFile interface {
		io.ReadWriteSeeker
		io.Closer
		Stat() (os.FileInfo, error)
	}

	DiskRoot interface {
		OpenFile(name string, flag int, perm os.FileMode) (DiskFile, error)
		Rename(oldpath, newpath string) error
		Mkdir(name string, perm os.FileMode) error
		Remove(name string) error
		FullPath(name string) string
	}
)
