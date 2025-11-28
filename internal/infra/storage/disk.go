package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	mathRand "math/rand/v2"
	"os"
	"path/filepath"
	"time"

	"github.com/platacard/cacheprog/internal/app/cacheprog"
)

// metaEntry is the metadata that Disk stores on disk for an ActionID.
type metaEntry struct {
	Version  int       `json:"version"`
	OutputID []byte    `json:"outputID"`
	Size     int64     `json:"size"`
	Time     time.Time `json:"time"`
}

// Disk is a simple implementation of the disk-backed object storage.
// It's main purpose of short-term storing of build artifacts.
// So it doesn't have expiration policy and doesn't support any kind of garbage collection.
// It stores two files for each ActionID: .meta and .data.
// .meta file contains metadata in JSON format.
// .data file contains raw data.
// To reduce amount of files in a single directory,
//
//	it uses first byte of ActionID as a subdirectory name in base directory.
type Disk struct {
	root DiskRoot
}

func NewDisk(root DiskRoot) (*Disk, error) {
	// check if base dir is suitable to create files
	ret := &Disk{root: root}
	for range 1000 {
		var randBytes [16]byte
		if _, err := rand.Read(randBytes[:]); err != nil {
			return nil, fmt.Errorf("generate random bytes: %w", err)
		}

		testFileName := fmt.Sprintf(".cacheprog_test_%x", randBytes)
		err := ret.writeFile(testFileName, int64(len(randBytes)), bytes.NewReader(randBytes[:]))
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			if unlinkErr := root.Remove(testFileName); unlinkErr != nil {
				err = fmt.Errorf("%w; cleanup test file: %w", err, unlinkErr)
			}

			return nil, fmt.Errorf("failed to test directory: %w", err)
		}

		// remove test file
		if err = root.Remove(testFileName); err != nil {
			return nil, fmt.Errorf("failed to remove test file %q: %w", testFileName, err)
		}

		return ret, nil
	}

	return nil, fmt.Errorf("failed to create test file, retry attempts exceeded")
}

func ConfigureDisk(rootDir string) (*Disk, error) {
	var err error

	if rootDir != "" && !filepath.IsAbs(rootDir) {
		slog.Warn("Root directory for disk storage is not absolute which may break in some cases. Trying fallback paths.", "root", rootDir)
		rootDir = ""
	}
	// we do not attempt to use 'user cache directory' because now we don't have pruning mechanism and this may lead to disk bloating
	if rootDir == "" {
		rootDir, err = os.MkdirTemp("", "cacheprog")
		if err != nil {
			return nil, fmt.Errorf("failed to create temporary directory: %w", err)
		}
	}

	slog.Info("Configuring disk storage", "root", rootDir)

	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create root directory: %w", err)
	}

	root, err := NewSystemDiskRoot(rootDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create disk root: %w", err)
	}

	return NewDisk(root)
}

func (d *Disk) GetLocal(_ context.Context, request *cacheprog.LocalGetRequest) (*cacheprog.LocalGetResponse, error) {
	meta, err := d.readMeta(d.metaFilePath(request.ActionID, false))
	if err != nil {
		return nil, fmt.Errorf("read meta: %w", err)
	}

	dataFilePath := d.dataFilePath(request.ActionID, false)
	dataFile, err := d.root.OpenFile(dataFilePath, os.O_RDONLY, 0)
	if dataFile != nil {
		defer dataFile.Close()
	}
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("inconsistent state: meta file exists, but data file is missing")
	}
	if err != nil {
		return nil, fmt.Errorf("open data file: %w", err)
	}

	// check if file has expected size
	stat, err := dataFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat data file: %w", err)
	}

	if stat.Size() != meta.Size {
		return nil, fmt.Errorf("inconsistent state: meta file size %d, but data file size %d",
			meta.Size, stat.Size(),
		)
	}

	return &cacheprog.LocalGetResponse{
		OutputID: meta.OutputID,
		Size:     meta.Size,
		ModTime:  meta.Time,
		DiskPath: d.root.FullPath(dataFilePath),
	}, nil
}

func (d *Disk) GetLocalObject(_ context.Context, request *cacheprog.LocalObjectGetRequest) (*cacheprog.LocalObjectGetResponse, error) {
	// ensure that everything is stored correctly
	meta, err := d.readMeta(d.metaFilePath(request.ActionID, false))
	if err != nil {
		return nil, fmt.Errorf("read meta: %w", err)
	}

	dataFilePath := d.dataFilePath(request.ActionID, false)
	dataFile, err := d.root.OpenFile(dataFilePath, os.O_RDONLY, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("inconsistent state: meta file exists, but data file is missing")
	}
	if err != nil {
		return nil, fmt.Errorf("open data file: %w", err)
	}

	// check if file has expected size
	stat, err := dataFile.Stat()
	if err != nil {
		_ = dataFile.Close()
		return nil, fmt.Errorf("stat data file: %w", err)
	}

	if stat.Size() != meta.Size {
		_ = dataFile.Close()
		return nil, fmt.Errorf("inconsistent state: meta file size %d, but data file size %d",
			meta.Size, stat.Size(),
		)
	}

	return &cacheprog.LocalObjectGetResponse{
		ActionID: request.ActionID,
		OutputID: meta.OutputID,
		Size:     meta.Size,
		Body:     dataFile,
	}, nil
}

func (d *Disk) PutLocal(_ context.Context, request *cacheprog.LocalPutRequest) (*cacheprog.LocalPutResponse, error) {
	err := d.root.Mkdir(d.objectDirName(request.ActionID), 0766)
	if err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, fmt.Errorf("create object dir: %w", err)
	}

	// Perform "atomic create via replace" pattern for two files
	// to guarantee that GetLocal will see consistent state.
	// 1. write temp metadata file
	// 2. write temp data file
	// 3. rename temp data file to data file
	// 4. rename temp metadata file to metadata file (last because it checked first in GetLocal)
	//
	// If error occurs at any step, we should clean up all created files.
	// We do not use 'fsync' after write because 0-sized result is kinda valid for our use case, Go's internal cache also handle it this way.

	metaFilePath := d.metaFilePath(request.ActionID, false)
	var tempMetaFilePath string

	// retry if file already exists
	err = retryOnExist(func() error {
		tempMetaFilePath = d.metaFilePath(request.ActionID, true)
		return d.writeMeta(tempMetaFilePath, &metaEntry{
			Version:  1,
			OutputID: request.OutputID,
			Size:     request.Size,
			Time:     time.Now().UTC(),
		})
	})
	if err != nil {
		return nil, fmt.Errorf("write meta file: %w", err)
	}

	// here we have temporary meta file

	dataFilePath := d.dataFilePath(request.ActionID, false)
	var tempDataFilePath string

	err = retryOnExist(func() error {
		tempDataFilePath = d.dataFilePath(request.ActionID, true)
		return d.writeFile(tempDataFilePath, request.Size, request.Body)
	})
	if err != nil {
		err = fmt.Errorf("write data file: %w", err)
		if unlinkErr := d.root.Remove(tempMetaFilePath); unlinkErr != nil {
			err = fmt.Errorf("%w; cleanup meta: %w", err, unlinkErr)
		}

		return nil, err
	}

	// here we have temporary meta file and temporary data file

	if err = d.root.Rename(tempDataFilePath, dataFilePath); err != nil {
		err = fmt.Errorf("rename data file: %w", err)
		if unlinkErr := d.root.Remove(tempMetaFilePath); unlinkErr != nil {
			err = fmt.Errorf("%w; cleanup meta: %w", err, unlinkErr)
		}
		if unlinkErr := d.root.Remove(tempDataFilePath); unlinkErr != nil {
			err = fmt.Errorf("%w; cleanup data: %w", err, unlinkErr)
		}

		return nil, err
	}

	// here we have temporary meta file and data file

	if err = d.root.Rename(tempMetaFilePath, metaFilePath); err != nil {
		err = fmt.Errorf("rename meta file: %w", err)
		if unlinkErr := d.root.Remove(dataFilePath); unlinkErr != nil {
			err = fmt.Errorf("%w; cleanup data: %w", err, unlinkErr)
		}
		if unlinkErr := d.root.Remove(tempMetaFilePath); unlinkErr != nil {
			err = fmt.Errorf("%w; cleanup meta: %w", err, unlinkErr)
		}

		return nil, err
	}

	// here we have meta file and data file, everything is fine

	return &cacheprog.LocalPutResponse{
		DiskPath: d.root.FullPath(dataFilePath),
	}, nil
}

func (d *Disk) objectDirName(actionID []byte) string {
	return hex.EncodeToString(actionID[:1])
}

func (d *Disk) metaFilePath(actionID []byte, temp bool) string {
	name := hex.EncodeToString(actionID) + "-meta"
	if temp {
		name = d.tempOf(name)
	}
	return filepath.Join(d.objectDirName(actionID), name)
}

func (d *Disk) dataFilePath(actionID []byte, temp bool) string {
	// It's really, REALLY important to NOT have extension here.
	// Extension presence breaks https://pkg.go.dev/golang.org/x/tools/go/packages#Load
	// used by golangci-lint with projects that use CGO.
	// Especially it brokes this part of code: https://cs.opensource.google/go/x/tools/+/refs/tags/v0.34.0:go/packages/golist.go;l=548
	name := hex.EncodeToString(actionID) + "-data"
	if temp {
		name = d.tempOf(name)
	}
	return filepath.Join(d.objectDirName(actionID), name)
}

func (d *Disk) tempOf(name string) string {
	// using Uint64 because it wired directly to runtime random generator
	return fmt.Sprintf(".%s.%d.tmp", name, mathRand.Uint64()) //nolint:gosec // this is not for security
}

func (d *Disk) writeFile(file string, size int64, data io.Reader) error {
	f, err := d.root.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}

	written, err := io.Copy(f, data)
	if err != nil {
		return fmt.Errorf("write to file: %w", err)
	}
	if size > 0 && written != size {
		return fmt.Errorf("write to file: %w", io.ErrShortWrite)
	}

	if err = f.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}

	return nil
}

func (d *Disk) readMeta(file string) (*metaEntry, error) {
	metaFile, err := d.root.OpenFile(file, os.O_RDONLY, 0)
	if metaFile != nil {
		defer metaFile.Close()
	}
	if errors.Is(err, fs.ErrNotExist) {
		return nil, cacheprog.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	var meta metaEntry
	if err = json.NewDecoder(metaFile).Decode(&meta); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	return &meta, nil
}

func (d *Disk) writeMeta(file string, meta *metaEntry) error {
	metaFile, err := d.root.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}

	if err = json.NewEncoder(metaFile).Encode(meta); err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	if err = metaFile.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}

	return nil
}

func retryOnExist(fn func() error) error {
	for range 1000 {
		err := fn()
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		return err
	}
	return fmt.Errorf("retry on exist: %w", fs.ErrExist)
}
