package storage

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/platacard/cacheprog/internal/app/cacheprog"
)

func TestNewDisk(t *testing.T) {
	testFileMatch := gomock.Cond(func(in string) bool {
		return strings.HasPrefix(in, ".cacheprog_test_")
	})

	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		diskMock := NewMockDiskRoot(ctrl)

		testFileMock := NewMockDiskFile(ctrl)
		diskMock.EXPECT().OpenFile(testFileMatch, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.FileMode(0666)).Return(testFileMock, nil)
		testFileMock.EXPECT().Write(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			return len(p), nil
		}).AnyTimes()
		testFileMock.EXPECT().Close().Return(nil)
		diskMock.EXPECT().Remove(testFileMatch).Return(nil)

		_, err := NewDisk(diskMock)
		require.NoError(t, err)
	})

	t.Run("real disk", func(t *testing.T) {
		_, err := NewSystemDiskRoot(t.TempDir())
		require.NoError(t, err)
	})

	t.Run("retries on existing file", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		diskMock := NewMockDiskRoot(ctrl)

		testFileMock := NewMockDiskFile(ctrl)
		diskMock.EXPECT().OpenFile(testFileMatch, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.FileMode(0666)).Return(nil, os.ErrExist)
		diskMock.EXPECT().OpenFile(testFileMatch, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.FileMode(0666)).Return(testFileMock, nil)
		testFileMock.EXPECT().Write(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			return len(p), nil
		}).AnyTimes()
		testFileMock.EXPECT().Close().Return(nil)
		diskMock.EXPECT().Remove(testFileMatch).Return(nil)

		_, err := NewDisk(diskMock)
		require.NoError(t, err)
	})

	t.Run("error on open file", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		diskMock := NewMockDiskRoot(ctrl)

		diskMock.EXPECT().OpenFile(testFileMatch, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.FileMode(0666)).Return(nil, fmt.Errorf("open file error"))
		diskMock.EXPECT().Remove(testFileMatch).Return(nil)

		_, err := NewDisk(diskMock)
		require.Error(t, err)
	})

	t.Run("error on write", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		diskMock := NewMockDiskRoot(ctrl)

		testFileMock := NewMockDiskFile(ctrl)
		diskMock.EXPECT().OpenFile(testFileMatch, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.FileMode(0666)).Return(testFileMock, nil)
		testFileMock.EXPECT().Write(gomock.Any()).Return(0, fmt.Errorf("write error"))
		diskMock.EXPECT().Remove(testFileMatch).Return(nil)

		_, err := NewDisk(diskMock)
		require.Error(t, err)
	})

	t.Run("error on close", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		diskMock := NewMockDiskRoot(ctrl)

		testFileMock := NewMockDiskFile(ctrl)
		diskMock.EXPECT().OpenFile(testFileMatch, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.FileMode(0666)).Return(testFileMock, nil)
		testFileMock.EXPECT().Write(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			return len(p), nil
		}).AnyTimes()
		testFileMock.EXPECT().Close().Return(fmt.Errorf("close error"))
		diskMock.EXPECT().Remove(testFileMatch).Return(nil)

		_, err := NewDisk(diskMock)
		require.Error(t, err)
	})

	t.Run("error on remove", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		diskMock := NewMockDiskRoot(ctrl)

		testFileMock := NewMockDiskFile(ctrl)
		diskMock.EXPECT().OpenFile(testFileMatch, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.FileMode(0666)).Return(testFileMock, nil)
		testFileMock.EXPECT().Write(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
			return len(p), nil
		}).AnyTimes()
		testFileMock.EXPECT().Close().Return(nil)
		diskMock.EXPECT().Remove(testFileMatch).Return(fmt.Errorf("remove error"))

		_, err := NewDisk(diskMock)
		require.Error(t, err)
	})
}

func TestDisk_GetLocal(t *testing.T) {
	actionID := []byte("test-action-id")
	outputID := []byte("test-output-id")
	size := int64(100)
	modTime := time.Now().UTC()
	testData := bytes.Repeat([]byte("a"), int(size))

	t.Run("success", func(t *testing.T) {
		// Setup real disk
		root, err := NewSystemDiskRoot(t.TempDir())
		require.NoError(t, err)
		disk, err := NewDisk(root)
		require.NoError(t, err)

		// Write test data first using PutLocal
		_, err = disk.PutLocal(context.Background(), &cacheprog.LocalPutRequest{
			ActionID: actionID,
			OutputID: outputID,
			Size:     size,
			Body:     bytes.NewReader(testData),
		})
		require.NoError(t, err)

		// Test GetLocal
		resp, err := disk.GetLocal(context.Background(), &cacheprog.LocalGetRequest{
			ActionID: actionID,
		})

		require.NoError(t, err)
		assert.Equal(t, outputID, resp.OutputID)
		assert.Equal(t, size, resp.Size)
		assert.WithinDuration(t, modTime, resp.ModTime, 2*time.Second)
		assert.NotEmpty(t, resp.DiskPath)

		// Verify file contents
		data, err := os.ReadFile(resp.DiskPath)
		require.NoError(t, err)
		assert.Equal(t, testData, data)
	})

	t.Run("not found", func(t *testing.T) {
		root, err := NewSystemDiskRoot(t.TempDir())
		require.NoError(t, err)
		disk, err := NewDisk(root)
		require.NoError(t, err)

		_, err = disk.GetLocal(context.Background(), &cacheprog.LocalGetRequest{
			ActionID: actionID,
		})

		assert.ErrorIs(t, err, cacheprog.ErrNotFound)
	})
}

func TestDisk_PutLocal(t *testing.T) {
	actionID := []byte("test-action-id")
	outputID := []byte("test-output-id")
	data := []byte("test-data")
	size := int64(len(data))

	t.Run("success", func(t *testing.T) {
		root, err := NewSystemDiskRoot(t.TempDir())
		require.NoError(t, err)
		disk, err := NewDisk(root)
		require.NoError(t, err)

		resp, err := disk.PutLocal(context.Background(), &cacheprog.LocalPutRequest{
			ActionID: actionID,
			OutputID: outputID,
			Size:     size,
			Body:     bytes.NewReader(data),
		})

		require.NoError(t, err)
		assert.NotEmpty(t, resp.DiskPath)

		// Verify the file was written correctly
		storedData, err := os.ReadFile(resp.DiskPath)
		require.NoError(t, err)
		assert.Equal(t, data, storedData)

		// Verify metadata
		metaPath := filepath.Join(filepath.Dir(resp.DiskPath), hex.EncodeToString(actionID)+"-meta")
		metaData, err := os.ReadFile(metaPath)
		require.NoError(t, err)

		var meta metaEntry
		err = json.Unmarshal(metaData, &meta)
		require.NoError(t, err)
		assert.Equal(t, outputID, meta.OutputID)
		assert.Equal(t, size, meta.Size)
		assert.NotZero(t, meta.Time)
	})

	t.Run("wrong size", func(t *testing.T) {
		root, err := NewSystemDiskRoot(t.TempDir())
		require.NoError(t, err)
		disk, err := NewDisk(root)
		require.NoError(t, err)

		_, err = disk.PutLocal(context.Background(), &cacheprog.LocalPutRequest{
			ActionID: actionID,
			OutputID: outputID,
			Size:     size + 1, // Incorrect size
			Body:     bytes.NewReader(data),
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "write to file")
	})
}
