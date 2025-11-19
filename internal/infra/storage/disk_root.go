package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

type SystemDiskRoot struct {
	root *os.Root
}

func NewSystemDiskRoot(dir string) (*SystemDiskRoot, error) {
	if dir == "" {
		return nil, fmt.Errorf("dir is empty")
	}

	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	return &SystemDiskRoot{root: root}, nil
}

func (s *SystemDiskRoot) OpenFile(name string, flag int, perm os.FileMode) (DiskFile, error) {
	return s.root.OpenFile(name, flag, perm)
}

func (s *SystemDiskRoot) Rename(oldpath, newpath string) error {
	return s.root.Rename(oldpath, newpath)
}

func (s *SystemDiskRoot) Mkdir(name string, perm os.FileMode) error {
	return s.root.Mkdir(name, perm)
}

func (s *SystemDiskRoot) Remove(name string) error {
	return s.root.Remove(name)
}

func (s *SystemDiskRoot) FullPath(name string) string {
	return filepath.Join(s.root.Name(), name)
}

func (s *SystemDiskRoot) Close() error {
	return s.root.Close()
}
