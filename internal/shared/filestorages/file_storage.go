package filestorages

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var (
	ErrFileNotFound      = errors.New("file not found")
	ErrFileAlreadyExists = errors.New("file already exists")
	ErrInvalidKey        = errors.New("invalid file key")
	ErrInvalidRootDir    = errors.New("invalid root directory")
)

type PutResult struct {
	FileKey string
}

type PutOptions struct {
	AllowOverwrite bool
}

//go:generate mockgen -source=file_storage.go -destination=./mocks/file_storage_mock.go -package=mocks
type FileStorage interface {
	Put(ctx context.Context, key string, r io.Reader, opts PutOptions) (*PutResult, error)
	Get(ctx context.Context, key string) (io.ReadCloser, error)
}

type fileStorage struct {
	dir string
}

func NewFileStorage(rootDir string) (FileStorage, error) {
	if rootDir == "" {
		return nil, fmt.Errorf("%w: root directory cannot be empty", ErrInvalidRootDir)
	}

	absRootDir, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to resolve absolute path: %w", ErrInvalidRootDir, err)
	}

	return &fileStorage{dir: absRootDir}, nil
}

func (s *fileStorage) Put(ctx context.Context, key string, r io.Reader, opts PutOptions) (*PutResult, error) {
	if err := s.validateKey(key); err != nil {
		return nil, err
	}
	if opts.AllowOverwrite {
		return s.putOverwrite(ctx, key, r)
	}
	return s.putNoOverwrite(ctx, key, r)
}

func (s *fileStorage) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := s.validateKey(key); err != nil {
		return nil, err
	}

	fullPath := filepath.Join(s.dir, key)

	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrFileNotFound
		}
		return nil, err
	}

	return file, nil
}

func (s *fileStorage) validateKey(key string) error {
	if key == "" {
		return ErrInvalidKey
	}
	if filepath.IsAbs(key) {
		return ErrInvalidKey
	}
	cleanPath := filepath.Clean(key)
	if cleanPath == ".." || cleanPath == "." {
		return ErrInvalidKey
	}
	if strings.HasPrefix(cleanPath, "..") {
		return ErrInvalidKey
	}
	// Additional check: ensure the resolved path is within the root directory
	fullPath := filepath.Join(s.dir, cleanPath)
	absRoot, err := filepath.Abs(s.dir)
	if err != nil {
		return ErrInvalidKey
	}
	absFull, err := filepath.Abs(fullPath)
	if err != nil {
		return ErrInvalidKey
	}
	rel, err := filepath.Rel(absRoot, absFull)
	if err != nil {
		return ErrInvalidKey
	}
	if strings.HasPrefix(rel, "..") {
		return ErrInvalidKey
	}
	return nil
}

func (s *fileStorage) putOverwrite(ctx context.Context, key string, r io.Reader) (*PutResult, error) {
	finalPath := filepath.Join(s.dir, filepath.Clean(key))
	dir := filepath.Dir(finalPath)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Still write to temp to avoid partial files
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return nil, err
	}
	tmpPath := tmp.Name()
	defer func() { _ = tmp.Close(); _ = os.Remove(tmpPath) }()

	_, err = io.Copy(tmp, r)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}

	if err := tmp.Sync(); err != nil {
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}

	// Atomic replace (POSIX)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return nil, err
	}

	return &PutResult{FileKey: key}, nil
}
func (s *fileStorage) putNoOverwrite(ctx context.Context, key string, r io.Reader) (*PutResult, error) {
	finalPath := filepath.Join(s.dir, filepath.Clean(key))
	dir := filepath.Dir(finalPath)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return nil, err
	}
	tmpPath := tmp.Name()
	defer func() { _ = tmp.Close(); _ = os.Remove(tmpPath) }()

	_, err = io.Copy(tmp, r)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if err := tmp.Sync(); err != nil {
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}

	// Atomic publish-if-not-exists
	if err := os.Link(tmpPath, finalPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, ErrFileAlreadyExists
		}
		return nil, err
	}

	// Remove the temp name; final still points to same inode
	_ = os.Remove(tmpPath)

	return &PutResult{FileKey: key}, nil
}
