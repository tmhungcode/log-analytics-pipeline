package filestorages

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPut_ValidKey(t *testing.T) {
	t.Parallel()

	storage := newTestStorage(t)
	ctx := context.Background()

	validKeys := []string{
		"file.txt",
		"batches/123.json",
		"nested/deep/path/file.txt",
		"file-with-dashes.txt",
		"file_with_underscores.txt",
		"file.with.dots.txt",
		"subdir/file",
	}

	for _, key := range validKeys {
		t.Run(key, func(t *testing.T) {
			data := "test data"
			reader := strings.NewReader(data)

			result, err := storage.Put(ctx, key, reader, PutOptions{AllowOverwrite: false})
			require.NoError(t, err, "key %q should be valid", key)
			assert.Equal(t, key, result.FileKey)

			// Verify file was created
			fullPath := filepath.Join(storage.(*fileStorage).dir, key)
			content, err := os.ReadFile(fullPath)
			require.NoError(t, err)
			assert.Equal(t, data, string(content))
		})
	}
}

func TestPut_AllowOverwriteFalse_FileExists(t *testing.T) {
	t.Parallel()

	storage := newTestStorage(t)
	ctx := context.Background()

	key := "test.txt"
	data := "initial data"
	reader := strings.NewReader(data)

	// First put
	_, err := storage.Put(ctx, key, reader, PutOptions{AllowOverwrite: false})
	require.NoError(t, err)

	// Second put without overwrite
	newData := "new data"
	newReader := strings.NewReader(newData)
	_, err = storage.Put(ctx, key, newReader, PutOptions{AllowOverwrite: false})
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrFileAlreadyExists)

	// Verify original data is unchanged
	fullPath := filepath.Join(storage.(*fileStorage).dir, key)
	content, err := os.ReadFile(fullPath)
	require.NoError(t, err)
	assert.Equal(t, data, string(content))
}

func TestPut_AllowOverwriteTrue_FileExists(t *testing.T) {
	t.Parallel()

	storage := newTestStorage(t)
	ctx := context.Background()

	key := "test.txt"
	initialData := "initial data"
	reader := strings.NewReader(initialData)

	// First put
	_, err := storage.Put(ctx, key, reader, PutOptions{AllowOverwrite: false})
	require.NoError(t, err)

	// Second put with overwrite
	newData := "new data"
	newReader := strings.NewReader(newData)
	result, err := storage.Put(ctx, key, newReader, PutOptions{AllowOverwrite: true})
	require.NoError(t, err)
	assert.Equal(t, key, result.FileKey)

	// Verify data was overwritten
	fullPath := filepath.Join(storage.(*fileStorage).dir, key)
	content, err := os.ReadFile(fullPath)
	require.NoError(t, err)
	assert.Equal(t, newData, string(content))
}

func TestPut_InvalidKey(t *testing.T) {
	t.Parallel()

	storage := newTestStorage(t)
	ctx := context.Background()

	invalidKeys := []string{
		"",
		"/absolute/path",
		"..",
		"../file.txt",
		"../../etc/passwd",
		"batches/../../etc/passwd",
		"../",
		"a/../..",
		".",
	}

	for _, key := range invalidKeys {
		t.Run(key, func(t *testing.T) {
			reader := strings.NewReader("data")
			_, err := storage.Put(ctx, key, reader, PutOptions{AllowOverwrite: false})
			assert.Error(t, err, "key %q should be invalid", key)
			assert.ErrorIs(t, err, ErrInvalidKey)
		})
	}
}

func TestGet_FileNotFound(t *testing.T) {
	t.Parallel()

	storage := newTestStorage(t)
	ctx := context.Background()

	key := "nonexistent.txt"
	_, err := storage.Get(ctx, key)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrFileNotFound)
}

func TestGet_ReturnsReadCloser(t *testing.T) {
	t.Parallel()

	storage := newTestStorage(t)
	ctx := context.Background()

	key := "test.txt"
	data := "test data"
	reader := strings.NewReader(data)

	_, err := storage.Put(ctx, key, reader, PutOptions{AllowOverwrite: false})
	require.NoError(t, err)

	readCloser, err := storage.Get(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, readCloser)

	// Verify it implements io.ReadCloser
	var _ io.ReadCloser = readCloser

	// Verify we can read from it
	content, err := io.ReadAll(readCloser)
	require.NoError(t, err)
	assert.Equal(t, data, string(content))

	// Verify we can close it
	err = readCloser.Close()
	require.NoError(t, err)
}

func TestPutGet_RoundTrip(t *testing.T) {
	t.Parallel()

	storage := newTestStorage(t)
	ctx := context.Background()

	key := "batches/test-batch.json"
	data := `{"batch_id": "123", "entries": []}`
	reader := strings.NewReader(data)

	// Put
	result, err := storage.Put(ctx, key, reader, PutOptions{AllowOverwrite: false})
	require.NoError(t, err)
	assert.Equal(t, key, result.FileKey)

	// Get
	readCloser, err := storage.Get(ctx, key)
	require.NoError(t, err)
	defer readCloser.Close()

	content, err := io.ReadAll(readCloser)
	require.NoError(t, err)
	assert.Equal(t, data, string(content))
}

func TestPut_LargeData(t *testing.T) {
	t.Parallel()

	storage := newTestStorage(t)
	ctx := context.Background()

	key := "large-file.txt"
	// Create 1MB of data
	data := strings.Repeat("A", 5*1024*1024)
	reader := strings.NewReader(data)

	result, err := storage.Put(ctx, key, reader, PutOptions{AllowOverwrite: false})
	require.NoError(t, err)
	assert.Equal(t, key, result.FileKey)

	// Verify
	readCloser, err := storage.Get(ctx, key)
	require.NoError(t, err)
	defer readCloser.Close()

	content, err := io.ReadAll(readCloser)
	require.NoError(t, err)
	assert.Equal(t, len(data), len(content))
	assert.Equal(t, data, string(content))
}

func newTestStorage(t *testing.T) FileStorage {
	tmpDir := t.TempDir()
	storage, err := NewFileStorage(tmpDir)
	require.NoError(t, err)
	return storage
}
