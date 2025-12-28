package stores

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"log-analytics/internal/models"
	"log-analytics/internal/shared/filestorages"
	"log-analytics/internal/shared/filestorages/mocks"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestNewLogBatchStore(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewLogBatchStore(mockFileStorage)

	assert.NotNil(t, store)
}

func TestLogBatchStore_Put_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewLogBatchStore(mockFileStorage)

	ctx := context.Background()
	logBatch := &models.LogBatch{
		BatchID:    "batch-123",
		CustomerID: "cus-axon",
		Entries: []*models.LogEntry{
			{
				ReceivedAt: time.Date(2025, 12, 28, 18, 3, 15, 0, time.UTC),
				Method:     "GET",
				Path:       "/",
				UserAgent:  "Chrome",
			},
			{
				ReceivedAt: time.Date(2025, 12, 28, 18, 3, 16, 0, time.UTC),
				Method:     "GET",
				Path:       "/about",
				UserAgent:  "Firefox",
			},
		},
	}

	expectedKey := "raw-batches/cus-axon/batch-123.json"
	expectedJSON, _ := json.Marshal(logBatch)

	mockFileStorage.EXPECT().
		Put(ctx, expectedKey, gomock.Any(), filestorages.PutOptions{AllowOverwrite: false}).
		DoAndReturn(func(ctx context.Context, key string, r io.Reader, opts filestorages.PutOptions) (*filestorages.PutResult, error) {
			data, err := io.ReadAll(r)
			require.NoError(t, err)
			assert.Equal(t, expectedJSON, data)
			assert.False(t, opts.AllowOverwrite, "AllowOverwrite should be false")
			return &filestorages.PutResult{FileKey: key}, nil
		})

	err := store.Put(ctx, logBatch)
	assert.NoError(t, err)
}

func TestLogBatchStore_Put_FileAlreadyExists(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewLogBatchStore(mockFileStorage)

	ctx := context.Background()
	logBatch := &models.LogBatch{
		BatchID:    "batch-123",
		CustomerID: "cus-axon",
		Entries: []*models.LogEntry{
			{
				ReceivedAt: time.Date(2025, 12, 28, 18, 3, 15, 0, time.UTC),
				Method:     "GET",
				Path:       "/",
				UserAgent:  "Chrome",
			},
		},
	}

	expectedKey := "raw-batches/cus-axon/batch-123.json"

	mockFileStorage.EXPECT().
		Put(ctx, expectedKey, gomock.Any(), filestorages.PutOptions{AllowOverwrite: false}).
		Return(nil, filestorages.ErrFileAlreadyExists)

	err := store.Put(ctx, logBatch)
	assert.Error(t, err)
	assert.Equal(t, ErrLogBatchAlreadyExist, err)
	assert.ErrorIs(t, err, ErrLogBatchAlreadyExist)
}

func TestLogBatchStore_Put_StorageError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewLogBatchStore(mockFileStorage)

	ctx := context.Background()
	logBatch := &models.LogBatch{
		BatchID:    "batch-123",
		CustomerID: "cus-axon",
		Entries: []*models.LogEntry{
			{
				ReceivedAt: time.Date(2025, 12, 28, 18, 3, 15, 0, time.UTC),
				Method:     "GET",
				Path:       "/",
				UserAgent:  "Chrome",
			},
		},
	}

	expectedKey := "raw-batches/cus-axon/batch-123.json"
	storageError := errors.New("storage error")

	mockFileStorage.EXPECT().
		Put(ctx, expectedKey, gomock.Any(), filestorages.PutOptions{AllowOverwrite: false}).
		Return(nil, storageError)

	err := store.Put(ctx, logBatch)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to put log batch")
	assert.Contains(t, err.Error(), "storage error")
	assert.NotErrorIs(t, err, ErrLogBatchAlreadyExist)
}

func TestLogBatchStore_Put_KeyGeneration(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewLogBatchStore(mockFileStorage)

	ctx := context.Background()

	tests := []struct {
		name        string
		customerID  string
		batchID     string
		expectedKey string
	}{
		{
			name:        "standard batch",
			customerID:  "cus-axon",
			batchID:     "batch-123",
			expectedKey: "raw-batches/cus-axon/batch-123.json",
		},
		{
			name:        "different customer",
			customerID:  "cus-other",
			batchID:     "batch-456",
			expectedKey: "raw-batches/cus-other/batch-456.json",
		},
		{
			name:        "different batch ID",
			customerID:  "cus-axon",
			batchID:     "batch-789",
			expectedKey: "raw-batches/cus-axon/batch-789.json",
		},
		{
			name:        "ULID batch ID",
			customerID:  "cus-axon",
			batchID:     "01ARZ3NDEKTSV4RRFFQ69G5FAV",
			expectedKey: "raw-batches/cus-axon/01ARZ3NDEKTSV4RRFFQ69G5FAV.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			logBatch := &models.LogBatch{
				BatchID:    tt.batchID,
				CustomerID: tt.customerID,
				Entries: []*models.LogEntry{
					{
						ReceivedAt: time.Date(2025, 12, 28, 18, 3, 15, 0, time.UTC),
						Method:     "GET",
						Path:       "/",
						UserAgent:  "Chrome",
					},
				},
			}

			mockFileStorage.EXPECT().
				Put(ctx, tt.expectedKey, gomock.Any(), filestorages.PutOptions{AllowOverwrite: false}).
				Return(&filestorages.PutResult{FileKey: tt.expectedKey}, nil)

			err := store.Put(ctx, logBatch)
			assert.NoError(t, err)
		})
	}
}
func TestLogBatchStore_Put_AllowOverwriteFalse(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewLogBatchStore(mockFileStorage)

	ctx := context.Background()
	logBatch := &models.LogBatch{
		BatchID:    "batch-123",
		CustomerID: "cus-axon",
		Entries: []*models.LogEntry{
			{
				ReceivedAt: time.Date(2025, 12, 28, 18, 3, 15, 0, time.UTC),
				Method:     "GET",
				Path:       "/",
				UserAgent:  "Chrome",
			},
		},
	}

	expectedKey := "raw-batches/cus-axon/batch-123.json"

	mockFileStorage.EXPECT().
		Put(ctx, expectedKey, gomock.Any(), filestorages.PutOptions{AllowOverwrite: false}).
		Return(&filestorages.PutResult{FileKey: expectedKey}, nil)

	err := store.Put(ctx, logBatch)
	assert.NoError(t, err)
}

func TestLogBatchStore_Put_InvalidKeyError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewLogBatchStore(mockFileStorage)

	ctx := context.Background()
	logBatch := &models.LogBatch{
		BatchID:    "batch-123",
		CustomerID: "cus-axon",
		Entries: []*models.LogEntry{
			{
				ReceivedAt: time.Date(2025, 12, 28, 18, 3, 15, 0, time.UTC),
				Method:     "GET",
				Path:       "/",
				UserAgent:  "Chrome",
			},
		},
	}

	expectedKey := "raw-batches/cus-axon/batch-123.json"

	mockFileStorage.EXPECT().
		Put(ctx, expectedKey, gomock.Any(), filestorages.PutOptions{AllowOverwrite: false}).
		Return(nil, filestorages.ErrInvalidKey)

	err := store.Put(ctx, logBatch)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to put log batch")
	assert.NotErrorIs(t, err, ErrLogBatchAlreadyExist)
}

func TestLogBatchStore_Put_JSONMarshaling(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewLogBatchStore(mockFileStorage)

	ctx := context.Background()
	logBatch := &models.LogBatch{
		BatchID:    "batch-json-test",
		CustomerID: "cus-axon",
		Entries: []*models.LogEntry{
			{
				ReceivedAt: time.Date(2025, 12, 28, 18, 3, 15, 123456789, time.UTC),
				Method:     "POST",
				Path:       "/api/v1/users",
				UserAgent:  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
			},
			{
				ReceivedAt: time.Date(2025, 12, 28, 18, 3, 16, 987654321, time.UTC),
				Method:     "GET",
				Path:       "/api/v1/products?page=1&limit=10",
				UserAgent:  "curl/7.88.1",
			},
		},
	}

	expectedKey := "raw-batches/cus-axon/batch-json-test.json"
	expectedJSON, _ := json.Marshal(logBatch)

	mockFileStorage.EXPECT().
		Put(ctx, expectedKey, gomock.Any(), filestorages.PutOptions{AllowOverwrite: false}).
		DoAndReturn(func(ctx context.Context, key string, r io.Reader, opts filestorages.PutOptions) (*filestorages.PutResult, error) {
			data, err := io.ReadAll(r)
			require.NoError(t, err)

			// Verify JSON is valid and matches expected
			var unmarshaled models.LogBatch
			err = json.Unmarshal(data, &unmarshaled)
			require.NoError(t, err)
			assert.Equal(t, logBatch.BatchID, unmarshaled.BatchID)
			assert.Equal(t, logBatch.CustomerID, unmarshaled.CustomerID)
			assert.Equal(t, len(logBatch.Entries), len(unmarshaled.Entries))

			assert.Equal(t, expectedJSON, data)
			return &filestorages.PutResult{FileKey: key}, nil
		})

	err := store.Put(ctx, logBatch)
	assert.NoError(t, err)
}
