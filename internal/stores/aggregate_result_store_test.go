package stores

import (
	"bytes"
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

func TestNewAggregateResultStore(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewAggregateResultStore(mockFileStorage)

	assert.NotNil(t, store)
}

func TestAggregateResultStore_Upsert_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewAggregateResultStore(mockFileStorage)

	ctx := context.Background()
	windowStart := time.Date(2025, 12, 28, 18, 3, 0, 0, time.UTC)
	aggregateResult := &models.WindowAggregateResult{
		CustomerID:  "cus-axon",
		WindowStart: windowStart,
		WindowSize:  models.WindowMinute,
		RequestsByPath: map[string]int64{
			"GET /":      4000,
			"GET /about": 4000,
		},
		RequestsByUserAgent: map[string]int64{
			"Chrome":  4000,
			"Firefox": 4000,
		},
	}

	expectedKey := "aggregate-results/cus-axon/20251228T1803Z.json"
	expectedJSON, _ := json.Marshal(aggregateResult)

	mockFileStorage.EXPECT().
		Put(ctx, expectedKey, gomock.Any(), filestorages.PutOptions{AllowOverwrite: true}).
		DoAndReturn(func(ctx context.Context, key string, r io.Reader, opts filestorages.PutOptions) (*filestorages.PutResult, error) {
			data, err := io.ReadAll(r)
			require.NoError(t, err)
			assert.Equal(t, expectedJSON, data)
			return &filestorages.PutResult{FileKey: key}, nil
		})

	err := store.Upsert(ctx, aggregateResult)
	assert.NoError(t, err)
}

func TestAggregateResultStore_Upsert_PutError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewAggregateResultStore(mockFileStorage)

	ctx := context.Background()
	windowStart := time.Date(2025, 12, 28, 18, 3, 0, 0, time.UTC)
	aggregateResult := &models.WindowAggregateResult{
		CustomerID:  "cus-axon",
		WindowStart: windowStart,
		WindowSize:  models.WindowMinute,
		RequestsByPath: map[string]int64{
			"GET /": 4000,
		},
		RequestsByUserAgent: map[string]int64{
			"Chrome": 4000,
		},
	}

	expectedKey := "aggregate-results/cus-axon/20251228T1803Z.json"
	putError := errors.New("storage error")

	mockFileStorage.EXPECT().
		Put(ctx, expectedKey, gomock.Any(), filestorages.PutOptions{AllowOverwrite: true}).
		Return(nil, putError)

	err := store.Upsert(ctx, aggregateResult)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to put aggregate result")
	assert.Contains(t, err.Error(), "storage error")
}

func TestAggregateResultStore_Get_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewAggregateResultStore(mockFileStorage)

	ctx := context.Background()
	windowStart := time.Date(2025, 12, 28, 18, 3, 0, 0, time.UTC)
	expectedResult := &models.WindowAggregateResult{
		CustomerID:  "cus-axon",
		WindowStart: windowStart,
		WindowSize:  models.WindowMinute,
		RequestsByPath: map[string]int64{
			"GET /":      4000,
			"GET /about": 4000,
		},
		RequestsByUserAgent: map[string]int64{
			"Chrome":  4000,
			"Firefox": 4000,
		},
	}

	expectedKey := "aggregate-results/cus-axon/20251228T1803Z.json"
	jsonData, _ := json.Marshal(expectedResult)
	readCloser := io.NopCloser(bytes.NewReader(jsonData))

	mockFileStorage.EXPECT().
		Get(ctx, expectedKey).
		Return(readCloser, nil)

	result, err := store.Get(ctx, "cus-axon", windowStart, models.WindowMinute)
	require.NoError(t, err)
	assert.Equal(t, expectedResult.CustomerID, result.CustomerID)
	assert.Equal(t, expectedResult.WindowStart, result.WindowStart)
	assert.Equal(t, expectedResult.WindowSize, result.WindowSize)
	assert.Equal(t, expectedResult.RequestsByPath, result.RequestsByPath)
	assert.Equal(t, expectedResult.RequestsByUserAgent, result.RequestsByUserAgent)
}

func TestAggregateResultStore_Get_FileNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewAggregateResultStore(mockFileStorage)

	ctx := context.Background()
	windowStart := time.Date(2025, 12, 28, 18, 3, 0, 0, time.UTC)
	expectedKey := "aggregate-results/cus-axon/20251228T1803Z.json"

	mockFileStorage.EXPECT().
		Get(ctx, expectedKey).
		Return(nil, filestorages.ErrFileNotFound)

	result, err := store.Get(ctx, "cus-axon", windowStart, models.WindowMinute)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "cus-axon", result.CustomerID)
	assert.Equal(t, windowStart, result.WindowStart)
	assert.Equal(t, models.WindowMinute, result.WindowSize)
	assert.NotNil(t, result.RequestsByPath)
	assert.NotNil(t, result.RequestsByUserAgent)
	assert.Empty(t, result.RequestsByPath)
	assert.Empty(t, result.RequestsByUserAgent)
}

func TestAggregateResultStore_Get_StorageError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewAggregateResultStore(mockFileStorage)

	ctx := context.Background()
	windowStart := time.Date(2025, 12, 28, 18, 3, 0, 0, time.UTC)
	expectedKey := "aggregate-results/cus-axon/20251228T1803Z.json"
	storageError := errors.New("storage error")

	mockFileStorage.EXPECT().
		Get(ctx, expectedKey).
		Return(nil, storageError)

	result, err := store.Get(ctx, "cus-axon", windowStart, models.WindowMinute)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get aggregate result")
	assert.Contains(t, err.Error(), "storage error")
}

func TestAggregateResultStore_Get_ReadError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewAggregateResultStore(mockFileStorage)

	ctx := context.Background()
	windowStart := time.Date(2025, 12, 28, 18, 3, 0, 0, time.UTC)
	expectedKey := "aggregate-results/cus-axon/20251228T1803Z.json"

	// Create a ReadCloser that will fail on Read
	readCloser := io.NopCloser(&errorReader{err: errors.New("read error")})

	mockFileStorage.EXPECT().
		Get(ctx, expectedKey).
		Return(readCloser, nil)

	result, err := store.Get(ctx, "cus-axon", windowStart, models.WindowMinute)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read aggregate result")
	assert.Contains(t, err.Error(), "read error")
}

func TestAggregateResultStore_Get_UnmarshalError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewAggregateResultStore(mockFileStorage)

	ctx := context.Background()
	windowStart := time.Date(2025, 12, 28, 18, 3, 0, 0, time.UTC)
	expectedKey := "aggregate-results/cus-axon/20251228T1803Z.json"

	// Invalid JSON
	invalidJSON := []byte(`{"invalid": json}`)
	readCloser := io.NopCloser(bytes.NewReader(invalidJSON))

	mockFileStorage.EXPECT().
		Get(ctx, expectedKey).
		Return(readCloser, nil)

	result, err := store.Get(ctx, "cus-axon", windowStart, models.WindowMinute)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal aggregate result")
}

func TestAggregateResultStore_GetKey_Formatting(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewAggregateResultStore(mockFileStorage)

	ctx := context.Background()

	tests := []struct {
		name        string
		customerID  string
		windowStart time.Time
		windowSize  models.WindowSize
		expectedKey string
	}{
		{
			name:        "minute window",
			customerID:  "cus-axon",
			windowStart: time.Date(2025, 12, 28, 18, 3, 45, 0, time.UTC),
			windowSize:  models.WindowMinute,
			expectedKey: "aggregate-results/cus-axon/20251228T1803Z.json",
		},
		{
			name:        "hour window",
			customerID:  "cus-axon",
			windowStart: time.Date(2025, 12, 28, 18, 30, 0, 0, time.UTC),
			windowSize:  models.WindowHour,
			expectedKey: "aggregate-results/cus-axon/20251228T18Z.json",
		},
		{
			name:        "different customer",
			customerID:  "cus-other",
			windowStart: time.Date(2025, 12, 28, 18, 3, 0, 0, time.UTC),
			windowSize:  models.WindowMinute,
			expectedKey: "aggregate-results/cus-other/20251228T1803Z.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			aggregateResult := &models.WindowAggregateResult{
				CustomerID:  tt.customerID,
				WindowStart: tt.windowStart,
				WindowSize:  tt.windowSize,
				RequestsByPath: map[string]int64{
					"GET /": 1000,
				},
				RequestsByUserAgent: map[string]int64{
					"Chrome": 1000,
				},
			}

			mockFileStorage.EXPECT().
				Put(ctx, tt.expectedKey, gomock.Any(), filestorages.PutOptions{AllowOverwrite: true}).
				Return(&filestorages.PutResult{FileKey: tt.expectedKey}, nil)

			err := store.Upsert(ctx, aggregateResult)
			assert.NoError(t, err)
		})
	}
}

func TestAggregateResultStore_Get_ClosesReadCloser(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewAggregateResultStore(mockFileStorage)

	ctx := context.Background()
	windowStart := time.Date(2025, 12, 28, 18, 3, 0, 0, time.UTC)
	expectedKey := "aggregate-results/cus-axon/20251228T1803Z.json"

	expectedResult := &models.WindowAggregateResult{
		CustomerID:  "cus-axon",
		WindowStart: windowStart,
		WindowSize:  models.WindowMinute,
		RequestsByPath: map[string]int64{
			"GET /": 4000,
		},
		RequestsByUserAgent: map[string]int64{
			"Chrome": 4000,
		},
	}

	jsonData, _ := json.Marshal(expectedResult)
	readCloser := &closableReader{Reader: bytes.NewReader(jsonData)}

	mockFileStorage.EXPECT().
		Get(ctx, expectedKey).
		Return(readCloser, nil)

	result, err := store.Get(ctx, "cus-axon", windowStart, models.WindowMinute)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, readCloser.closed, "ReadCloser should be closed")
}

// errorReader is a reader that always returns an error
type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (n int, err error) {
	return 0, r.err
}

// closableReader is a ReadCloser that tracks if it was closed
type closableReader struct {
	io.Reader
	closed bool
}

func (r *closableReader) Close() error {
	r.closed = true
	return nil
}

func TestAggregateResultStore_Upsert_HourWindow(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewAggregateResultStore(mockFileStorage)

	ctx := context.Background()
	windowStart := time.Date(2025, 12, 28, 18, 30, 45, 0, time.UTC)
	aggregateResult := &models.WindowAggregateResult{
		CustomerID:  "cus-axon",
		WindowStart: windowStart,
		WindowSize:  models.WindowHour,
		RequestsByPath: map[string]int64{
			"GET /": 10000,
		},
		RequestsByUserAgent: map[string]int64{
			"Chrome": 10000,
		},
	}

	expectedKey := "aggregate-results/cus-axon/20251228T18Z.json"

	mockFileStorage.EXPECT().
		Put(ctx, expectedKey, gomock.Any(), filestorages.PutOptions{AllowOverwrite: true}).
		Return(&filestorages.PutResult{FileKey: expectedKey}, nil)

	err := store.Upsert(ctx, aggregateResult)
	assert.NoError(t, err)
}

func TestAggregateResultStore_Get_InvalidJSONStructure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileStorage := mocks.NewMockFileStorage(ctrl)
	store := NewAggregateResultStore(mockFileStorage)

	ctx := context.Background()
	windowStart := time.Date(2025, 12, 28, 18, 3, 0, 0, time.UTC)
	expectedKey := "aggregate-results/cus-axon/20251228T1803Z.json"

	// Valid JSON but wrong structure (missing required fields)
	invalidJSON := []byte(`{"customerId": "cus-axon"}`)
	readCloser := io.NopCloser(bytes.NewReader(invalidJSON))

	mockFileStorage.EXPECT().
		Get(ctx, expectedKey).
		Return(readCloser, nil)

	result, err := store.Get(ctx, "cus-axon", windowStart, models.WindowMinute)
	// Unmarshal should succeed but result may have zero values
	// Let's check what actually happens - unmarshal should work but fields will be zero
	require.NoError(t, err)
	assert.NotNil(t, result)
	// The unmarshal will succeed but WindowStart will be zero time
	assert.True(t, result.WindowStart.IsZero())
}
