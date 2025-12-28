package ingestors_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"log-analytics/internal/ingestors"
	ingestormocks "log-analytics/internal/ingestors/mocks"
	"log-analytics/internal/models"
	"log-analytics/internal/shared/svcerrors"
	"log-analytics/internal/stores"
	storemocks "log-analytics/internal/stores/mocks"
	streammocks "log-analytics/internal/streams/mocks"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestIngestBatch_ErrValidationFailed_InvalidFormat(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	batchSummarizer := ingestormocks.NewMockBatchSummarizer(ctrl)
	batchStore := storemocks.NewMockLogBatchStore(ctrl)
	partialInsightProducer := streammocks.NewMockPartialInsightProducer(ctrl)
	service := ingestors.NewIngestionService(batchSummarizer, batchStore, partialInsightProducer)

	ctx := context.Background()
	body := bytes.NewReader([]byte(`{}`))
	result, err := service.IngestBatch(ctx, "customer1", "key1", "xml", body)

	require.Error(t, err, "expected error")
	svcErr, ok := svcerrors.AsServiceError(err)
	require.True(t, ok, "expected ServiceError")
	assert.Equal(t, "ING_1000", svcErr.Code)
	assert.Equal(t, "invalid_argument", svcErr.Category)
	assert.Nil(t, result, "expected nil result on error")
}

func TestIngestBatch_ErrValidationFailed_InvalidJSON(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	batchSummarizer := ingestormocks.NewMockBatchSummarizer(ctrl)
	batchStore := storemocks.NewMockLogBatchStore(ctrl)
	partialInsightProducer := streammocks.NewMockPartialInsightProducer(ctrl)
	service := ingestors.NewIngestionService(batchSummarizer, batchStore, partialInsightProducer)

	ctx := context.Background()
	invalidJSON := bytes.NewReader([]byte(`{invalid json}`))
	result, err := service.IngestBatch(ctx, "customer1", "key1", "json", invalidJSON)

	require.Error(t, err, "expected error")
	svcErr, ok := svcerrors.AsServiceError(err)
	require.True(t, ok, "expected ServiceError")
	assert.Equal(t, "ING_1000", svcErr.Code)
	assert.Equal(t, "invalid_argument", svcErr.Category)
	assert.Nil(t, result, "expected nil result on error")
}

func TestIngestBatch_ErrValidationFailed_BatchTooLarge(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	batchSummarizer := ingestormocks.NewMockBatchSummarizer(ctrl)
	batchStore := storemocks.NewMockLogBatchStore(ctrl)
	partialInsightProducer := streammocks.NewMockPartialInsightProducer(ctrl)
	service := ingestors.NewIngestionService(batchSummarizer, batchStore, partialInsightProducer)

	ctx := context.Background()
	// Create body with size 2*1024*1024 + 1 bytes
	largeBody := make([]byte, 2*1024*1024+1)
	body := bytes.NewReader(largeBody)

	_, err := service.IngestBatch(ctx, "customer1", "key1", "json", body)

	require.Error(t, err, "expected error")
	svcErr, ok := svcerrors.AsServiceError(err)
	require.True(t, ok, "expected ServiceError")
	assert.Equal(t, "ING_1000", svcErr.Code)
	assert.Equal(t, "invalid_argument", svcErr.Category)
	assert.Equal(t, "batch too large: must be <= 2MB", svcErr.Message)
}

func TestIngestBatch_ErrValidationFailed_LogEntryValidation(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	batchSummarizer := ingestormocks.NewMockBatchSummarizer(ctrl)
	batchStore := storemocks.NewMockLogBatchStore(ctrl)
	partialInsightProducer := streammocks.NewMockPartialInsightProducer(ctrl)
	service := ingestors.NewIngestionService(batchSummarizer, batchStore, partialInsightProducer)

	tests := []struct {
		name string
		json string
	}{
		{
			name: "empty entries",
			json: `[]`,
		},
		{
			name: "missing receivedAt",
			json: `[{"method":"GET","path":"/","userAgent":"test"}]`,
		},
		{
			name: "invalid receivedAt format",
			json: `[{"receivedAt":"invalid-time","method":"GET","path":"/","userAgent":"test"}]`,
		},
		{
			name: "missing method",
			json: `[{"receivedAt":"2025-12-21T14:21:00.000Z","path":"/","userAgent":"test"}]`,
		},
		{
			name: "missing path",
			json: `[{"receivedAt":"2025-12-21T14:21:00.000Z","method":"GET","userAgent":"test"}]`,
		},
		{
			name: "missing userAgent",
			json: `[{"receivedAt":"2025-12-21T14:21:00.000Z","method":"GET","path":"/"}]`,
		},
		{
			name: "path exceeds max length",
			json: `[{"receivedAt":"2025-12-21T14:21:00.000Z","method":"GET","path":"` + strings.Repeat("a", 2049) + `","userAgent":"test"}]`,
		},
		{
			name: "userAgent exceeds max length",
			json: `[{"receivedAt":"2025-12-21T14:21:00.000Z","method":"GET","path":"/","userAgent":"` + strings.Repeat("a", 1025) + `"}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			body := bytes.NewReader([]byte(tt.json))
			result, err := service.IngestBatch(ctx, "customer1", "key1", "json", body)

			require.Error(t, err, "expected error")
			svcErr, ok := svcerrors.AsServiceError(err)
			require.True(t, ok, "expected ServiceError")
			assert.Equal(t, "ING_1000", svcErr.Code)
			assert.Nil(t, result, "expected nil result on error")
		})
	}
}

func TestIngestBatch_ErrBatchPutFailed(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name             string
		putError         error
		expectedCode     string
		expectedCategory string
	}{
		{
			name:             "log batch already exists",
			putError:         stores.ErrLogBatchAlreadyExist,
			expectedCode:     "ING_1001",
			expectedCategory: "resource_conflict",
		},
		{
			name:             "log batch put failed",
			putError:         assert.AnError,
			expectedCode:     "ING_9000",
			expectedCategory: "internal",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batchSummarizer := ingestormocks.NewMockBatchSummarizer(ctrl)
			batchStore := storemocks.NewMockLogBatchStore(ctrl)
			partialInsightProducer := streammocks.NewMockPartialInsightProducer(ctrl)

			batchStore.EXPECT().Put(gomock.Any(), gomock.Any()).Return(tt.putError)

			service := ingestors.NewIngestionService(batchSummarizer, batchStore, partialInsightProducer)

			ctx := context.Background()
			validJSON := `[{"receivedAt":"2025-12-21T14:21:00.000Z","method":"GET","path":"/","userAgent":"test"}]`
			body := bytes.NewReader([]byte(validJSON))

			result, err := service.IngestBatch(ctx, "customer1", "key1", "json", body)

			require.Error(t, err, "expected error")
			svcErr, ok := svcerrors.AsServiceError(err)
			require.True(t, ok, "expected ServiceError")
			assert.Equal(t, tt.expectedCode, svcErr.Code)
			assert.Equal(t, tt.expectedCategory, svcErr.Category)
			assert.Nil(t, result, "expected nil result on error")
		})
	}
}

func TestIngestBatch_ErrPartialInsightPublishFailed(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	batchSummarizer := ingestormocks.NewMockBatchSummarizer(ctrl)
	batchStore := storemocks.NewMockLogBatchStore(ctrl)
	partialInsightProducer := streammocks.NewMockPartialInsightProducer(ctrl)

	batchStore.EXPECT().Put(gomock.Any(), gomock.Any()).Return(nil)
	batchSummarizer.EXPECT().Summarize(gomock.Any()).Return(&models.BatchSummary{})
	partialInsightProducer.EXPECT().Produce(gomock.Any(), gomock.Any()).
		Return(assert.AnError)

	service := ingestors.NewIngestionService(batchSummarizer, batchStore, partialInsightProducer)

	ctx := context.Background()
	validJSON := `[{"receivedAt":"2025-12-21T14:21:00.000Z","method":"GET","path":"/","userAgent":"test"}]`
	body := bytes.NewReader([]byte(validJSON))

	result, err := service.IngestBatch(ctx, "customer1", "key1", "json", body)

	require.Error(t, err, "expected error")
	svcErr, ok := svcerrors.AsServiceError(err)
	require.True(t, ok, "expected ServiceError")
	assert.Equal(t, "ING_9001", svcErr.Code)
	assert.Equal(t, "internal", svcErr.Category)
	assert.Nil(t, result, "expected nil result on error")
}

func TestIngestBatch_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	batchSummarizer := ingestormocks.NewMockBatchSummarizer(ctrl)
	batchStore := storemocks.NewMockLogBatchStore(ctrl)
	partialInsightProducer := streammocks.NewMockPartialInsightProducer(ctrl)

	var storedBatch *models.LogBatch
	var publishedSummary *models.BatchSummary
	var summarizedBatch *models.LogBatch

	batchStore.EXPECT().Put(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, batch *models.LogBatch) {
			storedBatch = batch
		}).
		Return(nil)

	batchSummarizer.EXPECT().Summarize(gomock.Any()).
		Do(func(batch *models.LogBatch) {
			summarizedBatch = batch
		}).
		Return(&models.BatchSummary{
			BatchID:       "key1",
			CustomerID:    "customer1",
			ByWindowStart: map[string]models.WindowAggregates{},
		})

	partialInsightProducer.EXPECT().Produce(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, batchSummary *models.BatchSummary) {
			publishedSummary = batchSummary
		}).
		Return(nil)

	service := ingestors.NewIngestionService(batchSummarizer, batchStore, partialInsightProducer)

	ctx := context.Background()
	customerID := "customer1"
	idempotencyKey := "key1"
	validJSON := `[{"receivedAt":"2025-12-21T14:21:00.000Z","method":"GET","path":"/","userAgent":"test"}]`
	body := bytes.NewReader([]byte(validJSON))

	result, err := service.IngestBatch(ctx, customerID, idempotencyKey, "json", body)

	require.NoError(t, err, "unexpected error")
	assert.NotNil(t, result, "expected non-nil result")

	// Verify parameters were passed correctly
	assert.NotNil(t, storedBatch)
	assert.NotNil(t, summarizedBatch)
	assert.NotNil(t, publishedSummary)
	assert.Equal(t, "key1", storedBatch.BatchID)
	assert.Equal(t, "customer1", storedBatch.CustomerID)
	assert.Equal(t, "key1", publishedSummary.BatchID)
	assert.Equal(t, "customer1", publishedSummary.CustomerID)
}
