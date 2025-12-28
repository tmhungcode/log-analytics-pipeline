package http

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"log-analytics/internal/ingestors"
	ingestormocks "log-analytics/internal/ingestors/mocks"
	"log-analytics/internal/shared/svcerrors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestIngestLogHandler_Handle_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockIngestionService := ingestormocks.NewMockIngestionService(ctrl)
	handler := NewIngestLogHandler(mockIngestionService)

	req := httptest.NewRequest(http.MethodPost, "/logs", bytes.NewReader([]byte(`[]`)))
	req.Header.Set(headerCustomerID, "customer123")
	req.Header.Set(headerIdempotencyKey, "key123")
	req.Header.Set(headerContentType, "application/json")
	rr := httptest.NewRecorder()

	mockIngestionService.EXPECT().
		IngestBatch(
			gomock.Any(),
			"customer123",
			"key123",
			"application/json",
			gomock.Any(),
		).
		Return(&ingestors.IngestResult{}, nil)

	err := handler.Handle(rr, req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, rr.Code)
}

func TestIngestLogHandler_Handle_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockIngestionService := ingestormocks.NewMockIngestionService(ctrl)
	handler := NewIngestLogHandler(mockIngestionService)

	req := httptest.NewRequest(http.MethodPost, "/logs", bytes.NewReader([]byte(`[]`)))
	req.Header.Set(headerCustomerID, "customer123")
	req.Header.Set(headerIdempotencyKey, "key123")
	req.Header.Set(headerContentType, "application/json")
	rr := httptest.NewRecorder()

	expectedErr := svcerrors.NewInvalidArgumentError("TEST_1000", "validation failed", nil)
	mockIngestionService.EXPECT().
		IngestBatch(
			gomock.Any(),
			"customer123",
			"key123",
			"application/json",
			gomock.Any(),
		).
		Return(nil, expectedErr)

	err := handler.Handle(rr, req)

	require.Error(t, err)
	svcErr, ok := svcerrors.AsServiceError(err)
	require.True(t, ok)
	assert.Equal(t, "TEST_1000", svcErr.Code)
	// Status should not be set when error occurs
	assert.Equal(t, http.StatusOK, rr.Code)
}
