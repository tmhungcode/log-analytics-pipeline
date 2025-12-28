package http

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"log-analytics/internal/shared/svcerrors"

	"github.com/stretchr/testify/assert"
)

func TestNewAppResponseWriter(t *testing.T) {
	t.Parallel()

	rr := httptest.NewRecorder()
	appWriter := newAppResponseWriter(rr, 1)

	assert.NotNil(t, appWriter)
	assert.Nil(t, appWriter.svcError)
	assert.Equal(t, "", appWriter.ErrorCode())
}

func TestAppResponseWriter_SetServiceError_And_ErrorCode(t *testing.T) {
	t.Parallel()

	rr := httptest.NewRecorder()
	appWriter := newAppResponseWriter(rr, 1)

	// Initially no error
	assert.Equal(t, "", appWriter.ErrorCode())

	// Set InvalidArgument error
	svcErr1 := svcerrors.NewInvalidArgumentError("TEST_1000", "test error", nil)
	appWriter.SetServiceError(svcErr1)
	assert.Equal(t, svcErr1, appWriter.svcError)
	assert.Equal(t, "TEST_1000", appWriter.ErrorCode())

	// Set Internal error
	svcErr2 := svcerrors.NewInternalError("TEST_5000", nil)
	appWriter.SetServiceError(svcErr2)
	assert.Equal(t, svcErr2, appWriter.svcError)
	assert.Equal(t, "TEST_5000", appWriter.ErrorCode())

	// Clear error by setting nil
	appWriter.SetServiceError(nil)
	assert.Nil(t, appWriter.svcError)
	assert.Equal(t, "", appWriter.ErrorCode())
}

func TestAppResponseWriter_WrapsResponseWriter(t *testing.T) {
	t.Parallel()

	rr := httptest.NewRecorder()
	appWriter := newAppResponseWriter(rr, 1)

	// Test WriteHeader and Status tracking
	appWriter.WriteHeader(http.StatusCreated)
	assert.Equal(t, http.StatusCreated, appWriter.Status())
	assert.Equal(t, http.StatusCreated, rr.Code)

	// Test Write and body content
	appWriter.Write([]byte("test body"))
	assert.Equal(t, "test body", rr.Body.String())
	assert.Equal(t, http.StatusCreated, appWriter.Status()) // Status should not change after Write

	// Test WriteHeader with different status
	rr2 := httptest.NewRecorder()
	appWriter2 := newAppResponseWriter(rr2, 1)
	appWriter2.WriteHeader(http.StatusNotFound)
	assert.Equal(t, http.StatusNotFound, appWriter2.Status())
	assert.Equal(t, http.StatusNotFound, rr2.Code)

	// Write should not change status
	appWriter2.Write([]byte("not found"))
	assert.Equal(t, http.StatusNotFound, appWriter2.Status())
	assert.Equal(t, http.StatusNotFound, rr2.Code)
}
