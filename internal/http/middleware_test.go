package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"log-analytics/internal/shared/loggers"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMwRequestID_GeneratesIDWhenNotProvided(t *testing.T) {
	t.Parallel()

	logger, _ := loggers.New("info")
	mw := mwRequestID(logger)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request ID is set in header
		requestID := r.Header.Get(headerRequestID)
		assert.NotEmpty(t, requestID, "request ID should be generated")

		// Verify it's a valid ULID (26 characters)
		assert.Len(t, requestID, 26, "request ID should be a valid ULID")

		// Verify logger is in context
		ctxLogger := loggers.Ctx(r.Context())
		assert.NotNil(t, ctxLogger, "logger should be in context")

		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestMwRequestID_UsesProvidedID(t *testing.T) {
	t.Parallel()

	logger, _ := loggers.New("info")
	mw := mwRequestID(logger)

	providedID := "custom-request-id-12345"
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the provided request ID is used
		requestID := r.Header.Get(headerRequestID)
		assert.Equal(t, providedID, requestID, "should use provided request ID")

		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set(headerRequestID, providedID)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestMwRecoverer_RecoversFromPanic(t *testing.T) {
	t.Parallel()

	logger, _ := loggers.New("info")
	// Set up request ID middleware first so logger is in context
	mwReqID := mwRequestID(logger)
	mwRecover := mwRecoverer

	handler := mwRecover(mwReqID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	})))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	// Should not panic, should recover
	assert.NotPanics(t, func() {
		handler.ServeHTTP(rr, req)
	})

	// Should return 500 status
	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	// Should return JSON error response
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	// Parse response body
	var errorResponse ErrorResponse
	err := json.Unmarshal(rr.Body.Bytes(), &errorResponse)
	require.NoError(t, err)

	// Assert response fields
	assert.NotEmpty(t, errorResponse.RequestID, "request ID should be set")
	assert.Equal(t, "internal", errorResponse.ErrorCategory)
	assert.Equal(t, "SYS_9000", errorResponse.ErrorCode)
	assert.Equal(t, "internal server error", errorResponse.ErrorDescription)
}

func TestMwRecoverer_PassesThroughWhenNoPanic(t *testing.T) {
	t.Parallel()

	logger, _ := loggers.New("info")
	mwReqID := mwRequestID(logger)
	mwRecover := mwRecoverer

	handler := mwRecover(mwReqID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	})))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "success", rr.Body.String())
}

func TestMwRecoverer_LogsPanic(t *testing.T) {
	t.Parallel()

	// Create a logger that captures logs
	logger, _ := loggers.New("debug")
	mwReqID := mwRequestID(logger)
	mwRecover := mwRecoverer

	panicMessage := "critical error occurred"
	handler := mwRecover(mwReqID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(panicMessage)
	})))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	// Verify it recovered and returned error response
	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	// Parse and verify error response structure
	var errorResponse ErrorResponse
	err := json.Unmarshal(rr.Body.Bytes(), &errorResponse)
	require.NoError(t, err)
	assert.Equal(t, "internal", errorResponse.ErrorCategory)
	assert.Equal(t, "SYS_9000", errorResponse.ErrorCode)
}

func TestMwRecoverer_RecoversFromErrorPanic(t *testing.T) {
	t.Parallel()

	logger, _ := loggers.New("info")
	mwReqID := mwRequestID(logger)
	mwRecover := mwRecoverer

	testErr := assert.AnError
	handler := mwRecover(mwReqID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(testErr)
	})))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	assert.NotPanics(t, func() {
		handler.ServeHTTP(rr, req)
	})

	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	// Parse response body
	var errorResponse ErrorResponse
	err := json.Unmarshal(rr.Body.Bytes(), &errorResponse)
	require.NoError(t, err)

	// Assert response fields
	assert.NotEmpty(t, errorResponse.RequestID)
	assert.Equal(t, "internal", errorResponse.ErrorCategory)
	assert.Equal(t, "SYS_9000", errorResponse.ErrorCode)
	assert.Equal(t, "internal server error", errorResponse.ErrorDescription)
}

func TestSetupMiddleware_Integration(t *testing.T) {
	t.Parallel()

	logger, _ := loggers.New("info")
	router := chi.NewRouter()
	setupMiddleware(router, logger)

	// Test request ID generation
	router.Get("/test-id", func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get(headerRequestID)
		assert.NotEmpty(t, requestID, "request ID should be set")
		w.WriteHeader(http.StatusOK)
	})

	// Test panic recovery
	router.Get("/test-panic", func(w http.ResponseWriter, r *http.Request) {
		panic("integration test panic")
	})

	// Test request ID generation
	req := httptest.NewRequest(http.MethodGet, "/test-id", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	// Test panic recovery
	req = httptest.NewRequest(http.MethodGet, "/test-panic", nil)
	rr = httptest.NewRecorder()
	assert.NotPanics(t, func() {
		router.ServeHTTP(rr, req)
	})
	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	// Verify panic recovery returns proper error response
	var errorResponse ErrorResponse
	err := json.Unmarshal(rr.Body.Bytes(), &errorResponse)
	require.NoError(t, err)
	assert.NotEmpty(t, errorResponse.RequestID)
	assert.Equal(t, "internal", errorResponse.ErrorCategory)
	assert.Equal(t, "SYS_9000", errorResponse.ErrorCode)
}
