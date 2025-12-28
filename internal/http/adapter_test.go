package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"log-analytics/internal/shared/svcerrors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorHandlingAdapter_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		err              error
		expectedStatus   int
		expectedCategory string
		expectedCode     string
		expectedMessage  string
	}{
		{
			name:             "InvalidArgument error",
			err:              svcerrors.NewInvalidArgumentError("TEST_1000", "test validation error", nil),
			expectedStatus:   http.StatusBadRequest,
			expectedCategory: "invalid_argument",
			expectedCode:     "TEST_1000",
			expectedMessage:  "test validation error",
		},
		{
			name:             "Internal error",
			err:              svcerrors.NewInternalError("TEST_5000", nil),
			expectedStatus:   http.StatusInternalServerError,
			expectedCategory: "internal",
			expectedCode:     "TEST_5000",
			expectedMessage:  "internal server error",
		},
		{
			name:             "Non-ServiceError",
			err:              assert.AnError,
			expectedStatus:   http.StatusInternalServerError,
			expectedCategory: "internal",
			expectedCode:     "SYS_9001",
			expectedMessage:  "internal server error",
		},
		{
			name:             "ResourceConflict error",
			err:              svcerrors.NewResourceConflictError("TEST_4090", "resource already exists", nil),
			expectedStatus:   http.StatusConflict,
			expectedCategory: "resource_conflict",
			expectedCode:     "TEST_4090",
			expectedMessage:  "resource already exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := errorHandlingAdapter(&testHandler{
				handleFunc: func(w http.ResponseWriter, r *http.Request) error {
					return tt.err
				},
			})

			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			reqID := "test-request-id-" + tt.name
			req.Header.Set(headerRequestID, reqID)

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			assert.Equal(t, tt.expectedStatus, rr.Code)
			assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

			var errorResponse ErrorResponse
			err := json.Unmarshal(rr.Body.Bytes(), &errorResponse)
			require.NoError(t, err)

			assert.Equal(t, reqID, errorResponse.RequestID)
			assert.Equal(t, tt.expectedCategory, errorResponse.ErrorCategory)
			assert.Equal(t, tt.expectedCode, errorResponse.ErrorCode)
			assert.Equal(t, tt.expectedMessage, errorResponse.ErrorDescription)
		})
	}
}

func TestErrorHandlingAdapter_NoError(t *testing.T) {
	t.Parallel()

	handler := errorHandlingAdapter(&testHandler{
		handleFunc: func(w http.ResponseWriter, r *http.Request) error {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("success"))
			return nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "success", rr.Body.String())
}

// testHandler wraps a function to implement AppHttpHandler interface for testing
type testHandler struct {
	handleFunc func(w http.ResponseWriter, r *http.Request) error
}

func (h *testHandler) Handle(w http.ResponseWriter, r *http.Request) error {
	return h.handleFunc(w, r)
}
