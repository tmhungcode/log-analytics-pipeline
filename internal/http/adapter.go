package http

import (
	"encoding/json"
	"net/http"

	"log-analytics/internal/shared/loggers"
	"log-analytics/internal/shared/svcerrors"
)

// ErrorResponse represents an HTTP error response.
type ErrorResponse struct {
	RequestID        string `json:"requestId"`
	ErrorCategory    string `json:"errorCategory"`
	ErrorCode        string `json:"errorCode"`
	ErrorDescription string `json:"errorDescription"`
}

func errorHandlingAdapter(httpHandler AppHttpHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := httpHandler.Handle(w, r)
		if err == nil {
			return
		}

		svcErr, ok := svcerrors.AsServiceError(err)
		if !ok {
			svcErr = svcerrors.NewInternalErrorUndefined(err)
		}

		// Log internal errors at error level
		if svcErr.IsInternalError() {
			logger := loggers.Ctx(r.Context())

			logger.Error().
				Err(svcErr.Cause).
				Str(loggers.FieldErrorCode, svcErr.Code).
				Msg("internal error in handler")
		}

		writeErrorResponse(w, r, svcErr)
	}
}

func writeErrorResponse(w http.ResponseWriter, r *http.Request, svcErr *svcerrors.ServiceError) {
	// set serviceError for middlewares
	if appWriter, ok := w.(*appResponseWriter); ok {
		appWriter.SetServiceError(svcErr)
	}

	// write response
	requestID := requestID(r)
	errorResponse := ErrorResponse{
		RequestID:        requestID,
		ErrorCategory:    svcErr.Category,
		ErrorCode:        svcErr.Code,
		ErrorDescription: svcErr.Message,
	}
	logger := loggers.Ctx(r.Context())
	// Log error response at debug level
	logger.Debug().
		Str(loggers.FieldErrorCode, svcErr.Code).
		Str("errorCategory", svcErr.Category).
		Str("errorMessage", svcErr.Message).
		Int("httpStatusCode", svcErr.HttpStatusCode).
		Msg("error response")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(svcErr.HttpStatusCode)

	_ = json.NewEncoder(w).Encode(errorResponse)
}
