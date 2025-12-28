package http

import (
	"log-analytics/internal/shared/svcerrors"
	"net/http"

	"github.com/go-chi/chi/v5/middleware"
)

// appResponseWriter is a wrapper around the http.ResponseWriter that stores app details for middleware access
type appResponseWriter struct {
	middleware.WrapResponseWriter
	svcError *svcerrors.ServiceError
}

func newAppResponseWriter(w http.ResponseWriter, protoMajor int) *appResponseWriter {
	return &appResponseWriter{
		WrapResponseWriter: middleware.NewWrapResponseWriter(w, protoMajor),
	}
}

func (w *appResponseWriter) SetServiceError(svcError *svcerrors.ServiceError) {
	w.svcError = svcError
}

func (w *appResponseWriter) ErrorCode() string {
	if w.svcError != nil {
		return w.svcError.Code
	}
	return ""
}
