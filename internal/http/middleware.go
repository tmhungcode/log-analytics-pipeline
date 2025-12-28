package http

import (
	"fmt"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"log-analytics/internal/shared/loggers"
	"log-analytics/internal/shared/svcerrors"
	"log-analytics/internal/shared/ulid"

	"github.com/go-chi/chi/v5"
)

func setupMiddleware(router *chi.Mux, httpLogger loggers.Logger) {
	router.Use(mwRequestID(httpLogger))
	router.Use(mwAppResponseWriter)
	router.Use(mwPrometheus)
	router.Use(mwRequestCompletionLog)
	router.Use(mwRecoverer)
}

// mwAppResponseWriter initializes the appResponseWriter once and passes it through the middleware chain.
func mwAppResponseWriter(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		appWriter := newAppResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(appWriter, r)
	})
}

// mwPrometheus provides Prometheus metrics middleware.
// It records HTTP request counts and duration using route patterns instead of raw paths
// to avoid high-cardinality metrics that could overwhelm Prometheus.
func mwPrometheus(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)

		// Get route pattern to avoid high-cardinality raw paths
		routePattern := chi.RouteContext(r.Context()).RoutePattern()
		if routePattern == "" {
			routePattern = r.URL.Path
		}

		// Get status code, default to 200 if not set
		status := 0
		errorCode := ""
		if appWriter, ok := w.(*appResponseWriter); ok {
			status = appWriter.Status()
			errorCode = appWriter.ErrorCode()
		}
		if status == 0 {
			status = http.StatusOK
		}
		statusStr := strconv.Itoa(status)

		// Record request count
		metricHTTPRequestsTotal.WithLabelValues(
			r.Method,
			routePattern,
			statusStr,
			errorCode,
		).Inc()

		// Record request duration
		metricHTTPRequestDuration.WithLabelValues(
			r.Method,
			routePattern,
			statusStr,
			errorCode,
		).Observe(time.Since(start).Seconds())
	})
}

// mwRequestID extracts or generates a request ID and attaches a request-scoped logger to context.
func mwRequestID(httpLogger loggers.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestID := requestID(r)
			if requestID == "" {
				requestID = ulid.NewULID()
				setRequestID(r, requestID)
			}
			ctxWithReqLogger := httpLogger.With().
				Str(loggers.FieldRequestID, requestID).
				Logger().WithContext(r.Context())

			next.ServeHTTP(w, r.WithContext(ctxWithReqLogger))
		})
	}
}

// mwRequestCompletionLog provides structured logging middleware
func mwRequestCompletionLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		start := time.Now()
		defer func() {
			status := 0
			if appWriter, ok := w.(*appResponseWriter); ok {
				status = appWriter.Status()
			}
			if status == 0 {
				status = http.StatusOK
			}
			loggers.Ctx(r.Context()).Info().
				Str(loggers.FieldHttpMethod, r.Method).
				Str(loggers.FieldHttpPath, r.URL.Path).
				Int(loggers.FieldHttpStatus, status).
				Int64(loggers.FieldDuration, time.Since(start).Milliseconds()).
				Msg("request completed")
		}()

		next.ServeHTTP(w, r)
	})
}

// mwRecoverer provides panic recovery middleware.
func mwRecoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if p := recover(); p != nil {
				loggers.Ctx(r.Context()).Error().
					Bytes(loggers.FieldErrorStack, debug.Stack()).
					Msgf("http panic recovered: %v", p)

				// Convert panic value to error
				var panicErr error
				if err, ok := p.(error); ok {
					panicErr = err
				} else {
					panicErr = fmt.Errorf("%v", p)
				}

				svcErr := svcerrors.NewInternalErrorPanic(panicErr)
				writeErrorResponse(w, r, svcErr)
			}
		}()

		next.ServeHTTP(w, r)
	})
}
