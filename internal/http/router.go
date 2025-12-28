package http

import (
	"net/http"

	"log-analytics/internal/ingestors"
	"log-analytics/internal/shared/loggers"
	"log-analytics/internal/shared/metrics"

	"github.com/go-chi/chi/v5"
)

// NewRouter creates and configures the HTTP router.
func NewRouter(ingestionService ingestors.IngestionService, httpLogger loggers.Logger) http.Handler {
	router := chi.NewRouter()
	setupMiddleware(router, httpLogger)

	// Initialize handlers
	ingestLogHandler := NewIngestLogHandler(ingestionService)

	// Routes
	router.Post("/logs", errorHandlingAdapter(ingestLogHandler))
	router.Get("/metrics", metrics.PromHTTP.Handler().ServeHTTP)

	return router
}
