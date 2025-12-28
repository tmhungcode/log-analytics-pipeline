package http

import (
	"log-analytics/internal/ingestors"
	"net/http"
)

type AppHttpHandler interface {
	Handle(w http.ResponseWriter, r *http.Request) error
}

type ingestLogHandler struct {
	ingestionService ingestors.IngestionService
}

func NewIngestLogHandler(ingestionService ingestors.IngestionService) AppHttpHandler {
	return &ingestLogHandler{
		ingestionService: ingestionService,
	}
}

// Handle HandleLogs processes POST /logs requests.
func (h *ingestLogHandler) Handle(w http.ResponseWriter, r *http.Request) error {
	_, err := h.ingestionService.IngestBatch(r.Context(), customerID(r), idempotencyKey(r), contentType(r), r.Body)
	if err != nil {
		return err
	}

	w.WriteHeader(http.StatusAccepted)
	return nil
}
