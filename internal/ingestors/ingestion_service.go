package ingestors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"log-analytics/internal/models"
	"log-analytics/internal/shared/loggers"
	"log-analytics/internal/shared/metrics"
	"log-analytics/internal/shared/ulid"
	"log-analytics/internal/stores"
	"log-analytics/internal/streams"
)

const (
	maxBatchBytes   = 2 * 1024 * 1024
	maxPathLen      = 2048
	maxUserAgentLen = 1024
)

const (
	FormatJSON = "json"
)

// IngestResult represents the result of a batch ingestion operation.
type IngestResult struct {
	BatchID     string
	StoredCount int
}

//go:generate mockgen -source=service.go -destination=./mocks/ingestion_service_mock.go -package=mocks
type IngestionService interface {
	// IngestBatch processes a batch of log entries from JSON format.
	IngestBatch(ctx context.Context, customerID string, idempotencyKey string, format string, r io.Reader) (*IngestResult, error)
}

type ingestionService struct {
	batchSummarizer        BatchSummarizer
	batchStore             stores.LogBatchStore
	partialInsightProducer streams.PartialInsightProducer
}

func NewIngestionService(batchSummarizer BatchSummarizer, batchStore stores.LogBatchStore, partialInsightProducer streams.PartialInsightProducer) IngestionService {
	return &ingestionService{
		batchSummarizer:        batchSummarizer,
		batchStore:             batchStore,
		partialInsightProducer: partialInsightProducer,
	}
}

func (s *ingestionService) IngestBatch(ctx context.Context, customerID string, idempotencyKey string, format string, r io.Reader) (*IngestResult, error) {
	logger := loggers.Ctx(ctx)
	logger.Debug().Msgf("started ingesting batch with customer ID: %s, idempotency key: %s, format: %s", customerID, idempotencyKey, format)

	logEntries, err := s.validateLogBatch(customerID, format, r)
	if err != nil {
		return nil, err
	}

	batchID := strings.TrimSpace(idempotencyKey)
	if batchID == "" {
		batchID = ulid.NewULID()
	}

	logBatch := &models.LogBatch{
		BatchID:    batchID,
		CustomerID: customerID,
		Entries:    logEntries,
	}

	// Store the log batch
	err = s.batchStore.Put(ctx, logBatch)
	if err != nil {
		if errors.Is(err, stores.ErrLogBatchAlreadyExist) {
			svcError := errLogBatchAlreadyProcessed(err)
			metricBatchIngestedTotal.WithLabelValues(svcError.Code).Inc()
			return nil, svcError
		}
		return nil, errInternalLogBatchStoreFailed(err)
	}

	// create summary and publish
	batchSummary := s.batchSummarizer.Summarize(logBatch)

	// publish the partial insight
	err = s.partialInsightProducer.Produce(ctx, batchSummary)
	if err != nil {
		return nil, errInternalPartialInsightPublisherFailed(err)
	}

	metricBatchIngestedTotal.WithLabelValues(metrics.ValueNoError).Inc()
	return &IngestResult{}, nil
}

func (s *ingestionService) validateLogBatch(customerID string, format string, r io.Reader) ([]*models.LogEntry, error) {
	if customerID == "" {
		return nil, errValidationFailed("customerID is required", nil)
	}

	// Handle nil reader
	if r == nil {
		return nil, errValidationFailed("empty request body", nil)
	}

	// Read with size limit
	buf, err := s.readWithLimit(r, maxBatchBytes)
	if err != nil {
		return nil, errValidationFailed("batch too large: must be <= 2MB", nil)
	}

	// Normalize format to lowercase for comparison
	formatLower := strings.ToLower(string(format))

	// Parse based on format (using contains for flexible matching)
	var entries []*models.LogEntry
	if strings.Contains(formatLower, FormatJSON) {
		entries, err = s.parseJSON(buf)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errValidationFailed(fmt.Sprintf("unsupported input format: %q", format), nil)
	}

	// Validate that entries are not empty
	if len(entries) == 0 {
		return nil, errValidationFailed("log entries cannot be empty", nil)
	}

	return entries, nil
}

// readWithLimit reads up to max+1 bytes from r and checks if it exceeds max.
func (s *ingestionService) readWithLimit(r io.Reader, max int) ([]byte, error) {
	limitedReader := io.LimitReader(r, int64(max+1))
	buf := make([]byte, max+1)
	n, err := limitedReader.Read(buf)
	if err != nil && err != io.EOF {
		return nil, err
	}

	// If we read more than max bytes, the batch is too large
	if n > max {
		return nil, errValidationFailed("batch too large", nil)
	}

	return buf[:n], nil
}

// parseJSON parses buf as a JSON array of objects into LogEntry slice.
func (s *ingestionService) parseJSON(buf []byte) ([]*models.LogEntry, error) {
	var arr []map[string]any
	if err := json.Unmarshal(buf, &arr); err != nil {
		return nil, errValidationFailed("invalid json", err)
	}

	entries := make([]*models.LogEntry, 0, len(arr))
	for i, item := range arr {
		entry, err := s.jsonObjectToLogEntry(item, i)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// jsonObjectToLogEntry converts a JSON object map to LogEntry.
func (s *ingestionService) jsonObjectToLogEntry(obj map[string]any, index int) (*models.LogEntry, error) {
	entry := &models.LogEntry{}

	// Parse receivedAt
	if receivedAtVal, ok := obj["receivedAt"]; ok {
		receivedAtStr, ok := receivedAtVal.(string)
		if !ok {
			return entry, errValidationFailed(fmt.Sprintf("item at index %d: receivedAt must be a string", index), nil)
		}
		receivedAt, err := s.parseTime(receivedAtStr, index)
		if err != nil {
			return entry, err
		}
		entry.ReceivedAt = receivedAt
	} else {
		return entry, errValidationFailed(fmt.Sprintf("item at index %d: missing receivedAt", index), nil)
	}

	// Parse method
	if methodVal, ok := obj["method"]; ok {
		if method, ok := methodVal.(string); ok {
			entry.Method = method
		} else {
			return entry, errValidationFailed(fmt.Sprintf("item at index %d: method must be a string", index), nil)
		}
	} else {
		return entry, errValidationFailed(fmt.Sprintf("item at index %d: missing method", index), nil)
	}

	// Parse path
	if pathVal, ok := obj["path"]; ok {
		if path, ok := pathVal.(string); ok {
			entry.Path = path
		} else {
			return entry, errValidationFailed(fmt.Sprintf("item at index %d: path must be a string", index), nil)
		}
	} else {
		return entry, errValidationFailed(fmt.Sprintf("item at index %d: missing path", index), nil)
	}

	// Parse userAgent
	if userAgentVal, ok := obj["userAgent"]; ok {
		if userAgent, ok := userAgentVal.(string); ok {
			entry.UserAgent = userAgent
		} else {
			return entry, errValidationFailed(fmt.Sprintf("item at index %d: userAgent must be a string", index), nil)
		}
	} else {
		return entry, errValidationFailed(fmt.Sprintf("item at index %d: missing userAgent", index), nil)
	}

	s.normalizeLogEntry(entry)
	if err := s.validateLogEntry(entry, index); err != nil {
		return entry, err
	}
	return entry, nil
}

func (s *ingestionService) normalizeLogEntry(entry *models.LogEntry) {
	entry.Path = strings.TrimSpace(entry.Path)
	entry.Method = strings.ToUpper(strings.TrimSpace(entry.Method))
	entry.UserAgent = strings.TrimSpace(entry.UserAgent)
}

func (s *ingestionService) validateLogEntry(e *models.LogEntry, index int) error {
	if len(e.Path) > maxPathLen {
		return errValidationFailed(fmt.Sprintf("item at index %d: path too long: max %d characters", index, maxPathLen), nil)
	}
	if len(e.UserAgent) > maxUserAgentLen {
		return errValidationFailed(fmt.Sprintf("item at index %d: userAgent too long: max %d characters", index, maxUserAgentLen), nil)
	}
	return nil
}

// parseTime parses a time string in RFC3339 or ISO-8601 format.
func (s *ingestionService) parseTime(timeStr string, index int) (time.Time, error) {

	// Try ISO-8601 with milliseconds
	t, err := time.Parse("2006-01-02T15:04:05.000Z", timeStr)
	if err == nil {
		return t, nil
	}

	// Try ISO-8601 without milliseconds
	t, err = time.Parse("2006-01-02T15:04:05Z07:00", timeStr)
	if err == nil {
		return t, nil
	}

	return time.Time{}, errValidationFailed(fmt.Sprintf("item at index %d: invalid time format: %s", index, timeStr), nil)
}
