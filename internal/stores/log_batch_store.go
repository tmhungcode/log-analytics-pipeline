package stores

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"log-analytics/internal/models"
	"log-analytics/internal/shared/filestorages"
)

var (
	ErrLogBatchAlreadyExist = errors.New("log batch already exists")
)

// LogBatchStore simulates S3's atomic PUT operations for deduplication. When Put is called with
// AllowOverwrite: false, it performs an atomic "create-if-not-exists" operation, similar to
// S3's conditional PUT behavior.
//
// Example scenario (similar to S3 conditional PUT):
//   - Request A and Request B both try to store batch "batch-123" simultaneously
//   - Request A's Put succeeds → batch stored
//   - Request B's Put fails → ErrLogBatchAlreadyExist returned (duplicate detected)
//   - This enables idempotent batch ingestion: duplicate batches are detected and rejected
//
//go:generate mockgen -source=log_batch_store.go -destination=./mocks/log_batch_store_mock.go -package=mocks
type LogBatchStore interface {
	Put(ctx context.Context, logBatch *models.LogBatch) error
}

type logBatchStore struct {
	fileStorage filestorages.FileStorage
	dir         string
}

func NewLogBatchStore(fileStorage filestorages.FileStorage) LogBatchStore {
	return &logBatchStore{fileStorage: fileStorage, dir: "raw-batches"}
}

func (s *logBatchStore) Put(ctx context.Context, logBatch *models.LogBatch) error {
	jsonData, err := json.Marshal(logBatch)
	if err != nil {
		return fmt.Errorf("failed to marshal log batch: %w", err)
	}
	reader := bytes.NewReader(jsonData)

	key := fmt.Sprintf("%s/%s/%s.json", s.dir, logBatch.CustomerID, logBatch.BatchID)

	_, err = s.fileStorage.Put(ctx, key, reader, filestorages.PutOptions{AllowOverwrite: false})
	if err != nil {
		if errors.Is(err, filestorages.ErrFileAlreadyExists) {
			return ErrLogBatchAlreadyExist
		}
		return fmt.Errorf("failed to put log batch: %w", err)
	}
	return nil
}
