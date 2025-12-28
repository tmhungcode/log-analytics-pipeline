package stores

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"log-analytics/internal/models"
	"log-analytics/internal/shared/filestorages"
)

//go:generate mockgen -source=aggregate_result_store.go -destination=./mocks/aggregate_result_store_mock.go -package=mocks
type AggregateResultStore interface {
	Upsert(ctx context.Context, aggregateResult *models.WindowAggregateResult) error
	Get(ctx context.Context, customerID string, windowStart time.Time, windowSize models.WindowSize) (*models.WindowAggregateResult, error)
}

type aggregateResultStore struct {
	fileStorage filestorages.FileStorage
	dir         string
}

func NewAggregateResultStore(fileStorage filestorages.FileStorage) AggregateResultStore {
	return &aggregateResultStore{fileStorage: fileStorage, dir: "aggregate-results"}
}

func (s *aggregateResultStore) Upsert(ctx context.Context, aggregateResult *models.WindowAggregateResult) error {
	jsonData, err := json.Marshal(aggregateResult)
	if err != nil {
		return fmt.Errorf("failed to marshal aggregate result: %w", err)
	}
	reader := bytes.NewReader(jsonData)
	key := s.getKey(aggregateResult.CustomerID, aggregateResult.WindowStart, aggregateResult.WindowSize)
	_, err = s.fileStorage.Put(ctx, key, reader, filestorages.PutOptions{AllowOverwrite: true})
	if err != nil {
		return fmt.Errorf("failed to put aggregate result: %w", err)
	}
	return nil
}

func (s *aggregateResultStore) Get(ctx context.Context, customerID string, windowStart time.Time, windowSize models.WindowSize) (*models.WindowAggregateResult, error) {
	key := s.getKey(customerID, windowStart, windowSize)
	readCloser, err := s.fileStorage.Get(ctx, key)
	if err != nil {
		if errors.Is(err, filestorages.ErrFileNotFound) {
			return models.NewEmptyWindowAggregateResult(customerID, windowStart, windowSize), nil
		}
		return nil, fmt.Errorf("failed to get aggregate result: %w", err)
	}

	defer readCloser.Close()
	data, err := io.ReadAll(readCloser)
	if err != nil {
		return nil, fmt.Errorf("failed to read aggregate result: %w", err)
	}
	var aggregateResult models.WindowAggregateResult
	if err := json.Unmarshal(data, &aggregateResult); err != nil {
		return nil, fmt.Errorf("failed to unmarshal aggregate result: %w", err)
	}
	return &aggregateResult, nil
}

func (s *aggregateResultStore) getKey(customerID string, windowStart time.Time, windowSize models.WindowSize) string {
	utcTime := windowSize.FormatWindowStart(windowStart)
	return fmt.Sprintf("%s/%s/%s.json", s.dir, customerID, utcTime)
}
