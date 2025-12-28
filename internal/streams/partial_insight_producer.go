package streams

import (
	"context"
	"log-analytics/internal/events"
	"log-analytics/internal/models"
	"time"
)

// PartialInsightProducer converts a BatchSummary into PartialInsightEvents and publishes them
// to a partitioned queue. Each window in the batch summary produces one PartialInsightEvent.
//
// Partition Strategy for Race Condition Prevention, and achieving parallelism:
//
// The producer uses a partition key derived from the window aggregate identity:
//
//	partitionKey = "<windowSize>-<windowKey>"
//
// Examples:
//   - Minute window at 18:03:00 UTC → partitionKey = "minute-03"
//   - Hour window at 18:00:00 UTC → partitionKey = "hour-18"
//
// Events with the same partition key are routed to the same partition in the queue.
// Since the consumer processes each partition with a single worker goroutine, all events
// targeting the same window aggregate are processed sequentially, eliminating race conditions
// when rolling up partial insights into the final aggregate result.
//
// This single-writer-per-partition guarantee ensures that:
//   - Multiple partial insights for the same window are processed in order
//   - No concurrent rollup operations occur on the same aggregate (race condition prevention)
//   - Data integrity is maintained without requiring distributed locking
//   - Maximum parallelism is achieved across different window aggregates (throughput optimization)
//
//go:generate mockgen -source=partial_insight_producer.go -destination=./mocks/partial_insight_producer_mock.go -package=mocks
type PartialInsightProducer interface {
	Produce(ctx context.Context, batchSummary *models.BatchSummary) error
}

type partialInsightProducer struct {
	queue *PartitionedQueue[events.PartialInsightEvent]
}

func NewPartialInsightProducer(queue *PartitionedQueue[events.PartialInsightEvent]) PartialInsightProducer {
	return &partialInsightProducer{
		queue: queue,
	}
}

func (producer *partialInsightProducer) Produce(ctx context.Context, batchSummary *models.BatchSummary) error {
	// Iterate over each window in ByWindowStart and produce one PartialInsightEvent per window
	for windowKey, windowAggregates := range batchSummary.ByWindowStart {
		// Parse the window key (RFC3339 format) back to time.Time
		windowStart, err := time.Parse(time.RFC3339, windowKey)
		if err != nil {
			return err
		}

		// Create PartialInsightEvent for this window
		event := events.PartialInsightEvent{
			CustomerID:          batchSummary.CustomerID,
			BatchID:             batchSummary.BatchID,
			WindowStart:         windowStart,
			WindowSize:          batchSummary.WindowSize,
			RequestsByPath:      windowAggregates.RequestsByPath,
			RequestsByUserAgent: windowAggregates.RequestsByUserAgent,
		}
		partitionKey := event.WindowSize.BucketID(event.WindowStart)

		// Publish the event
		if err := producer.publishPartialInsightEvent(ctx, partitionKey, event); err != nil {
			return err
		}
		metricPartialInsightProducedTotal.WithLabelValues(streamPartialInsight).Inc()
	}

	return nil
}

func (producer *partialInsightProducer) publishPartialInsightEvent(ctx context.Context, partitionKey string, event events.PartialInsightEvent) error {

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Partition by aggregate identity (single-writer guarantee).
	producer.queue.Publish(partitionKey, event)
	return nil
}
