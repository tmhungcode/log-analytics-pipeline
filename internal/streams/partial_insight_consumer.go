package streams

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"

	"log-analytics/internal/aggregators"
	"log-analytics/internal/events"
	"log-analytics/internal/shared/loggers"
	"log-analytics/internal/shared/metrics"
	"log-analytics/internal/shared/svcerrors"
	"log-analytics/internal/shared/ulid"
)

//go:generate mockgen -source=partial_insight_consumer.go -destination=./mocks/partial_insight_consumer_mock.go -package=mocks
type PartialInsightConsumer interface {
	Start(ctx context.Context)
	Stop()
}

type partialInsightConsumer struct {
	queue              *PartitionedQueue[events.PartialInsightEvent]
	aggregationService aggregators.AggregationService

	wg sync.WaitGroup

	stopOnce sync.Once
	stopCh   chan struct{}

	logger loggers.Logger
}

func NewPartialInsightConsumer(queue *PartitionedQueue[events.PartialInsightEvent], aggregationService aggregators.AggregationService, logger loggers.Logger) PartialInsightConsumer {
	return &partialInsightConsumer{
		queue:              queue,
		aggregationService: aggregationService,
		stopCh:             make(chan struct{}),
		logger:             logger,
	}
}

// Start spawns 1 worker goroutine per partition.
// Each partition is a single-writer lane for aggregate keys routed by the producer.
func (consumer *partialInsightConsumer) Start(ctx context.Context) {
	for partitionIndex := 0; partitionIndex < consumer.queue.PartitionCount(); partitionIndex++ {
		ch := consumer.queue.partitions[partitionIndex]
		consumer.wg.Add(1)
		go func() {
			defer consumer.wg.Done()
			
			consumer.runPartitionWorker(ctx, partitionIndex, ch)
		}()
	}
}

// Stop waits for workers to stop (best called during app shutdown).
func (consumer *partialInsightConsumer) Stop() {
	consumer.stopOnce.Do(func() { close(consumer.stopCh) })
	consumer.wg.Wait()
}

func (consumer *partialInsightConsumer) runPartitionWorker(ctx context.Context, partitionIndex int, ch <-chan events.PartialInsightEvent) {

	for {
		select {
		case <-ctx.Done():
			return
		case <-consumer.stopCh:
			return
		case event, _ := <-ch:
			// Handle panic recovery to prevent worker goroutine from crashing
			func() {
				defer func() {
					if r := recover(); r != nil {
						// Log panic details
						loggers.Ctx(ctx).Error().
							Bytes(loggers.FieldErrorStack, debug.Stack()).
							Msg("coonsumer panic recovered")

						// Convert panic value to error
						var panicErr error
						if err, ok := r.(error); ok {
							panicErr = err
						} else {
							panicErr = fmt.Errorf("%v", r)
						}

						// Increment metric with panic error code
						svcErr := svcerrors.NewInternalErrorPanic(panicErr)
						metricPartialInsightConsumedTotal.WithLabelValues(streamPartialInsight, svcErr.Code).Inc()
					}
				}()

				ctx = consumer.logger.With().
					Str(loggers.FieldPartitionId, fmt.Sprintf("%d", partitionIndex)).
					Str(loggers.FieldRequestID, ulid.NewULID()).
					Logger().WithContext(ctx)
				svcError := consumer.aggregationService.Aggregate(ctx, &event)
				if svcError != nil {
					metricPartialInsightConsumedTotal.WithLabelValues(streamPartialInsight, svcError.Code).Inc()
				} else {
					metricPartialInsightConsumedTotal.WithLabelValues(streamPartialInsight, metrics.ValueNoError).Inc()
				}
			}()
		}
	}
}
