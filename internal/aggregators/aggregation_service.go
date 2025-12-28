package aggregators

import (
	"context"

	"log-analytics/internal/events"
	"log-analytics/internal/shared/loggers"
	"log-analytics/internal/shared/svcerrors"
	"log-analytics/internal/stores"
)

//go:generate mockgen -source=aggregation_service.go -destination=./mocks/aggregation_service_mock.go -package=mocks
type AggregationService interface {
	Aggregate(ctx context.Context, partialInsightEvent *events.PartialInsightEvent) *svcerrors.ServiceError
}

type aggregationService struct {
	aggregateRolluper    WindowAggregateRolluper
	aggregateResultStore stores.AggregateResultStore
}

func NewAggregationService(aggregateRolluper WindowAggregateRolluper, aggregateResultStore stores.AggregateResultStore) AggregationService {
	return &aggregationService{aggregateRolluper: aggregateRolluper, aggregateResultStore: aggregateResultStore}
}

func (s *aggregationService) Aggregate(ctx context.Context, partialInsightEvent *events.PartialInsightEvent) *svcerrors.ServiceError {
	logger := loggers.Ctx(ctx)
	logger.Debug().Msg("started aggregating partial insight event for customer ID: " + partialInsightEvent.CustomerID + " and window start: " + partialInsightEvent.WindowSize.BucketID(partialInsightEvent.WindowStart))
	aggregateResult, err := s.aggregateResultStore.Get(ctx, partialInsightEvent.CustomerID, partialInsightEvent.WindowStart, partialInsightEvent.WindowSize)
	isNewAggregate := aggregateResult.IsNewAggregate()
	if err != nil {
		return errInternalAggregateResultStoreFailed(err)
	}
	err = s.aggregateRolluper.Rollup(aggregateResult, partialInsightEvent)
	if err != nil {
		return errInternalAggregateRollupFailed(err)
	}
	err = s.aggregateResultStore.Upsert(ctx, aggregateResult)
	if err != nil {
		return errInternalAggregateResultStoreFailed(err)
	}

	if isNewAggregate {
		bucketID := partialInsightEvent.WindowSize.BucketID(partialInsightEvent.WindowStart)
		metricWindowAggregateCreatedTotal.WithLabelValues(bucketID).Inc()
	}

	return nil
}
