package aggregators

import (
	"fmt"

	"log-analytics/internal/shared/svcerrors"
)

const (
	codeInternalAggregateRollupFailed      = "AGG_9000"
	codeInternalAggregateResultStoreFailed = "AGG_9001"
)

// errInternalAggregateRollupFailed returns an error when a aggregate rollup fails.
func errInternalAggregateRollupFailed(cause error) *svcerrors.ServiceError {
	return svcerrors.NewInternalError(codeInternalAggregateRollupFailed, fmt.Errorf("aggregateRollupFailed: %w", cause))
}

// errInternalAggregateResultStoreFailed returns an error when a aggregate result store operation fails.
func errInternalAggregateResultStoreFailed(cause error) *svcerrors.ServiceError {
	return svcerrors.NewInternalError(codeInternalAggregateResultStoreFailed, fmt.Errorf("aggregateResultStoreFailed: %w", cause))
}
