package aggregators

import (
	"fmt"
	"log-analytics/internal/events"
	"log-analytics/internal/models"
)

//go:generate mockgen -source=aggregate_rolluper.go -destination=./mocks/aggregate_rolluper_mock.go -package=mocks
type WindowAggregateRolluper interface {
	// Rollup mutates agg by accumulating values from partial.
	Rollup(agg *models.WindowAggregateResult, partial *events.PartialInsightEvent) error
}

type aggregateRolluper struct{}

func NewAggregateRolluper() WindowAggregateRolluper {
	return &aggregateRolluper{}
}

func (a *aggregateRolluper) Rollup(agg *models.WindowAggregateResult, partial *events.PartialInsightEvent) error {
	// Validate that identity fields match
	if agg.CustomerID != partial.CustomerID {
		return fmt.Errorf("customerID mismatch: agg=%q, partial=%q", agg.CustomerID, partial.CustomerID)
	}
	if !agg.WindowStart.Equal(partial.WindowStart) {
		return fmt.Errorf("windowStart mismatch: agg=%v, partial=%v", agg.WindowStart, partial.WindowStart)
	}
	if agg.WindowSize != partial.WindowSize {
		return fmt.Errorf("windowSize mismatch: agg=%q, partial=%q", agg.WindowSize, partial.WindowSize)
	}

	// Merge RequestsByPath
	if partial.RequestsByPath != nil {
		for k, v := range partial.RequestsByPath {
			agg.RequestsByPath[k] += v
		}
	}

	// Merge RequestsByUserAgent
	if partial.RequestsByUserAgent != nil {
		for k, v := range partial.RequestsByUserAgent {
			agg.RequestsByUserAgent[k] += v
		}
	}

	return nil
}
