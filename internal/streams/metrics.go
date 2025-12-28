package streams

import (
	"log-analytics/internal/shared/metrics"
)

var (
	streamPartialInsight              = "partial_insight"
	metricPartialInsightProducedTotal = metrics.NewCounterVec(
		metrics.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.SubStream,
			Name:      "partial_insight_published_total",
		},
		[]string{"stream_id"},
	)

	metricPartialInsightConsumedTotal = metrics.NewCounterVec(
		metrics.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.SubStream,
			Name:      "partial_insight_consumed_total",
		},
		[]string{"stream_id", metrics.FieldErrorCode},
	)
)
