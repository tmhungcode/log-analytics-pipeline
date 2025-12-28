package ingestors

import (
	"log-analytics/internal/shared/metrics"
)

var (
	metricBatchIngestedTotal = metrics.NewCounterVec(
		metrics.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.SubIngestion,
			Name:      "batch_ingested_total",
		},
		[]string{metrics.FieldErrorCode},
	)
)
