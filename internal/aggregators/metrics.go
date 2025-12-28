package aggregators

import (
	"log-analytics/internal/shared/metrics"
)

// metricWindowAggregateCreatedTotal counts the number of new window aggregates created.
//
// This metric is incremented when a partial insight event is rolled up into a window aggregate
// for the first time (i.e., when the aggregate result is newly created, not updated).
//
// The bucket_id label identifies the time bucket within the window:
//   - For minute windows: "minute-XX" where XX is the minute (00-59)
//     Example: For a window starting at 2025-12-28 18:03:00 UTC, bucket_id = "minute-03"
//   - For hour windows: "hour-XX" where XX is the hour (00-23)
//     Example: For a window starting at 2025-12-28 18:00:00 UTC, bucket_id = "hour-18"
//
// Example scenario:
//   - At 18:03:15 UTC, a partial insight event arrives for minute window 18:03:00
//   - This is the first partial insight for this window, so a new aggregate is created
//   - The metric is incremented with bucket_id="minute-03"
//   - Subsequent partial insights for the same 18:03:00 window will update the existing aggregate
//     and will NOT increment this metric
var (
	metricWindowAggregateCreatedTotal = metrics.NewCounterVec(
		metrics.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.SubAggregation,
			Name:      "window_aggregate_created_total",
		},
		[]string{"bucket_id"},
	)
)
