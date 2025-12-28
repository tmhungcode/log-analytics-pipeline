package http

import (
	"log-analytics/internal/shared/metrics"
)

var (
	// TotalRequests counts total HTTP requests.
	metricHTTPRequestsTotal = metrics.NewCounterVec(
		metrics.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.SubHTTP,
			Name:      "http_requests_total",
		},
		[]string{"method", "path", "status", metrics.FieldErrorCode},
	)

	metricHTTPRequestDuration = metrics.NewHistogramVec(
		metrics.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.SubHTTP,
			Name:      "request_latency",
			Buckets:   metrics.DefBuckets,
		},
		[]string{"method", "path", "status", metrics.FieldErrorCode},
	)
)
