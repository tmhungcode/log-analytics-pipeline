package events

import (
	"time"

	"log-analytics/internal/models"
)

// PartialInsightEvent represents a partial aggregation result for a specific time window
// from a single log batch. These events are produced during batch summarization and
// consumed by the aggregation service to roll up into final window aggregate results.
//
// Each PartialInsightEvent contains request counts for a single time window (e.g., one minute)
// from one batch. Multiple PartialInsightEvents for the same window are merged together
// to create the final aggregate result.
//
// Example JSON:
//
//	{
//	  "customerId": "cus-axon",
//	  "batchId": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
//	  "windowStart": "2025-12-28T18:03:00Z",
//	  "windowSize": "minute",
//	  "requestsByPath": {
//	    "GET /": 150,
//	    "GET /about": 50
//	  },
//	  "requestsByUserAgent": {
//	    "Chrome": 120,
//	    "Firefox": 80
//	  }
//	}
//
// In this example:
//   - The event represents partial insights from batch "01ARZ3NDEKTSV4RRFFQ69G5FAV"
//   - It covers the minute window starting at 2025-12-28 18:03:00 UTC
//   - The batch contained 200 requests (150 + 50) distributed across 2 paths
//   - The batch contained 200 requests (120 + 80) distributed across 2 user agents
//   - This partial insight will be merged with other partial insights for the same window
//     to create the final aggregate result for the 18:03 minute window
type PartialInsightEvent struct {
	CustomerID          string            `json:"customerId"`
	BatchID             string            `json:"batchId"`
	WindowStart         time.Time         `json:"windowStart"`
	WindowSize          models.WindowSize `json:"windowSize"`
	RequestsByPath      map[string]int64  `json:"requestsByPath"`
	RequestsByUserAgent map[string]int64  `json:"requestsByUserAgent"`
}
