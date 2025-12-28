package models

// BatchSummary represents an aggregated summary of a log batch, reducing thousands of raw log entries
// into compact time-windowed aggregates. This dramatically reduces the processing effort required
// downstream by eliminating the need to process individual log entries.
//
// Example: A batch with hundreds of raw log entries spanning multiple minute windows becomes a BatchSummary
// with just a few window aggregates (one per time window), each containing aggregated counts rather than
// individual entries. For instance, 500 entries spanning 2 windows reduces to 2 aggregated window summaries.
//
// Example JSON:
//
//	{
//	  "batchId": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
//	  "customerId": "cus-axon",
//	  "windowSize": "minute",
//	  "byWindowStart": {
//	    "2025-12-28T18:03:00Z": {
//	      "requestsByPath": {
//	        "GET /": 150,
//	        "GET /about": 50,
//	        "GET /careers": 30
//	      },
//	      "requestsByUserAgent": {
//	        "Chrome": 120,
//	        "Firefox": 80,
//	        "Googlebot": 30
//	      }
//	    },
//	    "2025-12-28T18:04:00Z": {
//	      "requestsByPath": {
//	        "GET /": 200,
//	        "GET /contact": 50
//	      },
//	      "requestsByUserAgent": {
//	        "Chrome": 180,
//	        "Firefox": 70
//	      }
//	    }
//	  }
//	}
type BatchSummary struct {
	BatchID       string                      `json:"batchId"`
	CustomerID    string                      `json:"customerId"`
	WindowSize    WindowSize                  `json:"windowSize"`
	ByWindowStart map[string]WindowAggregates `json:"byWindowStart"`
}

type WindowAggregates struct {
	RequestsByPath      map[string]int64 `json:"requestsByPath"`
	RequestsByUserAgent map[string]int64 `json:"requestsByUserAgent"`
}
