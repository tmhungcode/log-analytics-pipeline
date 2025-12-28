package models

import "time"

type WindowAggregateResult struct {
	CustomerID          string           `json:"customerId"`
	WindowStart         time.Time        `json:"windowStart"`
	WindowSize          WindowSize       `json:"windowSize"`
	RequestsByPath      map[string]int64 `json:"requestsByPath"`
	RequestsByUserAgent map[string]int64 `json:"requestsByUserAgent"`
}

func NewEmptyWindowAggregateResult(customerID string, windowStart time.Time, windowSize WindowSize) *WindowAggregateResult {
	return &WindowAggregateResult{
		CustomerID:          customerID,
		WindowStart:         windowStart,
		WindowSize:          windowSize,
		RequestsByPath:      make(map[string]int64),
		RequestsByUserAgent: make(map[string]int64),
	}
}

func (w *WindowAggregateResult) IsNewAggregate() bool {
	return len(w.RequestsByPath) == 0 && len(w.RequestsByUserAgent) == 0
}