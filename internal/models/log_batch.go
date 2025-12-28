package models

import "time"

type LogEntry struct {
	ReceivedAt time.Time
	Method     string
	Path       string
	UserAgent  string
}

type LogBatch struct {
	BatchID    string
	CustomerID string
	Entries    []*LogEntry
}
