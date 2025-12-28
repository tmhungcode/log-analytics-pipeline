package ingestors

import (
	"testing"
	"time"

	"log-analytics/internal/models"

	"github.com/stretchr/testify/assert"
)

func TestBatchSummarizer_Summarize_MultipleMinutes(t *testing.T) {
	t.Parallel()

	summarizer := NewBatchSummarizer(models.WindowMinute)

	// Create entries spanning 2 minutes
	minute1 := time.Date(2025, 12, 21, 14, 21, 0, 0, time.UTC)
	minute2 := time.Date(2025, 12, 21, 14, 22, 0, 0, time.UTC)

	batch := &models.LogBatch{
		BatchID:    "batch123",
		CustomerID: "customer123",
		Entries: []*models.LogEntry{
			{
				ReceivedAt: minute2.Add(15 * time.Second),
				Method:     "POST",
				Path:       "/logs",
				UserAgent:  "Chrome/90.0.0.0 Safari/537.36",
			},
			{
				ReceivedAt: minute1,
				Method:     "GET",
				Path:       "/",
				UserAgent:  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
			},
			{
				ReceivedAt: minute1.Add(30 * time.Second),
				Method:     "POST",
				Path:       "/logs",
				UserAgent:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0",
			},
			{
				ReceivedAt: minute1.Add(15 * time.Second),
				Method:     "POST",
				Path:       "/logs",
				UserAgent:  "Chrome/90.0.0.0 Safari/537.36",
			},
			{
				ReceivedAt: minute2,
				Method:     "GET",
				Path:       "/",
				UserAgent:  "curl/7.68.0",
			},
		},
	}

	summary := summarizer.Summarize(batch)

	minute1Key := minute1.Format(time.RFC3339)
	minute2Key := minute2.Format(time.RFC3339)

	expectedSummary := &models.BatchSummary{
		BatchID:    "batch123",
		CustomerID: "customer123",
		WindowSize: models.WindowMinute,
		ByWindowStart: map[string]models.WindowAggregates{
			minute1Key: {
				RequestsByPath: map[string]int64{
					"GET /":      1,
					"POST /logs": 2,
				},
				RequestsByUserAgent: map[string]int64{
					"Chrome":  1,
					"Firefox": 2,
				},
			},
			minute2Key: {
				RequestsByPath: map[string]int64{
					"GET /":      1,
					"POST /logs": 1,
				},
				RequestsByUserAgent: map[string]int64{
					"Chrome": 1,
					"curl":   1,
				},
			},
		},
	}

	assert.Equal(t, expectedSummary, summary)
}

func TestBatchSummarizer_Summarize_UserAgentParseFails(t *testing.T) {
	t.Parallel()

	summarizer := NewBatchSummarizer(models.WindowMinute)

	unknownUA := "SomeUnknownUserAgent/1.0"
	minute := time.Date(2025, 12, 21, 14, 21, 0, 0, time.UTC)
	batch := &models.LogBatch{
		BatchID:    "batch123",
		CustomerID: "customer123",
		Entries: []*models.LogEntry{
			{
				ReceivedAt: minute,
				Method:     "GET",
				Path:       "/",
				UserAgent:  unknownUA,
			},
		},
	}

	summary := summarizer.Summarize(batch)

	minuteKey := minute.Format(time.RFC3339)

	expectedSummary := &models.BatchSummary{
		BatchID:    "batch123",
		CustomerID: "customer123",
		WindowSize: models.WindowMinute,
		ByWindowStart: map[string]models.WindowAggregates{
			minuteKey: {
				RequestsByPath: map[string]int64{
					"GET /": 1,
				},
				RequestsByUserAgent: map[string]int64{
					"SomeUnknownUserAgent": 1,
				},
			},
		},
	}

	assert.Equal(t, expectedSummary, summary)
}

func TestBatchSummarizer_Summarize_MethodNormalization(t *testing.T) {
	t.Parallel()

	summarizer := NewBatchSummarizer(models.WindowMinute)

	minute := time.Date(2025, 12, 21, 14, 21, 0, 0, time.UTC)
	batch := &models.LogBatch{
		BatchID:    "batch123",
		CustomerID: "customer123",
		Entries: []*models.LogEntry{
			{
				ReceivedAt: minute,
				Method:     "get", // lowercase
				Path:       "/",
				UserAgent:  "test",
			},
			{
				ReceivedAt: minute.Add(30 * time.Second),
				Method:     "POST",
				Path:       "/logs",
				UserAgent:  "test",
			},
		},
	}

	summary := summarizer.Summarize(batch)

	minuteKey := minute.Format(time.RFC3339)

	expectedSummary := &models.BatchSummary{
		BatchID:    "batch123",
		CustomerID: "customer123",
		WindowSize: models.WindowMinute,
		ByWindowStart: map[string]models.WindowAggregates{
			minuteKey: {
				RequestsByPath: map[string]int64{
					"GET /":      1,
					"POST /logs": 1,
				},
				RequestsByUserAgent: map[string]int64{
					"test": 2,
				},
			},
		},
	}

	assert.Equal(t, expectedSummary, summary)
}

func TestBatchSummarizer_Summarize_UTCTimezone(t *testing.T) {
	t.Parallel()

	summarizer := NewBatchSummarizer(models.WindowMinute)

	// Create entries with the same UTC time but different timezones
	utcTime := time.Date(2025, 12, 21, 14, 21, 30, 0, time.UTC)
	pstLocation, _ := time.LoadLocation("America/Los_Angeles")
	pstTime := time.Date(2025, 12, 21, 6, 21, 30, 0, pstLocation) // 6:21 PST = 14:21 UTC

	batch := &models.LogBatch{
		BatchID:    "batch123",
		CustomerID: "customer123",
		Entries: []*models.LogEntry{
			{
				ReceivedAt: utcTime,
				Method:     "GET",
				Path:       "/",
				UserAgent:  "test1",
			},
			{
				ReceivedAt: pstTime, // Different timezone, but same UTC time
				Method:     "POST",
				Path:       "/logs",
				UserAgent:  "test2",
			},
		},
	}

	summary := summarizer.Summarize(batch)

	// Both entries should be in the same minute window (UTC)
	expectedMinuteKey := utcTime.UTC().Truncate(time.Minute).Format(time.RFC3339)

	// Verify WindowSize is set
	assert.Equal(t, models.WindowMinute, summary.WindowSize)

	// Verify there's only one window
	assert.Len(t, summary.ByWindowStart, 1, "entries with same UTC time should be in same window")

	// Verify the window key is based on UTC
	_, exists := summary.ByWindowStart[expectedMinuteKey]
	assert.True(t, exists, "window key should be based on UTC time")

	// Verify both entries are in the same window
	window := summary.ByWindowStart[expectedMinuteKey]
	assert.Equal(t, 2, len(window.RequestsByPath), "both entries should be in same window")
	assert.Equal(t, int64(1), window.RequestsByPath["GET /"], "GET / should have count 1")
	assert.Equal(t, int64(1), window.RequestsByPath["POST /logs"], "POST /logs should have count 1")
}
