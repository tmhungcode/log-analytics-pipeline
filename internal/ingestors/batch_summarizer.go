package ingestors

import (
	"sort"
	"strings"
	"time"

	"log-analytics/internal/models"

	"github.com/mileusna/useragent"
)

//go:generate mockgen -source=batch_summarizer.go -destination=./mocks/batch_summarizer_mock.go -package=mocks
type BatchSummarizer interface {
	Summarize(batch *models.LogBatch) *models.BatchSummary
}

type batchSummarizer struct {
	windowSize models.WindowSize
}

func NewMinuteBatchSummarizer() BatchSummarizer {
	return &batchSummarizer{
		windowSize: models.WindowMinute,
	}
}

func (s *batchSummarizer) Summarize(batch *models.LogBatch) *models.BatchSummary {
	// Map: windowStart (string) -> maps for counting
	type windowCounts struct {
		pathCounts map[string]int64
		uaCounts   map[string]int64
	}
	byWindowStart := make(map[string]*windowCounts)

	for _, entry := range batch.Entries {
		// Group by window duration
		windowStart := entry.ReceivedAt.UTC().Truncate(s.windowSize.Duration())
		windowKey := windowStart.Format(time.RFC3339)

		// Get or create window counts for this window
		window, exists := byWindowStart[windowKey]
		if !exists {
			window = &windowCounts{
				pathCounts: make(map[string]int64),
				uaCounts:   make(map[string]int64),
			}
			byWindowStart[windowKey] = window
		}

		// Normalize path: METHOD + " " + path
		normalizedPath := strings.ToUpper(entry.Method) + " " + entry.Path
		window.pathCounts[normalizedPath]++

		// Normalize user agent: parse family or use original
		normalizedUA := s.normalizeUserAgent(entry.UserAgent)
		window.uaCounts[normalizedUA]++
	}

	// Convert to result structure
	result := &models.BatchSummary{
		BatchID:       batch.BatchID,
		CustomerID:    batch.CustomerID,
		WindowSize:    s.windowSize,
		ByWindowStart: make(map[string]models.WindowAggregates),
	}

	// Sort window keys for deterministic ordering
	windowKeys := make([]string, 0, len(byWindowStart))
	for k := range byWindowStart {
		windowKeys = append(windowKeys, k)
	}
	sort.Strings(windowKeys)

	for _, windowKey := range windowKeys {
		window := byWindowStart[windowKey]

		result.ByWindowStart[windowKey] = models.WindowAggregates{
			RequestsByPath:      window.pathCounts,
			RequestsByUserAgent: window.uaCounts,
		}
	}

	return result
}

// normalizeUserAgent parses user agent to extract family, or returns original if parsing fails.
func (s *batchSummarizer) normalizeUserAgent(ua string) string {
	parsed := useragent.Parse(ua)
	if parsed.Name != "" {
		return parsed.Name
	}

	// If parsing fails or family is empty, return original
	return ua
}
