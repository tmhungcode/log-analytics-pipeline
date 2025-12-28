package aggregators

import (
	"testing"
	"time"

	"log-analytics/internal/events"
	"log-analytics/internal/models"

	"github.com/stretchr/testify/assert"
)

func TestAggregateRolluper_Rollup_MergesOverlappingKeys(t *testing.T) {
	t.Parallel()

	rolluper := NewAggregateRolluper()

	windowStart := time.Date(2025, 12, 21, 14, 21, 0, 0, time.UTC)

	agg := &models.WindowAggregateResult{
		CustomerID:          "customer123",
		WindowStart:         windowStart,
		WindowSize:          models.WindowMinute,
		RequestsByPath:      map[string]int64{"GET /": 5, "POST /logs": 3},
		RequestsByUserAgent: map[string]int64{"Chrome": 4, "Firefox": 2},
	}

	partial := &events.PartialInsightEvent{
		CustomerID:          "customer123",
		BatchID:             "batch456",
		WindowStart:         windowStart,
		WindowSize:          models.WindowMinute,
		RequestsByPath:      map[string]int64{"GET /": 2, "POST /logs": 1},
		RequestsByUserAgent: map[string]int64{"Chrome": 3, "Firefox": 1},
	}

	err := rolluper.Rollup(agg, partial)
	assert.NoError(t, err)

	// Verify overlapping keys are incremented correctly
	assert.Equal(t, int64(7), agg.RequestsByPath["GET /"], "GET / should be 5+2=7")
	assert.Equal(t, int64(4), agg.RequestsByPath["POST /logs"], "POST /logs should be 3+1=4")
	assert.Equal(t, int64(7), agg.RequestsByUserAgent["Chrome"], "Chrome should be 4+3=7")
	assert.Equal(t, int64(3), agg.RequestsByUserAgent["Firefox"], "Firefox should be 2+1=3")

	// Verify identity fields unchanged
	assert.Equal(t, "customer123", agg.CustomerID)
	assert.Equal(t, windowStart, agg.WindowStart)
	assert.Equal(t, models.WindowMinute, agg.WindowSize)
}

func TestAggregateRolluper_Rollup_AddsNewKeys(t *testing.T) {
	t.Parallel()

	rolluper := NewAggregateRolluper()

	windowStart := time.Date(2025, 12, 21, 14, 21, 0, 0, time.UTC)

	agg := &models.WindowAggregateResult{
		CustomerID:          "customer123",
		WindowStart:         windowStart,
		WindowSize:          models.WindowMinute,
		RequestsByPath:      map[string]int64{"GET /": 5},
		RequestsByUserAgent: map[string]int64{"Chrome": 4},
	}

	partial := &events.PartialInsightEvent{
		CustomerID:          "customer123",
		BatchID:             "batch456",
		WindowStart:         windowStart,
		WindowSize:          models.WindowMinute,
		RequestsByPath:      map[string]int64{"PUT /users": 3, "DELETE /sessions": 1},
		RequestsByUserAgent: map[string]int64{"Safari": 2, "curl": 1},
	}

	err := rolluper.Rollup(agg, partial)
	assert.NoError(t, err)

	// Verify existing keys unchanged
	assert.Equal(t, int64(5), agg.RequestsByPath["GET /"])
	assert.Equal(t, int64(4), agg.RequestsByUserAgent["Chrome"])

	// Verify new keys are created
	assert.Equal(t, int64(3), agg.RequestsByPath["PUT /users"])
	assert.Equal(t, int64(1), agg.RequestsByPath["DELETE /sessions"])
	assert.Equal(t, int64(2), agg.RequestsByUserAgent["Safari"])
	assert.Equal(t, int64(1), agg.RequestsByUserAgent["curl"])

	// Verify all keys are present
	expectedPaths := map[string]int64{
		"GET /":            5,
		"PUT /users":       3,
		"DELETE /sessions": 1,
	}
	expectedUserAgents := map[string]int64{
		"Chrome": 4,
		"Safari": 2,
		"curl":   1,
	}
	assert.Equal(t, expectedPaths, agg.RequestsByPath)
	assert.Equal(t, expectedUserAgents, agg.RequestsByUserAgent)
}

func TestAggregateRolluper_Rollup_ComplexMerge(t *testing.T) {
	t.Parallel()

	rolluper := NewAggregateRolluper()

	windowStart := time.Date(2025, 12, 21, 14, 21, 0, 0, time.UTC)

	agg := &models.WindowAggregateResult{
		CustomerID:          "customer123",
		WindowStart:         windowStart,
		WindowSize:          models.WindowMinute,
		RequestsByPath:      map[string]int64{"GET /": 10, "POST /logs": 5},
		RequestsByUserAgent: map[string]int64{"Chrome": 8, "Firefox": 3},
	}

	// First rollup
	partial1 := &events.PartialInsightEvent{
		CustomerID:          "customer123",
		BatchID:             "batch1",
		WindowStart:         windowStart,
		WindowSize:          models.WindowMinute,
		RequestsByPath:      map[string]int64{"GET /": 2, "PUT /users": 1},
		RequestsByUserAgent: map[string]int64{"Chrome": 1, "Safari": 2},
	}

	err := rolluper.Rollup(agg, partial1)
	assert.NoError(t, err)

	// Second rollup
	partial2 := &events.PartialInsightEvent{
		CustomerID:          "customer123",
		BatchID:             "batch2",
		WindowStart:         windowStart,
		WindowSize:          models.WindowMinute,
		RequestsByPath:      map[string]int64{"POST /logs": 3, "DELETE /sessions": 1},
		RequestsByUserAgent: map[string]int64{"Firefox": 2, "curl": 1},
	}

	err = rolluper.Rollup(agg, partial2)
	assert.NoError(t, err)

	// Verify final state
	expectedPaths := map[string]int64{
		"GET /":            12, // 10 + 2
		"POST /logs":       8,  // 5 + 3
		"PUT /users":       1,
		"DELETE /sessions": 1,
	}
	expectedUserAgents := map[string]int64{
		"Chrome":  9, // 8 + 1
		"Firefox": 5, // 3 + 2
		"Safari":  2,
		"curl":    1,
	}

	assert.Equal(t, expectedPaths, agg.RequestsByPath)
	assert.Equal(t, expectedUserAgents, agg.RequestsByUserAgent)
}

func TestAggregateRolluper_Rollup_ReturnsErrorOnCustomerIDMismatch(t *testing.T) {
	t.Parallel()

	rolluper := NewAggregateRolluper()

	windowStart := time.Date(2025, 12, 21, 14, 21, 0, 0, time.UTC)

	agg := &models.WindowAggregateResult{
		CustomerID:          "customer123",
		WindowStart:         windowStart,
		WindowSize:          models.WindowMinute,
		RequestsByPath:      map[string]int64{"GET /": 5},
		RequestsByUserAgent: map[string]int64{"Chrome": 4},
	}

	partial := &events.PartialInsightEvent{
		CustomerID:          "customer456",
		BatchID:             "batch456",
		WindowStart:         windowStart,
		WindowSize:          models.WindowMinute,
		RequestsByPath:      map[string]int64{"GET /": 2},
		RequestsByUserAgent: map[string]int64{"Chrome": 1},
	}

	err := rolluper.Rollup(agg, partial)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "customerID mismatch")
	assert.Contains(t, err.Error(), "customer123")
	assert.Contains(t, err.Error(), "customer456")

	// Verify agg was not modified
	assert.Equal(t, int64(5), agg.RequestsByPath["GET /"])
	assert.Equal(t, int64(4), agg.RequestsByUserAgent["Chrome"])
}

func TestAggregateRolluper_Rollup_ReturnsErrorOnWindowStartMismatch(t *testing.T) {
	t.Parallel()

	rolluper := NewAggregateRolluper()

	windowStart1 := time.Date(2025, 12, 21, 14, 21, 0, 0, time.UTC)
	windowStart2 := time.Date(2025, 12, 21, 14, 22, 0, 0, time.UTC)

	agg := &models.WindowAggregateResult{
		CustomerID:          "customer123",
		WindowStart:         windowStart1,
		WindowSize:          models.WindowMinute,
		RequestsByPath:      map[string]int64{"GET /": 5},
		RequestsByUserAgent: map[string]int64{"Chrome": 4},
	}

	partial := &events.PartialInsightEvent{
		CustomerID:          "customer123",
		BatchID:             "batch456",
		WindowStart:         windowStart2,
		WindowSize:          models.WindowMinute,
		RequestsByPath:      map[string]int64{"GET /": 2},
		RequestsByUserAgent: map[string]int64{"Chrome": 1},
	}

	err := rolluper.Rollup(agg, partial)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "windowStart mismatch")

	// Verify agg was not modified
	assert.Equal(t, int64(5), agg.RequestsByPath["GET /"])
	assert.Equal(t, int64(4), agg.RequestsByUserAgent["Chrome"])
}

func TestAggregateRolluper_Rollup_ReturnsErrorOnWindowSizeMismatch(t *testing.T) {
	t.Parallel()

	rolluper := NewAggregateRolluper()

	windowStart := time.Date(2025, 12, 21, 14, 21, 0, 0, time.UTC)

	agg := &models.WindowAggregateResult{
		CustomerID:          "customer123",
		WindowStart:         windowStart,
		WindowSize:          models.WindowMinute,
		RequestsByPath:      map[string]int64{"GET /": 5},
		RequestsByUserAgent: map[string]int64{"Chrome": 4},
	}

	partial := &events.PartialInsightEvent{
		CustomerID:          "customer123",
		BatchID:             "batch456",
		WindowStart:         windowStart,
		WindowSize:          models.WindowHour,
		RequestsByPath:      map[string]int64{"GET /": 2},
		RequestsByUserAgent: map[string]int64{"Chrome": 1},
	}

	err := rolluper.Rollup(agg, partial)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "windowSize mismatch")
	assert.Contains(t, err.Error(), "minute")
	assert.Contains(t, err.Error(), "hour")

	// Verify agg was not modified
	assert.Equal(t, int64(5), agg.RequestsByPath["GET /"])
	assert.Equal(t, int64(4), agg.RequestsByUserAgent["Chrome"])
}
