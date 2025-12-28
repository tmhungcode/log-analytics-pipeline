package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWindowSize_Duration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		window   WindowSize
		expected time.Duration
	}{
		{
			name:     "minute window",
			window:   WindowMinute,
			expected: time.Minute,
		},
		{
			name:     "hour window",
			window:   WindowHour,
			expected: time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.window.Duration())
		})
	}
}

func TestWindowSize_Duration_Invalid(t *testing.T) {
	t.Parallel()

	invalidWindow := WindowSize("invalid")
	assert.Panics(t, func() {
		invalidWindow.Duration()
	}, "Duration should panic on invalid WindowSize")
}

func TestWindowSize_FormatWindowStart(t *testing.T) {
	t.Parallel()

	// Use a fixed time for deterministic tests
	testTime := time.Date(2025, 12, 28, 18, 3, 45, 123456789, time.UTC)

	tests := []struct {
		name     string
		window   WindowSize
		input    time.Time
		expected string
	}{
		{
			name:     "minute window truncates to minute",
			window:   WindowMinute,
			input:    testTime,
			expected: "20251228T1803Z",
		},
		{
			name:     "hour window truncates to hour",
			window:   WindowHour,
			input:    testTime,
			expected: "20251228T18Z",
		},
		{
			name:     "minute window with different timezone",
			window:   WindowMinute,
			input:    time.Date(2025, 12, 28, 18, 3, 45, 123456789, time.FixedZone("EST", -5*3600)),
			expected: "20251228T2303Z", // Converted to UTC
		},
		{
			name:     "hour window at midnight",
			window:   WindowHour,
			input:    time.Date(2025, 12, 28, 0, 0, 0, 0, time.UTC),
			expected: "20251228T00Z",
		},
		{
			name:     "minute window at start of hour",
			window:   WindowMinute,
			input:    time.Date(2025, 12, 28, 18, 0, 30, 0, time.UTC),
			expected: "20251228T1800Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.window.FormatWindowStart(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestWindowSize_BucketID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		window   WindowSize
		input    time.Time
		expected string
	}{
		{
			name:     "minute window at minute 3",
			window:   WindowMinute,
			input:    time.Date(2025, 12, 28, 18, 3, 45, 0, time.UTC),
			expected: "minute-03",
		},
		{
			name:     "minute window at minute 0",
			window:   WindowMinute,
			input:    time.Date(2025, 12, 28, 18, 0, 30, 0, time.UTC),
			expected: "minute-00",
		},
		{
			name:     "minute window at minute 59",
			window:   WindowMinute,
			input:    time.Date(2025, 12, 28, 18, 59, 30, 0, time.UTC),
			expected: "minute-59",
		},
		{
			name:     "hour window at hour 18",
			window:   WindowHour,
			input:    time.Date(2025, 12, 28, 18, 3, 45, 0, time.UTC),
			expected: "hour-18",
		},
		{
			name:     "hour window at hour 0",
			window:   WindowHour,
			input:    time.Date(2025, 12, 28, 0, 30, 0, 0, time.UTC),
			expected: "hour-00",
		},
		{
			name:     "hour window at hour 23",
			window:   WindowHour,
			input:    time.Date(2025, 12, 28, 23, 30, 0, 0, time.UTC),
			expected: "hour-23",
		},
		{
			name:     "minute window with different timezone converts to UTC",
			window:   WindowMinute,
			input:    time.Date(2025, 12, 28, 18, 3, 45, 0, time.FixedZone("EST", -5*3600)),
			expected: "minute-03", // UTC minute (18:03 EST = 23:03 UTC, but minute is 03)
		},
		{
			name:     "hour window with different timezone converts to UTC",
			window:   WindowHour,
			input:    time.Date(2025, 12, 28, 18, 30, 0, 0, time.FixedZone("EST", -5*3600)),
			expected: "hour-23", // UTC hour (18:00 EST = 23:00 UTC)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.window.BucketID(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestWindowSize_FormatWindowStart_InvalidWindow(t *testing.T) {
	t.Parallel()

	invalidWindow := WindowSize("invalid")
	testTime := time.Date(2025, 12, 28, 18, 3, 45, 0, time.UTC)

	// FormatWindowStart calls Duration() which will panic on invalid window
	assert.Panics(t, func() {
		invalidWindow.FormatWindowStart(testTime)
	}, "FormatWindowStart should panic on invalid WindowSize")
}

func TestWindowSize_BucketID_InvalidWindow(t *testing.T) {
	t.Parallel()

	invalidWindow := WindowSize("invalid")
	testTime := time.Date(2025, 12, 28, 18, 3, 45, 0, time.UTC)

	// BucketID calls Duration() which will panic on invalid window
	assert.Panics(t, func() {
		invalidWindow.BucketID(testTime)
	}, "BucketID should panic on invalid WindowSize")
}
