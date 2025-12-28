package models

import (
	"fmt"
	"time"
)

type WindowSize string

const (
	WindowMinute WindowSize = "minute"
	WindowHour   WindowSize = "hour"
)

func (w WindowSize) Duration() time.Duration {
	switch w {
	case WindowMinute:
		return time.Minute
	case WindowHour:
		return time.Hour
	default:
		panic(fmt.Sprintf("invalid WindowSize: %q", w))
	}
}

func (w WindowSize) FormatWindowStart(t time.Time) string {
	utc := t.UTC()

	switch w.Duration() {
	case time.Minute:
		return utc.Truncate(time.Minute).Format("20060102T1504Z")

	case time.Hour:
		return utc.Truncate(time.Hour).Format("20060102T15Z")
	}

	return ""
}

func (w WindowSize) BucketID(t time.Time) string {
	utc := t.UTC()

	switch w.Duration() {
	case time.Minute:
		return fmt.Sprintf("minute-%02d", utc.Minute())
	case time.Hour:
		return fmt.Sprintf("hour-%02d", utc.Hour())
	}
	return ""
}
