package loggers

import (
	"context"
	"os"
	"time"

	"github.com/rs/zerolog"
)

// Logger is a wrapper around zerolog.Logger for convenience.
type Logger = zerolog.Logger

// New creates a new zerolog logger based on the provided log level string.
// Returns an error if the log level string cannot be parsed.
func New(level string) (Logger, error) {
	zerologLevel, err := zerolog.ParseLevel(level)
	if err != nil {
		return zerolog.Nop(), err
	}

	zerolog.TimestampFunc = func() time.Time {
		return time.Now().UTC()
	}

	// Create logger with JSON output, timestamp, and specified level
	logger := zerolog.New(os.Stdout).
		Level(zerologLevel).
		With().
		Timestamp().
		Caller().
		Logger()

	return logger, nil
}

// Ctx extracts a logger from the context.
// Returns a no-op logger if no logger is found in context.
var Ctx = func(ctx context.Context) *Logger {
	return zerolog.Ctx(ctx)
}
