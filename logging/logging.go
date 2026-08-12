// Package logging provides log/slog handlers with an established
// human-readable line format:
//
//	2006/01/02 15:04:05 [LEVEL] [service] file.go:42 - message | key=value
//
// It is stdlib-only, and it is a handler package rather than a logger API:
// applications use the standard *slog.Logger on top, so nothing here is a
// dependency they cannot walk away from. New wires a logger from the
// conventional environment variables (LOG_LEVEL, LOG_FORMAT=json for
// machine-readable output, NO_COLOR / LOG_NO_COLOR).
//
// The printf-style helpers -- InitializeGlobalLogger plus
// Debug/Info/Warn/Error/Fatal -- are a thin, stateless convenience over
// slog's default logger for code that has not moved to structured
// attributes. They hold no state and return nothing; ignoring them costs
// nothing.
package logging

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"time"
)

// InitializeGlobalLogger installs the service's logger as slog's default.
// Configuration comes from LOG_LEVEL, LOG_FORMAT and NO_COLOR/LOG_NO_COLOR.
func InitializeGlobalLogger(serviceName string) {
	slog.SetDefault(New(serviceName))
}

// Debug logs a printf-formatted message at debug level.
func Debug(msg string, args ...any) {
	logAt(slog.LevelDebug, msg, args...)
}

// Info logs a printf-formatted message at info level.
func Info(msg string, args ...any) {
	logAt(slog.LevelInfo, msg, args...)
}

// Warn logs a printf-formatted message at warn level.
func Warn(msg string, args ...any) {
	logAt(slog.LevelWarn, msg, args...)
}

// Error logs a printf-formatted message at error level.
func Error(msg string, args ...any) {
	logAt(slog.LevelError, msg, args...)
}

// Fatal logs a printf-formatted message at error level and exits.
func Fatal(msg string, args ...any) {
	logAt(slog.LevelError, msg, args...)
	os.Exit(1)
}

// logAt emits through the default slog handler with the caller's program
// counter, so file:line in the output points at the caller of
// Debug/Info/Warn/Error/Fatal rather than at this facade.
func logAt(level slog.Level, msg string, args ...any) {
	h := slog.Default().Handler()
	if !h.Enabled(context.Background(), level) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:]) // runtime.Callers, logAt, exported wrapper
	r := slog.NewRecord(time.Now(), level, fmt.Sprintf(msg, args...), pcs[0])
	_ = h.Handle(context.Background(), r)
}
