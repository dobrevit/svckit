// Package env provides typed helpers for reading configuration from
// environment variables with defaults. An unset or empty variable — or one
// that fails to parse — yields the provided default.
package env

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// String returns the value of the environment variable, or def when unset or empty.
func String(key, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}

// Int returns the variable parsed as an int, or def when unset or unparsable.
func Int(key string, def int) int {
	if value := os.Getenv(key); value != "" {
		if n, err := strconv.Atoi(value); err == nil {
			return n
		}
	}
	return def
}

// Int64 returns the variable parsed as an int64, or def when unset or unparsable.
func Int64(key string, def int64) int64 {
	if value := os.Getenv(key); value != "" {
		if n, err := strconv.ParseInt(value, 10, 64); err == nil {
			return n
		}
	}
	return def
}

// Bool returns the variable parsed as a bool ("1", "t", "true", "0", "false", ...),
// or def when unset or unparsable.
func Bool(key string, def bool) bool {
	if value := os.Getenv(key); value != "" {
		if b, err := strconv.ParseBool(value); err == nil {
			return b
		}
	}
	return def
}

// Duration returns the variable parsed with time.ParseDuration ("30s", "5m"),
// or def when unset or unparsable.
func Duration(key string, def time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return def
}

// Slice returns the variable split on commas with whitespace trimmed and empty
// items dropped, or def when unset or empty.
func Slice(key string, def []string) []string {
	if value := os.Getenv(key); value != "" {
		result := make([]string, 0)
		for item := range strings.SplitSeq(value, ",") {
			if trimmed := strings.TrimSpace(item); trimmed != "" {
				result = append(result, trimmed)
			}
		}
		return result
	}
	return def
}
