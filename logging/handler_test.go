package logging_test

import (
	"context"
	"log/slog"
	"regexp"
	"strings"
	"testing"

	"github.com/dobrevit/svckit/logging"
)

func TestHandlerLineFormat(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(logging.NewHandler(logging.Options{
		Service: "test-service",
		Level:   slog.LevelDebug,
		Writer:  &buf,
	}))

	logger.Info("something happened")

	line := buf.String()
	// 2006/01/02 15:04:05 [INFO] [test-service] file.go:NN - message
	pattern := `^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} \[INFO\] \[test-service\] \S+\.go:\d+ - something happened\n$`
	if !regexp.MustCompile(pattern).MatchString(line) {
		t.Errorf("line %q does not match platform format %q", line, pattern)
	}
}

func TestHandlerAttrsAppended(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(logging.NewHandler(logging.Options{
		Service: "test-service",
		Level:   slog.LevelDebug,
		Writer:  &buf,
	}))

	logger.Warn("watch out", "user", "u-1", "count", 3)

	line := buf.String()
	if !strings.Contains(line, "[WARN]") {
		t.Errorf("line %q missing level", line)
	}
	if !strings.Contains(line, " | user=u-1 count=3") {
		t.Errorf("line %q missing attr suffix", line)
	}
}

func TestHandlerLevelFiltering(t *testing.T) {
	var buf strings.Builder
	h := logging.NewHandler(logging.Options{
		Service: "test-service",
		Level:   slog.LevelWarn,
		Writer:  &buf,
	})

	if h.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("Info should be disabled at Warn level")
	}
	if !h.Enabled(context.Background(), slog.LevelError) {
		t.Error("Error should be enabled at Warn level")
	}

	logger := slog.New(h)
	logger.Info("hidden")
	logger.Error("visible")

	out := buf.String()
	if strings.Contains(out, "hidden") {
		t.Errorf("output %q contains filtered message", out)
	}
	if !strings.Contains(out, "visible") {
		t.Errorf("output %q missing enabled message", out)
	}
}

func TestWithAttrsPersist(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(logging.NewHandler(logging.Options{
		Service: "test-service",
		Level:   slog.LevelDebug,
		Writer:  &buf,
	})).With("request_id", "r-9")

	logger.Info("handled")

	if !strings.Contains(buf.String(), "request_id=r-9") {
		t.Errorf("output %q missing persistent attr", buf.String())
	}
}
