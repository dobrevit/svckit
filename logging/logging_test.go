package logging_test

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/dobrevit/svckit/logging"
)

func TestFacadeReportsCallerNotWrapper(t *testing.T) {
	var buf strings.Builder
	prev := slog.Default()
	defer slog.SetDefault(prev)
	slog.SetDefault(slog.New(logging.NewHandler(logging.Options{
		Service: "test-service",
		Level:   slog.LevelDebug,
		Writer:  &buf,
	})))

	logging.Info("formatted %s %d", "value", 7)

	line := buf.String()
	if !strings.Contains(line, "formatted value 7") {
		t.Errorf("line %q missing formatted message", line)
	}
	if !strings.Contains(line, "logging_test.go:") {
		t.Errorf("line %q attributes the log to the wrong file; caller PC lost", line)
	}
	if strings.Contains(line, "logging.go:") {
		t.Errorf("line %q attributes the log to the facade", line)
	}
}

func TestFacadeLevelFiltering(t *testing.T) {
	var buf strings.Builder
	prev := slog.Default()
	defer slog.SetDefault(prev)
	slog.SetDefault(slog.New(logging.NewHandler(logging.Options{
		Service: "test-service",
		Level:   slog.LevelWarn,
		Writer:  &buf,
	})))

	logging.Debug("hidden %s", "detail")
	logging.Error("visible")

	out := buf.String()
	if strings.Contains(out, "hidden") {
		t.Errorf("output %q contains filtered debug line", out)
	}
	if !strings.Contains(out, "visible") {
		t.Errorf("output %q missing error line", out)
	}
}
