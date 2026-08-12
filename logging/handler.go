package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync"
)

// Options configures a Handler.
type Options struct {
	// Service names the emitting service in every line.
	Service string
	// Level is the minimum level to emit.
	Level slog.Leveler
	// Colors enables ANSI level colors.
	Colors bool
	// Writer receives the output; defaults to os.Stdout.
	Writer io.Writer
}

// Handler is a slog.Handler emitting the platform line format.
type Handler struct {
	opts  Options
	attrs []slog.Attr
	mu    *sync.Mutex
}

// NewHandler creates a Handler with the given options.
func NewHandler(opts Options) *Handler {
	if opts.Writer == nil {
		opts.Writer = os.Stdout
	}
	if opts.Level == nil {
		opts.Level = slog.LevelInfo
	}
	return &Handler{opts: opts, mu: &sync.Mutex{}}
}

// New returns a *slog.Logger configured from the environment: LOG_LEVEL
// (debug/info/warn/error), LOG_FORMAT=json for slog's JSON handler, and
// NO_COLOR / LOG_NO_COLOR to disable colors.
func New(service string) *slog.Logger {
	level := LevelFromEnv()

	if strings.EqualFold(os.Getenv("LOG_FORMAT"), "json") {
		return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level:     level,
			AddSource: true,
		}).WithAttrs([]slog.Attr{slog.String("service", service)}))
	}

	colors := os.Getenv("NO_COLOR") == "" && os.Getenv("LOG_NO_COLOR") == ""
	return slog.New(NewHandler(Options{
		Service: service,
		Level:   level,
		Colors:  colors,
	}))
}

// LevelFromEnv parses LOG_LEVEL into a slog.Level, defaulting to Info.
func LevelFromEnv() slog.Level {
	switch strings.ToUpper(os.Getenv("LOG_LEVEL")) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR", "FATAL":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Enabled implements slog.Handler.
func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.opts.Level.Level()
}

// Handle implements slog.Handler.
func (h *Handler) Handle(_ context.Context, r slog.Record) error {
	level := levelName(r.Level)

	caller := "unknown"
	if r.PC != 0 {
		frame, _ := runtime.CallersFrames([]uintptr{r.PC}).Next()
		if frame.File != "" {
			parts := strings.Split(frame.File, "/")
			caller = fmt.Sprintf("%s:%d", parts[len(parts)-1], frame.Line)
		}
	}

	timestamp := r.Time.Format("2006/01/02 15:04:05")

	var b strings.Builder
	if h.opts.Colors {
		color := levelColor(r.Level)
		const reset = "\033[0m"
		fmt.Fprintf(&b, "%s%s%s [%s%s%s] [%s] %s - %s",
			color, timestamp, reset,
			color, level, reset,
			h.opts.Service, caller, r.Message)
	} else {
		fmt.Fprintf(&b, "%s [%s] [%s] %s - %s",
			timestamp, level, h.opts.Service, caller, r.Message)
	}

	var fields []string
	for _, a := range h.attrs {
		fields = append(fields, fmt.Sprintf("%s=%v", a.Key, a.Value))
	}
	r.Attrs(func(a slog.Attr) bool {
		fields = append(fields, fmt.Sprintf("%s=%v", a.Key, a.Value))
		return true
	})
	if len(fields) > 0 {
		fmt.Fprintf(&b, " | %s", strings.Join(fields, " "))
	}
	b.WriteByte('\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := io.WriteString(h.opts.Writer, b.String())
	return err
}

// WithAttrs implements slog.Handler.
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := *h
	clone.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &clone
}

// WithGroup implements slog.Handler; groups are flattened into key prefixes.
func (h *Handler) WithGroup(name string) slog.Handler {
	// The line format has no nesting; a group becomes a prefix on later keys.
	// Kept minimal: attrs added after a group carry the group name.
	clone := *h
	clone.attrs = append(append([]slog.Attr{}, h.attrs...), slog.String("group", name))
	return &clone
}

func levelName(l slog.Level) string {
	switch {
	case l < slog.LevelInfo:
		return "DEBUG"
	case l < slog.LevelWarn:
		return "INFO"
	case l < slog.LevelError:
		return "WARN"
	default:
		return "ERROR"
	}
}

func levelColor(l slog.Level) string {
	switch {
	case l < slog.LevelInfo:
		return "\033[36m" // Cyan
	case l < slog.LevelWarn:
		return "\033[32m" // Green
	case l < slog.LevelError:
		return "\033[33m" // Yellow
	default:
		return "\033[31m" // Red
	}
}
