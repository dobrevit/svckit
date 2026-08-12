package middleware

import (
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// pprofPrefix mirrors debug.Prefix.
//
// It is duplicated rather than imported because the debug package imports
// net/http/pprof, whose init registers the profiling handlers on
// http.DefaultServeMux. Importing it here would give every consumer of this
// package that registration as a side effect of logging requests.
const pprofPrefix = "/debug/pprof/"

// IsOperationalPath reports whether path is polled by infrastructure rather
// than requested by a user: liveness, readiness, metrics and profiling.
func IsOperationalPath(path string) bool {
	switch path {
	case "/health", "/healthz", "/ready", "/readyz", "/live", "/livez", MetricsPath:
		return true
	}
	return strings.HasPrefix(path, pprofPrefix)
}

// QuietSuccessfulProbes demotes successful requests to the operational
// endpoints from info to debug. Under an orchestrator these are the bulk of a
// service's log volume and none of its information.
//
// A probe that fails stays at info: a readiness check that starts flapping is
// precisely the thing worth seeing in normal output, and it is the worst
// possible moment to have silenced it.
func QuietSuccessfulProbes(r *http.Request, status int) bool {
	return status < http.StatusBadRequest && IsOperationalPath(r.URL.Path)
}

// LoggingOptions configures RequestLoggingWith.
type LoggingOptions struct {
	// ServiceName tags every line.
	ServiceName string
	// Logger receives the lines. A nil logger resolves to slog's default at
	// call time, so a service that installs its handler after wiring its
	// routes still gets the handler it installed.
	Logger *slog.Logger
	// Quiet decides which completed requests are logged at debug level
	// instead of info. Defaults to QuietSuccessfulProbes; set it to a
	// function returning false to log everything at info.
	Quiet func(r *http.Request, status int) bool
}

// RequestLogging returns a middleware that logs one line per completed
// request through slog's default logger, quieting successful probes.
func RequestLogging(serviceName string) func(http.Handler) http.Handler {
	return RequestLoggingWith(LoggingOptions{ServiceName: serviceName})
}

// RequestLoggingTo is RequestLogging against a specific logger.
func RequestLoggingTo(logger *slog.Logger, serviceName string) func(http.Handler) http.Handler {
	return RequestLoggingWith(LoggingOptions{ServiceName: serviceName, Logger: logger})
}

// RequestLoggingWith returns a middleware that logs one line per completed
// request: method, route, status, size, duration and client address.
func RequestLoggingWith(opts LoggingOptions) func(http.Handler) http.Handler {
	quiet := opts.Quiet
	if quiet == nil {
		quiet = QuietSuccessfulProbes
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := newRecorder(w)

			next.ServeHTTP(rec, r)

			log := opts.Logger
			if log == nil {
				log = slog.Default()
			}

			level := slog.LevelInfo
			if quiet(r, rec.status) {
				level = slog.LevelDebug
			}
			// Checking first keeps a quieted probe from formatting attributes
			// that will be discarded — the whole point of demoting it.
			if !log.Enabled(r.Context(), level) {
				return
			}

			log.LogAttrs(r.Context(), level, "http request",
				slog.String("service", opts.ServiceName),
				slog.String("method", r.Method),
				slog.String("route", Route(r)),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.status),
				slog.Int64("bytes", rec.written),
				slog.Duration("duration", time.Since(start)),
				slog.String("client_ip", ClientIP(r)),
			)
		})
	}
}
