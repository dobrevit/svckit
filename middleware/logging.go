package middleware

import (
	"log/slog"
	"net/http"
	"time"
)

// RequestLogging returns a middleware that logs one line per completed
// request through slog's default logger.
func RequestLogging(serviceName string) func(http.Handler) http.Handler {
	return RequestLoggingTo(nil, serviceName)
}

// RequestLoggingTo returns a middleware that logs one line per completed
// request — method, route, status, size, duration and client address, tagged
// with serviceName — through logger. A nil logger resolves to slog's default
// at call time, so a service that installs its handler after wiring its
// routes still gets the handler it installed.
func RequestLoggingTo(logger *slog.Logger, serviceName string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := newRecorder(w)

			next.ServeHTTP(rec, r)

			log := logger
			if log == nil {
				log = slog.Default()
			}
			log.LogAttrs(r.Context(), slog.LevelInfo, "http request",
				slog.String("service", serviceName),
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
