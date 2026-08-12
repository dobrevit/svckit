package ginx

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"

	webmw "github.com/dobrevit/svckit/middleware"
)

// LoggingMiddleware logs one line per completed request. It is Gin's
// equivalent of webmw.RequestLogging, kept separate because Gin records the
// status on its own writer rather than through a wrapper.
//
// Output goes through slog's default logger, so these lines carry the
// established format. The service name is omitted because the handler already
// prefixes every line with it.
func LoggingMiddleware() gin.HandlerFunc {
	return RequestLoggingMiddleware("")
}

func logRequest(serviceName string, c *gin.Context, elapsed time.Duration) {
	// Gin reports a size of -1 when the handler wrote no body; report zero,
	// since "-1 bytes" is not a thing.
	attrs := []slog.Attr{
		slog.String("method", c.Request.Method),
		slog.String("route", routeOf(c)),
		slog.String("path", c.Request.URL.Path),
		slog.Int("status", c.Writer.Status()),
		slog.Int64("bytes", int64(max(c.Writer.Size(), 0))),
		slog.Duration("duration", elapsed),
		slog.String("client_ip", c.ClientIP()),
	}
	if serviceName != "" {
		attrs = append([]slog.Attr{slog.String("service", serviceName)}, attrs...)
	}
	if len(c.Errors) > 0 {
		attrs = append(attrs, slog.String("errors", c.Errors.String()))
	}

	// Successful probes go to debug, matching webmw.RequestLogging. The
	// decision lives in the toolkit so the Gin and stdlib paths cannot drift
	// on which endpoints count as operational.
	level := slog.LevelInfo
	if webmw.QuietSuccessfulProbes(c.Request, c.Writer.Status()) {
		level = slog.LevelDebug
	}
	if !slog.Default().Enabled(c.Request.Context(), level) {
		return
	}

	slog.LogAttrs(c.Request.Context(), level, "http request", attrs...)
}

func routeOf(c *gin.Context) string {
	if pattern := c.FullPath(); pattern != "" {
		return pattern
	}
	return webmw.Route(c.Request)
}
