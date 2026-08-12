package middleware

import (
	"net/http"

	"github.com/dobrevit/svckit/httpclient"
)

// Tracing returns a middleware that adopts the caller's trace context or
// starts a new one, publishes it on the request context for outbound calls to
// continue, and echoes the identifiers on the response for correlation.
func Tracing() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			trace := httpclient.TraceContextFromHeaders(r.Header)
			if trace.TraceID == "" {
				trace = httpclient.NewTraceContext(r.Context())
			}

			w.Header().Set(httpclient.TraceIDHeader, trace.TraceID)
			w.Header().Set(httpclient.RequestIDHeader, trace.RequestID)
			if tp := trace.Traceparent(); tp != "" {
				w.Header().Set(httpclient.TraceparentHeader, tp)
			}

			next.ServeHTTP(w, r.WithContext(httpclient.ContextWithTrace(r.Context(), trace)))
		})
	}
}

// TraceFromRequest returns the trace context carried by r, or nil.
func TraceFromRequest(r *http.Request) *httpclient.TraceContext {
	return httpclient.TraceFromContext(r.Context())
}
