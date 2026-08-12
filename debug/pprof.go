// Package debug gates Go's runtime profiling endpoints behind an environment
// switch, so profiling is available while investigating a service and absent
// in normal operation.
//
// The package is framework-neutral: it exposes an http.Handler and a
// *http.ServeMux registration helper, both built on net/http/pprof.
package debug

import (
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
)

// Prefix is the route prefix under which the profiling endpoints are served.
const Prefix = "/debug/pprof/"

// Enabled reports whether profiling endpoints should be served. It is true
// when LOG_LEVEL is "debug", regardless of case.
func Enabled() bool {
	return strings.EqualFold(os.Getenv("LOG_LEVEL"), "debug")
}

// IsDebugEnabled is a deprecated alias for Enabled.
//
// Deprecated: use Enabled.
func IsDebugEnabled() bool { return Enabled() }

// Handler returns a handler serving the pprof endpoints under Prefix. It
// returns nil when profiling is disabled, which lets callers skip
// registration entirely rather than mount a handler that refuses every
// request.
//
// Responses carry X-Debug-Mode: enabled, so a caller can tell a served
// profile apart from a 404 produced by a service running without profiling.
func Handler() http.Handler {
	if !Enabled() {
		return nil
	}
	return handler()
}

// Register mounts the profiling endpoints on mux when profiling is enabled,
// and does nothing otherwise. It reports whether the endpoints were mounted.
func Register(mux *http.ServeMux) bool {
	h := Handler()
	if h == nil {
		return false
	}
	mux.Handle(Prefix, h)
	return true
}

// handler builds the pprof routing table on a private mux. Serving
// http.DefaultServeMux instead — as the previous Gin implementation did —
// would expose every route any dependency registered there, not just the
// profiling endpoints.
func handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(Prefix, pprof.Index)
	mux.HandleFunc(Prefix+"cmdline", pprof.Cmdline)
	mux.HandleFunc(Prefix+"profile", pprof.Profile)
	mux.HandleFunc(Prefix+"symbol", pprof.Symbol)
	mux.HandleFunc(Prefix+"trace", pprof.Trace)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Debug-Mode", "enabled")
		mux.ServeHTTP(w, r)
	})
}
