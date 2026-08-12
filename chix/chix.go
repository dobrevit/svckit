// Package chix adapts svckit to the chi router.
//
// There is very little to adapt. chi's Use takes
// func(http.Handler) http.Handler, which is the form every svckit middleware
// already has, so they compose directly:
//
//	r := chi.NewRouter()
//	r.Use(chix.Route())                        // see below
//	r.Use(middleware.Tracing())
//	r.Use(middleware.Metrics("orders"))
//	r.Use(middleware.RequestLogging("orders"))
//
// The one thing chi does not give svckit for free is the matched route
// pattern, and that is what this package supplies.
package chix

import (
	"net/http"

	"github.com/dobrevit/svckit/middleware"
	"github.com/go-chi/chi/v5"
)

// Route records chi's matched route pattern so that metrics and request logs
// are labelled by template — "/orders/{id}" — rather than by concrete path.
//
// Without it every distinct ID becomes its own Prometheus label value, and the
// series count grows with traffic rather than with the number of routes. That
// failure is quiet: the metrics look right until the cardinality bill arrives.
//
// Register it before the middleware that reads the label. chi only knows the
// pattern once it has matched, which is after this middleware runs, so the
// accessor is handed over rather than the value; svckit calls it when it
// formats the label, by which time chi has filled it in.
func Route() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rctx := chi.RouteContext(r.Context())
			if rctx == nil {
				// Not routed by chi — nothing to report, and the request must
				// still be served.
				next.ServeHTTP(w, r)
				return
			}
			next.ServeHTTP(w, r.WithContext(
				middleware.WithRouteFunc(r.Context(), rctx.RoutePattern)))
		})
	}
}
