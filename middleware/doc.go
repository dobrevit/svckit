// Package middleware provides HTTP middleware as func(http.Handler)
// http.Handler, the form the standard library and every stdlib-compatible
// router understand.
//
// It covers the cross-cutting concerns a service needs before its own
// handlers matter: CORS, distributed tracing, rate limiting, request logging,
// Prometheus metrics, and service-key authentication. Identity and the
// matched route travel on the request context behind typed accessors rather
// than in a framework-specific bag of values.
package middleware
