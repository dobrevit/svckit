// Package httpclient is an HTTP client for calling other services.
//
// Over net/http it adds the things a service-to-service call needs and a bare
// client leaves to you: retries with backoff, a circuit breaker that stops
// hammering a peer that is already down, distributed-trace propagation, and
// Prometheus metrics per target service.
package httpclient
