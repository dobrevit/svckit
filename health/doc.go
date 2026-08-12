// Package health reports whether a service and its dependencies are working.
//
// It answers two different questions that are easy to conflate: liveness, for
// an operator asking what is wrong, and readiness, for an orchestrator
// deciding whether to send traffic. A degraded soft dependency belongs in the
// first and not the second.
package health
