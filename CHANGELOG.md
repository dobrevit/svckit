# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) — with the
caveat that while the version is 0.x, the API may change between minor
releases.

## [Unreleased]

## [0.1.0] - 2026-08-12

First release. The toolkit was extracted from a 17-service platform where
these packages had been in production use; the extraction removed the
platform's domain concepts and rewrote the framework-bound parts against
standard-library contracts.

### Added

- `app` — service runtime wiring configuration, database, messaging, health
  checks and graceful shutdown. Accepts an `http.Handler` and never builds a
  router; schema migration plugs in as `func(*sql.DB) error`.
- `amqpcluster` — RabbitMQ publisher and subscriber with multi-node failover.
- `audit` — audit-event emitter over the event bus.
- `auth` — JWT issue and validate, `net/http` authentication middleware, and
  an `IdentityProvider` seam for authenticating against something other than
  this package's JWTs.
- `buildinfo` — build metadata injected through ldflags.
- `debug` — pprof endpoints behind an environment gate, served on a private
  mux rather than `http.DefaultServeMux`.
- `env` — typed environment-variable readers with defaults.
- `eventbus` — signed publish/subscribe with broadcast, dead-letter handling,
  metrics and a handler dispatcher that records real per-handler outcomes.
- `health` — health and readiness reporting over `*sql.DB` and the event bus.
- `httpclient` — HTTP client with circuit breaker, retries, distributed
  tracing and Prometheus metrics.
- `httpx` — response envelopes, bounded JSON decoding and clamped pagination.
- `lifecycle` — goroutine and HTTP-server lifecycle with graceful shutdown.
- `logging` — `log/slog` handlers producing human-readable lines or JSON,
  configured from `LOG_LEVEL`, `LOG_FORMAT` and `NO_COLOR`.
- `middleware` — CORS, tracing, rate limiting, request logging, Prometheus
  metrics and service-key authentication, all as
  `func(http.Handler) http.Handler`.
- `pgcluster` — Postgres cluster with writer detection, read balancing, health
  monitoring and a circuit breaker, over `database/sql`.
- `rediscluster` — Redis cluster client with health checking and load
  balancing.
- `secrets` — secret resolution over environment variables, Vault or
  Kubernetes Secrets.
- `testkit` — separate module carrying the test harness, so consumers of the
  runtime packages do not inherit testcontainers and Docker.

### Notes

- Distributed tracing emits the W3C `traceparent` header alongside the
  `X-Trace-ID` family, so OpenTelemetry-instrumented peers can join a trace
  without this module depending on OpenTelemetry.
- The main module depends on no web framework and no ORM.

[Unreleased]: https://github.com/dobrevit/svckit/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/dobrevit/svckit/releases/tag/v0.1.0
