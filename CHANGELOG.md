# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) — with the
caveat that while the version is 0.x, the API may change between minor
releases.

## [Unreleased]

### Changed

- The caller in a log line is now qualified with its package directory
  (`middleware/logging.go:32` rather than `logging.go:32`). The base name alone
  could not distinguish `middleware/logging.go` from `logging/logging.go`, so
  every HTTP access log appeared to be attributed to the logging package
  itself. Attribution was always correct; only the rendering was ambiguous.
  Anything parsing the human-readable format for a bare filename needs
  adjusting — the JSON handler is unaffected.

- `secrets` doc comments no longer cite sections of an architecture document
  that does not exist in this repository ("per the architecture doc §4", "see
  §7", "docs/architecture/SECRETS-MANAGEMENT-ARCHITECTURE.md"). They now say
  what they mean. The well-known path constants are documented as conventions
  a caller may ignore rather than as fixed platform paths; their values are
  unchanged, since they are a storage contract.

## [0.1.1] - 2026-08-12

**First published release.** The toolkit was extracted from a 17-service
platform where these packages had been in production use; the extraction
removed the platform's domain concepts and rewrote the framework-bound parts
against standard-library contracts.

v0.1.0 was prepared but never tagged — see below — so this is the first
version available to `go get`. The package inventory is under 0.1.0; what
follows is what changed after it.

### Added

- Tests for the six packages that had none: `amqpcluster`, `audit`,
  `buildinfo`, `eventbus`, `health` and `lifecycle`. Every published package
  now has behavioural tests.
- `audit.NewAuditClientWithPublisher`. `PublisherInterface` was exported but
  no constructor accepted one, so a service could not route audit events
  through a transport it already owned.

### Changed

- **go-redis upgraded from v8 to v9** (`github.com/redis/go-redis/v9`).
  `rediscluster` exposes go-redis types directly, so this would be a breaking
  change for anything naming them; it was done before the first tag, while
  that was still free. `ClusterAdapter.ExecuteWithRetry` now hands its
  callback a `context.Context` rather than relying on the removed
  `Client.WithContext`.
- `Close` on the Postgres, Redis and RabbitMQ cluster types now returns the
  failures it encounters, via `errors.Join`, instead of always returning
  `nil`.
- `interface{}` is spelled `any` throughout.

### Fixed

Security:

- **`eventbus.VerifyEvent` printed the signing key** — the first eight
  characters and the exact length — to stdout on every verification. The same
  leak had already been identified and removed from `SignEvent`; the fix was
  applied to one half only. Because the development default key is a constant
  in this repository, the line also announced which key was in use.
- Event and broadcast metadata travelled on the handler context under bare
  string keys, which any package could collide with or overwrite. They now use
  unexported key types, read through `eventbus.UserID` and
  `eventbus.Broadcast`.

Crashes and hangs:

- **Every `audit.AuditClient` method except `LogSecurityEvent` panicked on a
  nil client.** `app` leaves `Audit` nil when audit was wired as optional and
  the broker was unreachable, so a service using `WithOptionalAudit` crashed
  on its first audit call rather than skipping it. A nil client now means
  auditing is switched off, consistently across every method.
- **`lifecycle.Manager.Wait` blocked forever** when no goroutine had ever been
  started: tomb closes the channel it waits on only from inside a finishing
  goroutine. `WaitForShutdownSignal` calls `Wait`, so a service that
  registered no background work hung on SIGTERM instead of exiting.

Silently wrong behaviour:

- `Close` on the three cluster types discarded every per-node close failure
  (see Changed).
- `eventbus` and `amqpcluster` ignored the result of every AMQP ack and nack.
  A failed acknowledgement means the broker redelivers a message whose outcome
  it never recorded, so the duplicate surfaced far from its cause with nothing
  in the log connecting them.
- `rediscluster` applied its idle-connection setting to go-redis's
  `PoolTimeout`, which governs how long to wait for a free connection — a
  different thing entirely. It now sets `ConnMaxIdleTime`.
- `ClusterAdapter.ExecuteWithRetry` never applied `HealthCheckTimeout`: the
  timeout rode on the client via `WithContext`, which every command ignored
  because each takes a context of its own.
- `testkit`'s `NewTestContainersWithConfig` accepted a configuration and
  ignored it, silently starting the default images.
- `testkit`'s database cleanup ignored `rows.Err()`, so a failure part-way
  through iteration produced a short table list and silently skipped
  truncating the rest.
- `health`'s handlers ignored JSON encoding failures, hiding truncated
  responses.
- `lifecycle.WaitForShutdownSignal` left its signal handler registered after
  returning.

## [0.1.0] — prepared, never published

Tagging this was deliberately skipped. The commit carries the
`eventbus.VerifyEvent` key leak listed above, and Go module versions are
cached immutably by the module proxy: once published, a version can be
retracted but never withdrawn. Releasing a known credential leak under a
permanent version number was not worth the tidiness of a v0.1.0 tag.

It remains the record of what the toolkit contains.

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

[Unreleased]: https://github.com/dobrevit/svckit/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/dobrevit/svckit/releases/tag/v0.1.1
