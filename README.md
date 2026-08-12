# svckit

[![Go Reference](https://pkg.go.dev/badge/github.com/dobrevit/svckit.svg)](https://pkg.go.dev/github.com/dobrevit/svckit)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A Go microservice toolkit built on standard-library contracts.

svckit is the set of building blocks a service needs before it does anything
useful: configuration, logging, resilient connections to Postgres, Redis and
RabbitMQ, a signed event bus, HTTP middleware, health checks, secrets and
graceful shutdown. It grew out of a 17-service platform and was extracted once
the pieces had stopped changing shape.

## Design

**Standard-library contracts, frameworks as adapters.** Everything here is
written against `net/http`, `database/sql`, `log/slog` and `context.Context` —
never against a web framework or an ORM. Middleware is
`func(http.Handler) http.Handler`. The database layer hands back `*sql.DB`.
Logging ships `slog.Handler` implementations rather than a logger type.

The practical consequence: **the core module depends on no web framework and
no ORM.** Chi and the Go 1.22+ `http.ServeMux` consume the middleware
natively — `chi.Use` takes exactly `func(http.Handler) http.Handler`. Gin
needs translation, so it gets its own module. GORM or Ent bind to the
`*sql.DB` in about ten lines.

Adapters are separate modules, so importing svckit never pulls in a framework
you did not choose, and a project pinned to a different framework version is
unaffected by what an adapter requires.

**Where the abstraction stops.** Some things are not worth hiding: go-redis,
Prometheus and RabbitMQ appear as themselves. Small interfaces at the edges
(`Publisher`, `Subscriber`, `Store`, `IdentityProvider`) keep alternatives
possible without pre-building them.

## Install

```bash
go get github.com/dobrevit/svckit
```

Optional modules — take only what you use:

```bash
go get github.com/dobrevit/svckit/testkit  # test harness (Docker, testcontainers)
go get github.com/dobrevit/svckit/chix     # chi router adapter
go get github.com/dobrevit/svckit/ginx     # Gin framework adapter
```

### Using it from chi

Nothing to adapt — chi consumes the middleware directly:

```go
r := chi.NewRouter()
r.Use(chix.Route())                        // label metrics by route template
r.Use(middleware.Tracing())
r.Use(middleware.Metrics("orders"))
r.Use(middleware.RequestLogging("orders"))
```

`chix.Route()` is the only piece that needs a shim: chi knows the matched
pattern only after routing, and without it every path parameter becomes its
own Prometheus label value.

### Using it from Gin

Gin has its own handler type, context and response writer, so each middleware
needs translating — that is what `ginx` is:

```go
router := gin.New()
router.Use(ginx.Route())
router.Use(ginx.TracingMiddleware())
router.Use(ginx.PrometheusMiddleware("orders"))
router.Use(ginx.LoggingMiddleware())
```

`ginx.Use` adapts any `func(http.Handler) http.Handler` into a
`gin.HandlerFunc`, covering anything it does not wrap explicitly.

## Packages

| Package | What it does |
|---|---|
| [`app`](app) | Service runtime: wires config, database, messaging, health and shutdown; decorates your `http.Handler` and serves it |
| [`amqpcluster`](amqpcluster) | RabbitMQ publisher and subscriber with multi-node failover |
| [`audit`](audit) | Audit-event emitter over the event bus |
| [`auth`](auth) | JWT issue and validate, `net/http` authentication middleware, identity on the request context |
| [`buildinfo`](buildinfo) | Version metadata injected at build time via ldflags |
| [`debug`](debug) | pprof endpoints behind an environment gate |
| [`env`](env) | Typed environment-variable readers with defaults |
| [`eventbus`](eventbus) | Signed publish/subscribe, broadcast, dead-letter handling and a handler dispatcher |
| [`health`](health) | Health and readiness reporting over `*sql.DB` and the event bus |
| [`httpclient`](httpclient) | HTTP client with circuit breaker, retries, tracing and metrics |
| [`httpx`](httpx) | Response envelopes, bounded JSON decoding, clamped pagination |
| [`lifecycle`](lifecycle) | Goroutine and server lifecycle management with graceful shutdown |
| [`logging`](logging) | `log/slog` handlers: human-readable lines or JSON, configured from the environment |
| [`middleware`](middleware) | CORS, tracing, rate limiting, request logging, Prometheus metrics, service-key auth |
| [`pgcluster`](pgcluster) | Postgres cluster with writer detection, read balancing, health checks and a circuit breaker |
| [`rediscluster`](rediscluster) | Redis cluster client with health checking and load balancing |
| [`secrets`](secrets) | Secret resolution over environment variables, Vault or Kubernetes Secrets |
| [`testkit`](testkit) | Test harness: suites, mocks, assertions and container fixtures *(separate module)* |
| [`chix`](chix) | chi router adapter — route-template labelling *(separate module)* |
| [`ginx`](ginx) | Gin adapter — middleware, response helpers, pagination *(separate module)* |

## Quick start

```go
package main

import (
	"log"
	"net/http"

	"github.com/dobrevit/svckit/app"
)

func main() {
	a, err := app.New("orders",
		app.WithDatabase(),
		app.WithOptionalEventPublisher(),
	)
	if err != nil {
		log.Fatalf("bootstrap failed: %v", err)
	}
	defer a.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", handleGetOrder)

	// Handler adds tracing, metrics and request logging, and serves
	// /health, /ready, /metrics and the profiling endpoints in front of
	// your routes.
	a.Run(a.Handler(mux))
}
```

Migrations stay yours: `app.WithMigration(func(db *sql.DB) error { ... })`
takes goose, Atlas or anything else that accepts a `*sql.DB`.

## Testing

The unit tier needs nothing running:

```bash
go test -race -short ./...
```

The integration tier needs Postgres, Redis and RabbitMQ, and is gated behind a
build tag so a plain `go test ./...` never reaches it:

```bash
docker compose up -d
go test -race -tags integration ./...
```

`compose.yaml` in this repository starts the three services on the ports the
tests expect. Override any of them with `REDIS_TEST_URL`, `POSTGRES_TEST_URL`
or `RABBITMQ_TEST_URL`.

## Status

**v0.x — the API may still move.** The packages are in production use, but the
names and shapes are not yet frozen; that happens at v1.0.0. Changes are
recorded in [CHANGELOG.md](CHANGELOG.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the workflow and the design stance
that shapes review feedback. Contributions require a signed
[CLA](CLA.md); the bot prompts you on your first pull request.

## Security

Please report vulnerabilities privately rather than through an issue —
[SECURITY.md](SECURITY.md) has the process and what to expect.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
