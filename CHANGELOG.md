# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) — with the
caveat that while the version is 0.x, the API may change between minor
releases.

## [Unreleased]

### Added

- **Signing is an interface; HMAC is one implementation, with a configurable
  age limit and a strict key mode.** *Opt-in, with one edge case below.*
  `eventbus.Signer` (`Sign(ctx, event)`) and `eventbus.Verifier`
  (`Verify(ctx, signed)`) let a publisher or subscriber use another scheme;
  `eventbus.NewHMAC(config)` implements both, and `eventbus.WithSigner` /
  `WithVerifier` and `amqpcluster.WithSigner` / `WithVerifier` (new
  `amqpcluster.PublisherOption`) pass one in, with the `*SignatureConfig`
  constructors unchanged. `SignedEvent` gains `Scheme` (`hmac-sha256`; empty,
  as in every older message, means the same), and a verifier refuses a scheme
  it does not implement. `EVENT_SIGNATURE_MAX_AGE` sets the signature window
  (default 30 minutes), and a verifier's window is now what limits a
  message's age, measured from `SignedAt`, so a consumer can accept messages
  older than the publisher's window, as a long retry schedule needs.
  `eventbus.LoadSignatureConfigStrict`, `EVENT_SIGNING_KEY_REQUIRED=true` (for
  it, `LoadSignatureConfig` and `amqpcluster.LoadConfigFromEnv`) and
  `app.WithStrictSigning()` refuse to start without a key, or with
  `eventbus.DevelopmentSigningKey`, the fallback published in this
  repository. The lenient loaders and the 30-minute default remain.
  **Edge case:** a consumer whose window is shorter than the window a
  producer signed with now rejects messages older than its own window, where
  it used to follow the producer's `ExpiresAt`. With the default on both
  sides nothing changes; otherwise set `EVENT_SIGNATURE_MAX_AGE` on the
  consumer.
- **Messages can be signed with x509 certificates and verified against a
  CA** (`eventbus/x509sig`). *Opt-in.* A shared HMAC key cannot say which
  service signed a message, and cannot be revoked for one service. A
  `x509sig.Signer` signs with a service's own ECDSA P-256 key and attaches its
  certificate, taken from a `func() (*tls.Certificate, error)` so a renewing
  source can feed it; a `x509sig.Verifier` checks the chain against configured
  roots as of the signing time, the clock skew and maximum age, the signature
  over the same canonical data HMAC covers (now exported as
  `eventbus.CanonicalSigningData`), and that the service named in the
  certificate's URI SAN may publish the event's type, from a required
  allow-list. RSA, Ed25519 and other curves are refused. Revocation is by
  short certificate lifetime, and the package documentation states the window
  that leaves. Standard library only. `SignedEvent` gains `Certificate`.
- **Payloads can be sealed in an AES-256-GCM envelope under a wrapped
  per-message key** (`eventbus/seal`). *Opt-in; an unsealed event is
  unchanged on the wire.* A payload carrying personal data sat readable in
  the broker's queues, its dead-letter queues and their backups. A
  `seal.Sealer` encrypts `BaseEvent.Data` under a fresh data key per message,
  with a random 96-bit nonce and additional data binding the event's id,
  type and source, a contract version and the key's name and version, and
  zeroes the plaintext key after use; `seal.Opener` reverses it.
  `BaseEvent` gains `Sealed *SealedPayload`, and `Data` is empty while
  sealed. Data keys come from a `seal.KeyWrapper`; `seal.StaticWrapper` wraps
  them under a local key for tests and development. Signatures, HMAC and
  x509 alike, now cover a sealed payload, so a consumer verifies before it
  opens; an unsealed event's signed bytes are unchanged.
- **Publishers and subscribers connect over TLS with certificate (EXTERNAL)
  authentication.** *Opt-in.* Every connection was `amqp.Dial(url)`: an
  `amqps://` URL got the default TLS config, with no client certificate and
  no EXTERNAL authentication, so certificate-based broker access was
  impossible. `eventbus.WithTLS(config, external)` (for publishers and
  subscribers alike) and `amqpcluster.ClusterConfig.TLS` / `ExternalAuth`
  (also on `amqpcluster.Config`, for `NewClusterSubscriberWithConfig`) connect
  with a `*tls.Config` and, with external, authenticate as the user the
  certificate names. The config is used for every connection, so a
  `GetClientCertificate` that returns a renewed certificate is presented from
  the next reconnect on. `eventbus.PublisherOption` and `SubscriberOption` are
  now interfaces, so that one `WithTLS` serves both; the existing options are
  used as before. `compose.yaml` gains `rabbitmq-tls-test` (profile `tls`,
  port 5675), which the integration tests start with a CA they make.
- **`testkit/containers` starts a Vault.** *Opt-in, testkit module only.*
  `TestContainerConfig.Vault` also starts Vault in dev mode on a pinned image
  (`containers.DefaultVaultImage`, `hashicorp/vault:1.21.4`), with
  `GetVaultAddress()`, the root token `containers.VaultRootToken`,
  `EnableTransit`, `EnablePKI` (a root and an intermediate ECDSA CA, the
  intermediate signed by the root) and `VaultRequest` for anything else.

### Fixed

- **`rediscluster.NewClusterAdapter` applies the config defaults.** Only
  `LoadConfigFromEnv` did, so a `ClusterConfig` literal that left settings
  out ran with zeros: with no `HealthCheckTimeout` the adapter could not
  connect (the ping's context had already expired), with no
  `HealthCheckInterval` health monitoring panicked and silently never ran,
  with no `RetryBackoffBase` the first retry panicked in the caller's
  goroutine, and with no `CircuitBreakerThreshold` a node was degraded by its
  first error. The constructor now fills in the defaults on its own copy, a
  non-positive interval, timeout, backoff or threshold is replaced as well,
  and the backoff jitter no longer panics on a tiny base. The integration
  test that should have caught this skipped on a failed constructor; it now
  skips only when Redis itself does not answer. *Fix; no API change.*

## [0.1.5] - 2026-11-15

The messaging, database and shutdown fixes and additions of backlog items
SK-1 to SK-15. `testkit`, `chix` and `ginx` are released as v0.1.5 alongside,
each requiring svckit v0.1.5. Every item without a *Default change* label is a fix or is
opt-in; the default changes below need a look before upgrading, since a patch
version does not announce them.

### Changed (default changes)

- **Subscribers that verify signatures reject unsigned messages.** *Default
  change (security).* Whether a message was verified depended on its `signed`
  header, which the sender sets: without it, `amqpcluster.ClusterSubscriber`
  handed the message to the handler with a Warn even when built with a
  verifier, so anyone who could publish to the exchange skipped verification
  by leaving out one header. `app.New` builds exactly that subscriber. Now a
  subscriber with a verifier, in `amqpcluster` and in `eventbus`, dead-letters
  any message that is not signed and verified (reason
  `unsigned_event_rejected`), and a message marked signed reaching a
  subscriber with no verifier is dead-lettered (`no_verifier`) instead of
  being parsed as an unsigned event, which gave the handler a zero event.
  `amqpcluster.ClusterSubscriber` now records these in `RecordEventDLQ` as
  `eventbus.Subscriber` does.
  **Migration:** a consumer that still receives unsigned messages from some
  publishers sets `AllowUnsigned: true` on that subscription
  (`amqpcluster.SubscriptionOptions`, or the new
  `eventbus.SubscribeOptions` through `Subscriber.SubscribeWithOptions`) until
  they sign. `AllowUnsigned` is refused on a subscriber that requires signing:
  `eventbus`'s `requireSigning`, or `Config.SigningRequired` through the new
  `amqpcluster.NewClusterSubscriberWithConfig`, which also applies the
  config's health interval, retries and load balancing.
- **Event ids are UUIDs, and a caller can supply one.** *Default change.*
  `eventbus.NewEvent` generated ids from a nanosecond timestamp and eight
  characters from `math/rand`, which was not built to be unique across
  processes and could not serve as a database key. Ids are now random
  (version 4) UUIDs. A consumer that parsed the old timestamp prefix will
  notice; nothing in svckit did, and `ValidateEvent` still accepts any
  non-empty id. The new `eventbus.NewEventWithID(id, type, source, data)`
  keeps an id the caller chose, such as an outbox row's key. Every publish
  from `eventbus.Publisher` now carries the event id as the AMQP `MessageId`
  (it set only an `event_id` header), as `amqpcluster.ClusterPublisher`
  already did, so consumers can deduplicate on it.
- **`eventbus.Publisher` publishes persistent messages.** *Default change.*
  Its messages had no delivery mode, so they were transient, and a broker
  restart lost them even from durable queues, while
  `amqpcluster.ClusterPublisher` already published them persistent. Messages
  now survive a restart, at the cost of a disk write each. The exchange and
  queues svckit declares are durable, and a durable queue of transient
  messages is almost never intended. **Migration:** pass the new
  `eventbus.WithTransient()` to keep the old behaviour. The publisher
  constructors take `...PublisherOption` (source-compatible for callers).
- **Handlers run on a context `Close` cancels, with a configurable timeout,
  and `Close` waits for them.** *Small default change.* `eventbus` ran each
  handler on `context.Background()` with a fixed 30-second timeout, and
  `amqpcluster` on `context.Background()` with none; neither was cancelled on
  `Close`, and `eventbus.Subscriber.Close` did not wait for a handler in
  flight, so a shutdown cut a handler off mid-work or left it running past
  the process's grace period. Now `Close`, in both subscribers, stops handing
  messages to handlers (one already delivered goes back to its queue),
  cancels the consumers, waits for running handlers up to a close timeout
  (`WithCloseTimeout`, a new constructor option in both packages, default
  10 s), then cancels their context; a handler cancelled that way has its
  message requeued. `Close` returns within the close timeout plus a second,
  even with a handler that ignores its context (the broker requeues its
  message when the channel closes). New per subscription:
  `Context` (the handlers' parent) and `HandlerTimeout` (`eventbus`: zero
  keeps 30 s, negative means none; `amqpcluster`: zero keeps none).
  **What changes:** a handler still running when `Close` is called now sees
  its context cancelled after the close timeout instead of running on.
  Timeouts are otherwise unchanged. The subscriber constructors take
  `...SubscriberOption` (source-compatible for callers).
- **Per-message logs move to Debug and carry no user id.** *Default change,
  log output only.* `eventbus.Subscriber` logged `Processing event: <type>
  for user: <user id>` at Info for every message, and both packages logged
  each publish, verification and successful handling at Info: a user id is
  personal data in most deployments, and a line per message at Info is noise
  at any volume. Those lines are now Debug and name the event type and id,
  never the user; failure lines keep the type, the id and the error, never
  the body. Accepting an unsigned message stays a Warn where signatures are
  otherwise verified (`AllowUnsigned`), and is Debug on a subscriber that
  verifies nothing. Metrics are unchanged.
- **`lifecycle` drains HTTP before it stops background work, within a
  configurable budget.** *Default change.* `WaitForShutdownSignal` cancelled
  the background work (subscribers, periodic tasks) first and only then gave
  `server.Shutdown` a fixed five seconds, the opposite of what the package
  documentation says; an in-flight request that needed a background
  component failed during the drain. It now drains HTTP (requests still
  running after the drain timeout are cut), then cancels the background work
  and waits for it within what is left of the shutdown budget, then runs the
  cleanup functions. `NewManager(name, ...Option)` takes
  `WithDrainTimeout` (default 5 s), `WithShutdownBudget` (default 30 s), and
  `WithShutdownOrder(func(ctx) error)` to replace the sequence.
  **Migration:** for the old order, pass a custom order (the
  `WithShutdownOrder` doc shows one). Cleanup functions now run after the
  background work has stopped, not before: background work must end when the
  manager is dying (`Dying()`), not when a cleanup function closes its
  resource, or it waits out the budget.

### Added

- **A publish can return only after the broker confirms it, and unroutable
  messages are reported.** *Opt-in.* Without confirms a nil error from a
  publish means the frame reached the socket, not that the broker stored the
  message, and a message no queue was bound to was dropped without a trace.
  `eventbus.WithConfirms(timeout)` and the new
  `amqpcluster.ClusterConfig.PublisherConfirms` / `ConfirmTimeout` put the
  publish channel in confirm mode (again after every reconnect), publish
  mandatory, and wait for the broker: nil once it stored the message,
  `eventbus.ErrUnroutable` (a `*eventbus.ReturnedError` with the reply code
  and text) when no queue took it, `eventbus.ErrNacked` when it refused it,
  `eventbus.ErrConfirmTimeout` with no answer in time (default 5 s), and
  `ctx.Err()` when the context ends first. `amqpcluster` does not fail over a
  nacked, returned or timed-out message, which reached a broker; only a
  publish that never reached one goes to another node. Nacks and returns no
  longer count against a node's health. `RecordEventPublished` gains the
  statuses `returned`, `nacked` and `timeout`. Both publishers share one
  implementation (`internal/amqpconfirm`). New:
  `amqpcluster.NewClusterPublisherWithConfig` (to set `PublisherConfirms`)
  and `ClusterAdapter.PublishWithContext`; `ClusterPublisher.Publish` now
  honours its `ctx` and records publish metrics, which it did not.
  Unchanged without the option, except that `ClusterAdapter.Publish`'s
  final error now wraps the last node's error and failover no longer retries
  the node that just failed.
- **The queue TTL is configurable.** *Opt-in; the default is kept.* Every
  queue an `eventbus.Subscriber` declares had a hard-coded five-minute
  `x-message-ttl`, so any message waiting longer for a consumer (one down for
  a deploy, a backlog) was dead-lettered, and a consumer outage became a DLQ
  to drain by hand. `eventbus.SubscribeOptions.QueueTTL` chooses it per
  subscription, as `amqpcluster.SubscriptionOptions.TTL` already did; for
  both, the new `eventbus.NoQueueTTL` (or any negative value) declares the
  queue without one. The five-minute default stays on purpose: RabbitMQ
  refuses to redeclare an existing queue with a different TTL
  (`PRECONDITION_FAILED`, and the channel closes), so changing it would stop
  every existing consumer from starting against its own queue. A new queue
  should choose its TTL, or leave it to a broker policy, which can change
  without a redeclare.
- **Handlers mark errors retryable or permanent.** *Opt-in; unmarked errors
  behave as before.* Whether a failed message was requeued was decided by
  looking for words such as "connection", "timeout" or "invalid" in the
  error's text, with different lists in `eventbus` and `amqpcluster` (and the
  latter case-sensitive, despite its comment); a handler could not say what
  it meant. The new `eventbus.Retryable(err)` and `eventbus.Permanent(err)`
  mark an error, through any `%w` wrapping, and `eventbus.IsRetryable(err)`
  reads the mark. Both subscribers obey a mark first.
  **Deprecated:** the word matching, used only for unmarked errors, will be
  removed in a later release; mark handler errors instead.
- **Failed messages can be retried after a delay, a bounded number of times,
  then dead-lettered.** *Opt-in.* A retryable failure was
  `Nack(requeue=true)`, which put the message straight back: no delay, no
  attempt count, no limit, so a failing dependency became a hot loop that
  ended only when the queue TTL dead-lettered the message, and dead letters
  went back through the topic exchange, where any `#` binding received them
  too. The new `eventbus.RetryPolicy` (`Delays`, `MaxAttempts`), set as
  `eventbus.SubscribeOptions.Retry` or `amqpcluster.SubscriptionOptions.Retry`,
  republishes a retryable failure to a retry queue per delay
  (`<queue>.retry.<delay>`, which dead-letters back into the queue when the
  delay has passed) and dead-letters a permanent failure, or the last
  attempt, to `<queue>.dlq` with the headers `x-svckit-attempt` and
  `x-svckit-reason`. Retries and dead letters are routed by queue name and
  never pass through the topic exchange. The original is acknowledged only
  after its republish succeeds (`eventbus` republishes on a confirm-mode
  channel; `amqpcluster` through its adapter, confirmed with
  `PublisherConfirms`), so a crash in between duplicates a message rather
  than losing it. Handlers read the attempt with the new
  `eventbus.Attempt(ctx)`. Without a policy nothing changes. Enable it on a
  new queue: the main queue's dead-letter arguments differ, and RabbitMQ
  refuses to redeclare an existing queue with different arguments.
- **`pgcluster` opens lib/pq or pgx by configuration, and SQLSTATE checks
  work with either.** *Opt-in; the default stays pgx.*
  `ClusterConfig.DriverName` existed, but `LoadConfigFromEnv` never set it,
  so `app.WithDatabase` always opened pgx, and a service checking `*pq.Error`
  for a unique violation silently stopped recognising it. `LoadConfigFromEnv`
  now reads `DB_DRIVER` (`pgx`, or `postgres` for lib/pq, which the caller
  imports), and `Validate` refuses a driver that is not registered, naming
  the import. The new `pgcluster.SQLState(err)` and
  `pgcluster.IsUniqueViolation(err)` read the code through any wrapping from
  either driver's error, using the `SQLState()` method both implement, so
  svckit does not import lib/pq.
- **`app` can serve metrics and pprof on a separate operations listener.**
  *Opt-in.* `app.Handler` mounted `/metrics` and, with `LOG_LEVEL=debug`,
  pprof on the same mux as the service's routes, so a service that exposes
  its routes exposed those too. `app.WithOpsListener(addr)` (or `OPS_ADDR`)
  moves them to a second server, built from the new `App.OpsHandler()`, that
  `Run` starts and shuts down with the first; `/health` and `/ready` stay on
  the main listener, where orchestrators probe. `New` refuses an operations
  address on the main port. Without the option nothing changes.

### Deprecated

- The word matching that decides whether an unmarked handler error is retried
  (both subscribers), in favour of `eventbus.Retryable` / `eventbus.Permanent`;
  see *Handlers mark errors retryable or permanent* above. It will be removed
  in a later release.

### Fixed

- **`amqpcluster` no longer stops publishing when a node's connection drops.**
  The health check took the node's lock and then reconnected through a
  function that took the same lock again, so the first closed connection hung
  the health goroutine with the lock held, and every `Publish` after it
  blocked. The reconnect now dials outside the lock and swaps the connection
  in under it, so a slow dial no longer stalls node selection either; two
  reconnects of one node no longer race, and a reconnect that finishes after
  `Close` is discarded instead of leaked. *Fix; no API change.*
- **A degraded `amqpcluster` node returns to service.** A failed publish
  marks a node degraded, and the way back divided the node's total error
  count by the time since a check timestamp it had just reset, so the rate
  was infinite and the node stayed degraded until restart; with one node, a
  single failed publish ended publishing. Each health check now measures the
  errors since the previous one, and a degraded node whose connection and
  channel are open and whose rate is below the new
  `ClusterConfig.DegradedErrorRate` (errors per minute, default 1, today's
  intent) is healthy again. A publish channel the broker closed is recreated
  (only a nil one was). `GetNodeStats().ErrorCount` stays a running total.
  The health check now skips a node checked within half an interval rather
  than within a fixed ten seconds, so intervals below twenty seconds take
  effect. *Fix; one new optional field.*
- **`amqpcluster` consumers survive a reconnect, and prefetch is applied.** A
  node's consumers ended with its connection and were never started again, and
  a node that was down when `Subscribe` was called was never consumed from.
  The adapter now remembers its subscriptions and the exchanges, queues and
  bindings it declared, and after a node connects or reconnects (or its
  subscribe channel is recreated after a channel error) replays the
  declarations and resumes consuming into the same delivery channel.
  `SubscriptionOptions.PrefetchCount` was accepted but never applied, so a
  consumer received every ready message at once; it is now set with `Qos`
  before each consume. *Fix, with two visible effects:*
  `ClusterSubscriber.Subscribe`, whose default is `PrefetchCount: 1`, now
  holds one unacknowledged message per node at a time (pass
  `SubscribeWithOptions` with `PrefetchCount: 0` for the old unbounded
  delivery); and the channel `ClusterAdapter.Subscribe` returns is now closed
  by `Close`, so a `range` over it ends. `ClusterAdapter.Subscribe` itself
  still sets no prefetch.

### Development

- The integration tier runs one package at a time (`go test -tags integration
  -p 1`), in CI and in CONTRIBUTING: some tests restart a broker or make it
  drop every connection, which broke another package's tests running in
  parallel.
- `compose.yaml` gains `rabbitmq-test-2` (port 5674, `RABBITMQ_URL_2`), an
  independent second broker for the multi-node integration tests.

## [0.1.4] - 2026-11-15

A dependency and security release: no svckit API or behaviour changed.
`testkit`, `chix` and `ginx` are released as v0.1.4 alongside, each requiring
svckit v0.1.4.

### Changed

- **Go 1.26 is now the minimum** for every module (`go 1.26.0`; `ginx` needs
  `1.26.10`, which `gin-contrib/sse` requires). `prometheus/client_golang`
  1.25, `go-redis` 9.23 and current `golang.org/x/*` releases require it. CI
  now tests on Go 1.26 and 1.27. This is the only consumer-visible change in
  this entry; no svckit API or behaviour changed.
- Dependencies upgraded across all modules: `pgx` 5.13.0, `amqp091-go` 1.16.0,
  `go-redis` 9.23.0, `prometheus/client_golang` 1.25.0, `testify` 1.12.1,
  `chi` 5.5.0, `testcontainers-go` 0.45.0, and current `golang.org/x/*`.

### Security

- `amqp091-go` 1.13.0 → 1.16.0 fixes GO-2026-6700. It also fixes the 1.13.0
  regression that zeroed caller-owned SASL credentials after the handshake,
  which broke reusing an `amqp.Config` across reconnects.
- `pgx` 5.10.0 → 5.13.0 fixes GHSA-j38q-h3ph-jjpq (SQL injection in
  simple-protocol batches), GHSA-95fj-3fxw-g62g (`require_auth` /
  `channel_binding` bypass) and GHSA-w9rp-vfrh-q8vv (`Rows.Values` panic).
- `golang.org/x/text` fixes GO-2026-6629; in `ginx`, `golang.org/x/net` fixes
  GO-2026-6603, -6611, -6612 and -6617; in `testkit`, `klauspost/compress`
  (GO-2026-6865), `golang.org/x/crypto` (GO-2026-6354, -6355) and
  `moby/go-archive` (GO-2026-6253). `govulncheck` is clean on all four modules.

## [0.1.3] - 2026-08-12

### Added

- **`chix`** *(new module)* — chi router adapter. chi consumes svckit
  middleware directly, since `chi.Use` takes exactly
  `func(http.Handler) http.Handler`, so the module is one middleware:
  `chix.Route()` records chi's matched pattern so metrics and logs are
  labelled `/orders/{id}` rather than `/orders/42`. Without it the Prometheus
  series count grows with traffic instead of with the number of routes, and it
  does so quietly.
- **`ginx`** *(new module)* — the Gin adapter, moved out of the platform
  repository it was written in now that it has more than one consumer. Gin has
  its own handler type, context and response writer, so every middleware needs
  translating; `ginx.Use` adapts any stdlib middleware into a
  `gin.HandlerFunc` for anything not wrapped explicitly.
- `middleware.WithRouteFunc` — records a route pattern to be resolved when it
  is read, for routers that only know the match after routing. chi is the
  case that motivated it.

### Notes

- Adapters are separate modules on purpose: the core still depends on no web
  framework, so importing svckit never pulls in Gin or chi, and a project
  pinned to a different framework version is unaffected by what an adapter
  requires.

## [0.1.2] - 2026-08-12

### Changed

- **Successful requests to the operational endpoints are logged at debug
  instead of info.** Under an orchestrator, liveness and readiness probes are
  the bulk of a service's log volume and none of its information. A probe that
  *fails* stays at info: a readiness check that starts flapping is precisely
  what you want to see, and the worst thing to have silenced. Covers
  `/health`, `/healthz`, `/ready`, `/readyz`, `/live`, `/livez`, `/metrics`
  and the profiling routes; override with `RequestLoggingWith` and a `Quiet`
  function.

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

[Unreleased]: https://github.com/dobrevit/svckit/compare/v0.1.5...HEAD
[0.1.5]: https://github.com/dobrevit/svckit/releases/tag/v0.1.5
[0.1.4]: https://github.com/dobrevit/svckit/releases/tag/v0.1.4
[0.1.3]: https://github.com/dobrevit/svckit/releases/tag/v0.1.3
[0.1.2]: https://github.com/dobrevit/svckit/releases/tag/v0.1.2
[0.1.1]: https://github.com/dobrevit/svckit/releases/tag/v0.1.1
