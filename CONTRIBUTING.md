# Contributing to svckit

## Before you start

For anything beyond a typo or an obvious bug fix, open an issue first. svckit
is a toolkit with a specific design stance — see below — and it is quicker to
agree on the approach than to rework a finished pull request.

## The design stance

Two rules shape most review feedback, so they are worth knowing up front:

1. **Standard-library contracts.** Code here is written against `net/http`,
   `database/sql`, `log/slog` and `context.Context`. Middleware is
   `func(http.Handler) http.Handler`. A change that introduces a web framework
   or an ORM into the main module will be declined regardless of its merits —
   the absence of those dependencies is the point of the library.
2. **The abstraction stops somewhere.** go-redis, Prometheus and RabbitMQ
   appear as themselves. Proposals to hide them behind an interface need to
   show a second implementation someone actually wants, not just the
   possibility of one.

## Development

```bash
go build ./... && go vet ./... && go test -race -short ./...
cd testkit && go build ./... && go test -race -short ./...
```

The integration tier needs Postgres, Redis and RabbitMQ, and is behind a build
tag so an ordinary test run never reaches it:

```bash
docker compose up -d --wait
go test -race -tags integration ./...
docker compose down -v
```

Before pushing:

```bash
gofmt -l .            # must print nothing
golangci-lint run ./...
go mod tidy           # must leave go.mod and go.sum unchanged
```

CI runs all of these, plus a Go version matrix.

## Pull requests

- **One concern per pull request.** A refactor and a bug fix in the same
  branch take twice as long to review.
- **New behaviour needs a test.** A bug fix needs a test that fails before it.
- **Exported identifiers need a doc comment** that says what the thing is for,
  not what its name already says. Where behaviour is surprising or a default
  is deliberate, say why — that is the comment worth writing.
- **Keep the public API small.** Everything exported is something that cannot
  change without a version bump.

### Commit messages

Explain why the change is needed, not just what it does. If a change is
subtle, the commit message is where a future reader finds out what you knew at
the time.

### Breaking changes

svckit is v0.x, so the API can still move — but say so explicitly in the pull
request and add a `CHANGELOG.md` entry under **Unreleased**, so the next
release notes are accurate.

## Contributor License Agreement

Contributions require a signed CLA — see [CLA.md](CLA.md). The bot will prompt
you on your first pull request; you sign once and subsequent contributions are
covered.

If your employer owns the rights to your work, arrange that before signing.

## Security

Do not report vulnerabilities through issues or pull requests. See
[SECURITY.md](SECURITY.md) for the private reporting process.

## Licence

Contributions are licensed under [Apache-2.0](LICENSE), the same terms as the
project.
