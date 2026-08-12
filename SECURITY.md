# Security Policy

## Supported versions

svckit is pre-1.0. Security fixes land on the latest minor release only; there
are no long-term support branches yet.

| Version | Supported |
|---|---|
| latest `v0.x` | ✅ |
| older `v0.x`  | ❌ — upgrade to the latest |

Once v1.0.0 ships, this table will name a supported window.

## Reporting a vulnerability

**Please do not open a public issue for a security problem.**

Report it privately through GitHub:

1. Go to the [Security tab](https://github.com/dobrevit/svckit/security/advisories/new).
2. Click **Report a vulnerability**.

That opens a private advisory only you and the maintainers can see.

If GitHub private reporting is unavailable to you, email
**mdobrev@gmail.com** with `svckit security` in the subject.

### What to include

The more of this you can provide, the faster a fix lands:

- The affected package and version (or commit).
- What an attacker can do — the impact, not just the defect.
- Steps to reproduce, ideally a failing test or a short program.
- Any preconditions: configuration, backend in use, network position.

### What to expect

| Stage | Target |
|---|---|
| Acknowledgement that the report arrived | 3 working days |
| Initial assessment, including whether we agree it is a vulnerability | 10 working days |
| Fix released, or a plan with dates if the fix is involved | 90 days from acknowledgement |

This is a small project maintained in spare time; those are targets, not
contractual guarantees. If a deadline is going to slip you will hear why
rather than hear nothing.

We will credit you in the advisory and the changelog unless you ask us not to.
Please give us the 90 days before disclosing publicly — or less if a fix ships
sooner, and talk to us if you believe the situation calls for moving faster.

## Scope

**In scope** — anything in this repository that undermines a service using it
as documented:

- Authentication or authorization bypass in `auth` or `middleware`.
- Event signature forgery or verification bypass in `eventbus`.
- Secret disclosure through `secrets`, logs, or error messages.
- Injection reachable through the parameters these packages accept.
- Rate-limit bypass in `middleware`.
- Denial of service reachable with input a service would plausibly accept.

**Out of scope:**

- Vulnerabilities in dependencies — report those upstream. Tell us anyway if
  svckit's usage makes an upstream issue exploitable when it otherwise
  would not be.
- The `testkit` module's throwaway credentials and `compose.yaml`. They are
  deliberately weak and documented as such; they are for test fixtures on a
  development machine, never for anything holding real data.
- Missing hardening that is the deploying service's responsibility — TLS
  termination, network policy, container privileges.
- Results from automated scanners without a demonstrated impact.

## Security-relevant design notes

Worth knowing before you report, because these are deliberate:

- **`middleware.ClientIP` trusts `X-Forwarded-For` and `X-Real-IP`.** Both are
  forgeable by a direct caller, so the result is only meaningful behind a
  proxy that overwrites them. This is documented at the function; a service
  exposed directly to the internet must not treat the result as an identity.
- **`debug` serves pprof only when `LOG_LEVEL=debug`**, on a private mux
  rather than `http.DefaultServeMux`, so profiling is never exposed by
  accident and never carries unrelated routes registered elsewhere.
- **`eventbus` signs events**; verification is what stops a forged event, so a
  deployment that disables signing has opted out of that protection.
