# Enabling the CLA check

The CLA workflow (`.github/workflows/cla.yml`) skips itself until the
signature store is configured, so pull requests are not blocked by a check
that cannot work yet. Three steps switch it on.

## 1. Create the signatures repository

Create **`dobrevit/cla-signatures`**. It can be private. It only ever holds
`signatures/version1/cla.json`, a list of who has signed and when.

It is a separate repository on purpose: signatures are the record that a
contributor agreed to the terms, and that record should not be modifiable
from a pull request against the project it governs.

## 2. Create a token

A fine-grained personal access token with:

- **Repository access:** only `dobrevit/cla-signatures`
- **Permissions:** `Contents: Read and write`

Nothing else. The token never needs access to `svckit` itself — the workflow
uses the automatic `GITHUB_TOKEN` for commenting on pull requests.

## 3. Add it as a secret

In `svckit` → Settings → Secrets and variables → Actions, add a repository
secret named **`CLA_SIGNATURES_TOKEN`** holding that token.

The next pull request will run the check.

## Behaviour once enabled

- A contributor who has not signed gets a comment explaining how to.
- Signing is a comment on the pull request with exactly:
  `I have read the CLA document and I hereby sign the CLA`
- The signature is recorded against their GitHub account; later pull requests
  are covered automatically.
- **Bots are skipped entirely** — the job does not run when the actor's name
  ends in `[bot]`, so dependabot updates are never blocked.
- Accounts in the workflow's `allowlist` are exempt.

## If you would rather not run a CLA at all

Delete `.github/workflows/cla.yml`, `CLA.md` and this file, and remove the CLA
section from `CONTRIBUTING.md`. Apache-2.0 already grants inbound contribution
rights through §5 of the licence; a CLA adds an explicit patent grant and a
representation that the contributor had the right to contribute, which matters
most when accepting work from people whose employers may claim it.
