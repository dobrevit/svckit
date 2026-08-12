// Package secrets provides a backend-agnostic secret store abstraction for
// the platform, as designed in docs/architecture/SECRETS-MANAGEMENT-ARCHITECTURE.md.
//
// Consumers resolve secrets by logical path (e.g. "service-keys/billing")
// through the Store interface; the backend (env, Vault, Kubernetes Secrets) is
// selected at startup via the SECRETS_BACKEND environment variable. Phase 0
// ships the env backend only, so behavior is identical to reading environment
// variables directly.
package secrets

import (
	"context"
	"errors"
)

// Well-known backend names accepted in SECRETS_BACKEND.
const (
	BackendEnv        = "env"
	BackendVault      = "vault"
	BackendKubernetes = "kubernetes"
)

// ErrNotFound is returned when a secret does not exist at the given path.
// Chain treats it as "try the next backend"; any other error is terminal.
var ErrNotFound = errors.New("secret not found")

// ErrReadOnly is returned by Set on backends that cannot persist secrets
// (the env backend). The IAM seeder uses this to switch to adoption mode:
// read the pre-provided plaintext and derive the DB hash from it.
var ErrReadOnly = errors.New("secret store is read-only")

// ErrAccessDenied is returned when the backend refuses access (401/403).
// Under least-privilege RBAC this is an EXPECTED outcome for material a
// service is not entitled to (e.g. only IAM may read platform/jwt-secret),
// so callers can fall back quietly instead of treating it as breakage.
// It is terminal for Retrying - more attempts cannot change authorization.
var ErrAccessDenied = errors.New("access to secret denied")

// Store is the backend-agnostic secret access contract.
//
// Remote backends (Vault, Kubernetes) implement Get with retry-and-backoff
// until the secret exists or ctx is done, which absorbs bootstrap ordering:
// consumers may start before the IAM seeder has run. The env backend fails
// fast with ErrNotFound instead - environment variables cannot appear later
// in a process lifetime.
type Store interface {
	// Get returns the secret value at path.
	Get(ctx context.Context, path string) (string, error)

	// Set creates or overwrites the secret at path. Seeder (IAM) use only;
	// read-only backends return ErrReadOnly.
	Set(ctx context.Context, path string, value string) error

	// Exists reports whether a secret is present at path, without retrying.
	Exists(ctx context.Context, path string) (bool, error)
}

// ServiceKeyPath returns the logical path of a service's API key.
func ServiceKeyPath(serviceName string) string {
	return "service-keys/" + serviceName
}

// Well-known logical paths for platform key material (phase 4). The IAM
// seeder adopts these from env into the store; it never generates them - a
// generated KMS master key would orphan everything encrypted under the real
// one.
const (
	KMSMasterKeyPath    = "kms/master-key"
	EventSigningKeyPath = "kms/event-signing-key"
	JWTSecretPath       = "platform/jwt-secret"
)
