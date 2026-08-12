// Package secrets resolves secrets by logical path, independently of where
// they are stored.
//
// Consumers ask for a path such as "service-keys/billing" through the Store
// interface; the backend — environment variables, Vault, or Kubernetes
// Secrets — is chosen at startup from SECRETS_BACKEND. Swapping backends is a
// deployment decision, not a code change, and the env backend behaves exactly
// like reading the variable directly, so a service can start there and move
// later.
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

// Well-known logical paths for the key material an infrastructure deployment
// usually shares. They are conventions, not requirements: any string is a
// valid path, and a deployment that names things differently just passes its
// own.
//
// A seeder should adopt these values from the environment rather than
// generate them. Generating a master key would orphan everything already
// encrypted under the real one.
const (
	KMSMasterKeyPath    = "kms/master-key"
	EventSigningKeyPath = "kms/event-signing-key"
	JWTSecretPath       = "platform/jwt-secret"
)
