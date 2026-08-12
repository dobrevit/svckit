package secrets

import (
	"fmt"
	"os"
	"time"
)

// DefaultCacheTTL bounds how long a consumer keeps using a rotated-out
// secret. It must stay below the IAM rotation grace window (10 minutes).
const DefaultCacheTTL = 60 * time.Second

// Default retry backoff bounds for remote backends.
const (
	defaultInitialBackoff = 200 * time.Millisecond
	defaultMaxBackoff     = 3 * time.Second
)

// NewFromEnv builds the Store selected by SECRETS_BACKEND (default "env"),
// wrapped in a TTL cache (SECRETS_CACHE_TTL, default 60s). Options are
// applied to the env backend, which stays in the chain as the permanent
// fallback for remote backends.
//
// Composition for remote backends is Cached(Retrying(Chain(remote, env))):
// the chain consults the env fallback between retry rounds, so a secret
// provided via .env resolves immediately while the remote store is still
// being seeded.
//
// Backend "kubernetes" (Phase 3) is recognized but not implemented yet;
// selecting it returns an error rather than silently degrading to env.
func NewFromEnv(opts ...EnvOption) (Store, error) {
	backend := os.Getenv("SECRETS_BACKEND")
	if backend == "" {
		backend = BackendEnv
	}

	ttl := DefaultCacheTTL
	if raw := os.Getenv("SECRETS_CACHE_TTL"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid SECRETS_CACHE_TTL %q: %w", raw, err)
		}
		ttl = parsed
	}

	switch backend {
	case BackendEnv:
		return Cached(NewEnvStore(opts...), ttl), nil
	case BackendVault, BackendKubernetes:
		remote, err := NewWritableFromEnv()
		if err != nil {
			return nil, err
		}
		chain := Chain(remote, NewEnvStore(opts...))
		return Cached(Retrying(chain, defaultInitialBackoff, defaultMaxBackoff), ttl), nil
	default:
		return nil, fmt.Errorf("unknown secrets backend %q (valid: %s, %s, %s)",
			backend, BackendEnv, BackendVault, BackendKubernetes)
	}
}

// NewWritableFromEnv returns the raw writable backend selected by
// SECRETS_BACKEND, or (nil, nil) when the env backend is active - env vars
// cannot be written, so callers switch to adoption/manual flows. Used by the
// IAM seeder and rotation, which need direct single-shot access rather than
// the cached, retrying consumer composition.
func NewWritableFromEnv() (Store, error) {
	switch backend := os.Getenv("SECRETS_BACKEND"); backend {
	case "", BackendEnv:
		return nil, nil
	case BackendVault:
		return NewVaultStoreFromEnv()
	case BackendKubernetes:
		return NewKubernetesStoreFromEnv()
	default:
		return nil, fmt.Errorf("unknown secrets backend %q (valid: %s, %s, %s)",
			backend, BackendEnv, BackendVault, BackendKubernetes)
	}
}
