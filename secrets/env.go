package secrets

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// EnvStore resolves secrets from environment variables. It is the default
// backend, the permanent fallback link in the lookup chain, and the backend
// used by tests and the compose test profile (with fixture keys adopted by
// the IAM seeder).
type EnvStore struct {
	// aliases maps a logical path to environment variable names tried, in
	// order, before the generic EnvVarName transform. Used to keep legacy
	// variables working, e.g. service-keys/<self> -> SERVICE_API_KEY.
	aliases map[string][]string
}

// EnvOption configures an EnvStore.
type EnvOption func(*EnvStore)

// WithAlias registers environment variable names to try for a logical path
// before the generic transform. Later calls for the same path append.
func WithAlias(path string, envVars ...string) EnvOption {
	return func(s *EnvStore) {
		s.aliases[path] = append(s.aliases[path], envVars...)
	}
}

// NewEnvStore creates an environment-variable-backed Store.
func NewEnvStore(opts ...EnvOption) *EnvStore {
	s := &EnvStore{aliases: make(map[string][]string)}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// EnvVarName returns the environment variable a logical path maps to under
// the generic transform: "service-keys/billing" -> "SECRET_SERVICE_KEYS_BILLING".
// Exported so operators can predict variable names from documented paths.
func EnvVarName(path string) string {
	name := strings.ToUpper(path)
	name = strings.NewReplacer("/", "_", "-", "_", ".", "_").Replace(name)
	return "SECRET_" + name
}

// Get resolves path via its aliases first, then the generic transform.
// Unlike remote backends it fails fast with ErrNotFound: environment
// variables cannot appear later in the process lifetime, so retrying
// would only stall startup.
func (s *EnvStore) Get(_ context.Context, path string) (string, error) {
	for _, envVar := range s.candidates(path) {
		if value := os.Getenv(envVar); value != "" {
			return value, nil
		}
	}
	return "", fmt.Errorf("env backend: %q (tried %s): %w",
		path, strings.Join(s.candidates(path), ", "), ErrNotFound)
}

// Set is not supported: processes cannot persist environment variables.
func (s *EnvStore) Set(_ context.Context, path string, _ string) error {
	return fmt.Errorf("env backend: cannot write %q: %w", path, ErrReadOnly)
}

// Exists reports whether any candidate variable for path is set and non-empty.
func (s *EnvStore) Exists(_ context.Context, path string) (bool, error) {
	for _, envVar := range s.candidates(path) {
		if os.Getenv(envVar) != "" {
			return true, nil
		}
	}
	return false, nil
}

func (s *EnvStore) candidates(path string) []string {
	return append(append([]string{}, s.aliases[path]...), EnvVarName(path))
}
