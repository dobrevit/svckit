package eventbus

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/dobrevit/svckit/logging"
	"github.com/dobrevit/svckit/secrets"
)

// LoadSignatureConfig returns the standard event signature configuration with
// the signing key resolved, in order, from: the given secrets store
// (kms/event-signing-key), the EVENT_SIGNING_KEY environment variable, and the
// development fallback key. Pass a nil store to have one constructed from the
// environment (SECRETS_* configuration), which degrades to env-only resolution
// when no remote backend is configured.
//
// This replaces the getEventSigningKey() helper that was copy-pasted into
// every service entrypoint.
func LoadSignatureConfig(store secrets.Store) (*SignatureConfig, error) {
	if store == nil {
		if s, err := secrets.NewFromEnv(
			secrets.WithAlias(secrets.EventSigningKeyPath, "EVENT_SIGNING_KEY"),
		); err == nil {
			store = s
		}
	}

	key := ""
	if store != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if value, err := store.Get(ctx, secrets.EventSigningKeyPath); err == nil {
			key = value
		}
	}
	if key == "" {
		key = os.Getenv("EVENT_SIGNING_KEY")
	}
	if key == "" {
		logging.Warn("⚠️  Using default signing key - set EVENT_SIGNING_KEY in production!")
		key = "dev-signing-key-change-in-production"
	}
	if len(key) < 16 {
		return nil, fmt.Errorf("event signing key must be at least 16 characters long, got %d", len(key))
	}

	return &SignatureConfig{
		SigningKey:     []byte(key),
		ExpiryWindow:   30 * time.Minute,
		RequiredClaims: []string{"id", "type", "timestamp", "source"},
	}, nil
}
