package eventbus

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
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
//
// The signature window comes from EVENT_SIGNATURE_MAX_AGE (default 30
// minutes). With EVENT_SIGNING_KEY_REQUIRED=true it behaves as
// LoadSignatureConfigStrict.
func LoadSignatureConfig(store secrets.Store) (*SignatureConfig, error) {
	strict := strings.EqualFold(strings.TrimSpace(os.Getenv("EVENT_SIGNING_KEY_REQUIRED")), "true")
	return loadSignatureConfig(store, strict)
}

// LoadSignatureConfigStrict is LoadSignatureConfig without the development
// fallback: it returns an error when no key is configured, and when the key
// configured is DevelopmentSigningKey itself, which is published in this
// repository. app.WithStrictSigning uses it.
func LoadSignatureConfigStrict(store secrets.Store) (*SignatureConfig, error) {
	return loadSignatureConfig(store, true)
}

func loadSignatureConfig(store secrets.Store, strict bool) (*SignatureConfig, error) {
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
	if strict {
		switch key {
		case "":
			return nil, errors.New("no event signing key is configured (EVENT_SIGNING_KEY or the secret store), and signing keys are required")
		case DevelopmentSigningKey:
			return nil, errors.New("the event signing key is the development key published in svckit's source, and signing keys are required")
		}
	}
	if key == "" {
		logging.Warn("⚠️  Using default signing key - set EVENT_SIGNING_KEY in production!")
		key = DevelopmentSigningKey
	}
	if len(key) < 16 {
		return nil, fmt.Errorf("event signing key must be at least 16 characters long, got %d", len(key))
	}

	window, err := signatureMaxAge()
	if err != nil {
		return nil, err
	}

	return &SignatureConfig{
		SigningKey:     []byte(key),
		ExpiryWindow:   window,
		RequiredClaims: []string{"id", "type", "timestamp", "source"},
	}, nil
}
