package vaultx

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/dobrevit/svckit/eventbus/seal"
)

// TransitWrapper is a seal.KeyWrapper over Vault Transit: data keys are made
// and unwrapped by Vault, under a key-encryption key that never leaves it.
// A publisher's policy needs only transit/datakey/plaintext/<key>, and a
// consumer's only transit/decrypt/<key>, so neither can do the other's part.
type TransitWrapper struct {
	client *Client
	mount  string
}

var _ seal.KeyWrapper = (*TransitWrapper)(nil)

// NewTransitWrapper returns a TransitWrapper on the Transit engine mounted
// at mount ("transit" when empty).
func NewTransitWrapper(client *Client, mount string) *TransitWrapper {
	if mount == "" {
		mount = "transit"
	}
	return &TransitWrapper{client: client, mount: mount}
}

// NewDataKey asks Vault for a fresh 256-bit data key, returned both in the
// clear and wrapped under keyName, with the key version that wrapped it.
func (w *TransitWrapper) NewDataKey(ctx context.Context, keyName string) ([]byte, []byte, string, error) {
	resp, err := w.client.request(ctx, http.MethodPost, w.mount+"/datakey/plaintext/"+keyName, map[string]any{"bits": 256}, true)
	if err != nil {
		return nil, nil, "", err
	}
	plain, _ := resp.Data["plaintext"].(string)
	wrapped, _ := resp.Data["ciphertext"].(string)
	if plain == "" || wrapped == "" {
		return nil, nil, "", errors.New("vaultx: Transit returned no data key")
	}
	key, err := base64.StdEncoding.DecodeString(plain)
	if err != nil {
		return nil, nil, "", fmt.Errorf("vaultx: data key encoding: %w", err)
	}
	version, err := keyVersion(wrapped)
	if err != nil {
		return nil, nil, "", err
	}
	return key, []byte(wrapped), version, nil
}

// Unwrap asks Vault to decrypt a wrapped data key.
func (w *TransitWrapper) Unwrap(ctx context.Context, keyName string, wrapped []byte) ([]byte, error) {
	resp, err := w.client.request(ctx, http.MethodPost, w.mount+"/decrypt/"+keyName, map[string]any{"ciphertext": string(wrapped)}, true)
	if err != nil {
		return nil, err
	}
	plain, _ := resp.Data["plaintext"].(string)
	key, err := base64.StdEncoding.DecodeString(plain)
	if err != nil || len(key) == 0 {
		return nil, errors.New("vaultx: Transit returned no plaintext")
	}
	return key, nil
}

// keyVersion reads the version from a Transit ciphertext, "vault:v3:..." →
// "3".
func keyVersion(ciphertext string) (string, error) {
	parts := strings.SplitN(ciphertext, ":", 3)
	if len(parts) != 3 || parts[0] != "vault" || !strings.HasPrefix(parts[1], "v") {
		return "", fmt.Errorf("vaultx: %q is not a Transit ciphertext", ciphertext[:min(len(ciphertext), 12)])
	}
	return strings.TrimPrefix(parts[1], "v"), nil
}
