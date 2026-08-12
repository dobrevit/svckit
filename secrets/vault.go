package secrets

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// ErrUnavailable wraps transient backend failures (network errors, sealed or
// overloaded Vault, 5xx). Retrying treats it, like ErrNotFound, as worth
// retrying; every other error (bad token, permission denied) is terminal.
var ErrUnavailable = errors.New("secret store unavailable")

// VaultStore reads and writes secrets in a HashiCorp Vault KV v2 mount using
// plain HTTP - deliberately no Vault SDK, so the 17 vendored service modules
// do not inherit its dependency tree. Logical paths map to
// {addr}/v1/{mount}/data/{prefix}/{path} with the value stored under the
// "value" field.
type VaultStore struct {
	addr   string
	token  string
	mount  string
	prefix string
	client *http.Client
}

// DefaultVaultPrefix is the path prefix used when none is configured.
// Deployments that keep their secrets under a different tree set
// SECRETS_VAULT_PREFIX or pass the prefix to NewVaultStore.
var DefaultVaultPrefix = "app"

// NewVaultStore creates a Vault-backed Store. mount defaults to "secret" and
// prefix to DefaultVaultPrefix (overridable via SECRETS_VAULT_PREFIX) when empty.
func NewVaultStore(addr, token, mount, prefix string) *VaultStore {
	if mount == "" {
		mount = "secret"
	}
	if prefix == "" {
		prefix = os.Getenv("SECRETS_VAULT_PREFIX")
	}
	if prefix == "" {
		prefix = DefaultVaultPrefix
	}
	return &VaultStore{
		addr:   strings.TrimSuffix(addr, "/"),
		token:  token,
		mount:  mount,
		prefix: prefix,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// NewVaultStoreFromEnv builds a VaultStore from VAULT_ADDR, VAULT_TOKEN,
// SECRETS_VAULT_MOUNT and SECRETS_VAULT_PREFIX.
func NewVaultStoreFromEnv() (*VaultStore, error) {
	addr := os.Getenv("VAULT_ADDR")
	if addr == "" {
		return nil, errors.New("vault backend requires VAULT_ADDR")
	}
	token := os.Getenv("VAULT_TOKEN")
	if token == "" {
		return nil, errors.New("vault backend requires VAULT_TOKEN")
	}
	return NewVaultStore(addr, token, os.Getenv("SECRETS_VAULT_MOUNT"), os.Getenv("SECRETS_VAULT_PREFIX")), nil
}

func (v *VaultStore) url(path string) string {
	return fmt.Sprintf("%s/v1/%s/data/%s/%s", v.addr, v.mount, v.prefix, path)
}

func (v *VaultStore) do(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, v.url(path), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Vault-Token", v.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vault: %s %s: %v: %w", method, path, err, ErrUnavailable)
	}
	return resp, nil
}

// Get performs a single read attempt. Compose with Retrying (as NewFromEnv
// does) to absorb bootstrap ordering; keeping this single-shot lets Chain
// consult the env fallback between retry rounds instead of blocking on Vault.
func (v *VaultStore) Get(ctx context.Context, path string) (string, error) {
	resp, err := v.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if err := v.checkStatus(resp, path); err != nil {
		return "", err
	}

	var payload struct {
		Data struct {
			Data map[string]string `json:"data"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("vault: decoding %q: %w", path, err)
	}
	value, ok := payload.Data.Data["value"]
	if !ok || value == "" {
		return "", fmt.Errorf("vault: %q has no \"value\" field: %w", path, ErrNotFound)
	}
	return value, nil
}

// Set creates or overwrites the secret at path (a new KV v2 version).
func (v *VaultStore) Set(ctx context.Context, path string, value string) error {
	body, err := json.Marshal(map[string]any{"data": map[string]string{"value": value}})
	if err != nil {
		return err
	}
	resp, err := v.do(ctx, http.MethodPost, path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return v.checkStatus(resp, path)
}

// Exists performs a single presence check without retrying.
func (v *VaultStore) Exists(ctx context.Context, path string) (bool, error) {
	_, err := v.Get(ctx, path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return false, err
}

// checkStatus maps Vault HTTP statuses onto the package's error taxonomy.
func (v *VaultStore) checkStatus(resp *http.Response, path string) error {
	switch {
	case resp.StatusCode == http.StatusOK:
		return nil
	case resp.StatusCode == http.StatusNotFound:
		return fmt.Errorf("vault: %q: %w", path, ErrNotFound)
	case resp.StatusCode == http.StatusForbidden:
		return fmt.Errorf("vault: %q (check VAULT_TOKEN and policy): %w", path, ErrAccessDenied)
	case resp.StatusCode == http.StatusTooManyRequests,
		resp.StatusCode >= 500:
		return fmt.Errorf("vault: %q returned %d: %w", path, resp.StatusCode, ErrUnavailable)
	default:
		return fmt.Errorf("vault: %q returned unexpected status %d", path, resp.StatusCode)
	}
}
