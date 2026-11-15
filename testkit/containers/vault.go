package containers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// DefaultVaultImage is the Vault TestContainerConfig.Vault starts. It is
// pinned, so that a new Vault release does not change a test run.
const DefaultVaultImage = "hashicorp/vault:1.21.4"

// VaultRootToken is the root token of the test Vault. It runs in dev mode:
// in memory, unsealed, with this token; it is for tests only.
const VaultRootToken = "svckit-test-root"

// startVault starts Vault in dev mode.
func (tc *TestContainers) startVault(config *TestContainerConfig) error {
	req := testcontainers.ContainerRequest{
		Image:        config.VaultImage,
		ExposedPorts: []string{"8200/tcp"},
		Env: map[string]string{
			"VAULT_DEV_ROOT_TOKEN_ID":  VaultRootToken,
			"VAULT_DEV_LISTEN_ADDRESS": "0.0.0.0:8200",
			"SKIP_SETCAP":              "true", // dev mode needs no mlock
		},
		WaitingFor: wait.ForHTTP("/v1/sys/health").WithPort("8200/tcp").
			WithStatusCodeMatcher(func(status int) bool { return status == http.StatusOK }).
			WithStartupTimeout(config.StartTimeout),
	}

	container, err := testcontainers.GenericContainer(tc.ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return fmt.Errorf("failed to start vault container: %w", err)
	}

	tc.VaultContainer = container
	return nil
}

// GetVaultAddress returns the test Vault's address, such as
// http://localhost:32791, for VAULT_ADDR.
func (tc *TestContainers) GetVaultAddress() (string, error) {
	if tc.VaultContainer == nil {
		return "", errors.New("vault container not started (set TestContainerConfig.Vault)")
	}
	host, err := tc.VaultContainer.Host(tc.ctx)
	if err != nil {
		return "", err
	}
	port, err := tc.VaultContainer.MappedPort(tc.ctx, "8200")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port()), nil
}

// VaultRequest sends one request to the test Vault's API (path without the
// /v1 prefix, such as "transit/keys/orders") with the root token, and
// returns the response's "data" object, which is nil for a response
// without a body. A status of 400 or more is an error naming Vault's
// errors.
func (tc *TestContainers) VaultRequest(ctx context.Context, method, path string, body any) (map[string]any, error) {
	addr, err := tc.GetVaultAddress()
	if err != nil {
		return nil, err
	}
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, addr+"/v1/"+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Vault-Token", VaultRootToken)
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }() // read in full below; nothing to do if closing fails
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var decoded struct {
		Data   map[string]any `json:"data"`
		Errors []string       `json:"errors"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil, fmt.Errorf("vault %s %s: %d, unreadable body: %w", method, path, resp.StatusCode, err)
		}
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("vault %s %s: %d %v", method, path, resp.StatusCode, decoded.Errors)
	}
	return decoded.Data, nil
}

// EnableTransit mounts the Transit secrets engine at path ("transit" when
// empty) and returns the path.
func (tc *TestContainers) EnableTransit(ctx context.Context, path string) (string, error) {
	if path == "" {
		path = "transit"
	}
	if _, err := tc.VaultRequest(ctx, http.MethodPost, "sys/mounts/"+path, map[string]any{"type": "transit"}); err != nil {
		return "", err
	}
	return path, nil
}

// VaultPKI is a two-tier certificate authority in the test Vault: a root,
// and an intermediate signed by it that issues the certificates.
type VaultPKI struct {
	RootMount         string
	IntermediateMount string
	RootPEM           string // the root CA certificate
	IntermediatePEM   string // the intermediate CA certificate
}

// EnablePKI mounts two PKI engines, "pki" and "pki_int", generates an
// ECDSA P-256 root CA in the first, and an intermediate in the second
// signed by the root, ready to issue certificates once a role exists
// (VaultRequest on "pki_int/roles/<name>").
func (tc *TestContainers) EnablePKI(ctx context.Context) (*VaultPKI, error) {
	pki := &VaultPKI{RootMount: "pki", IntermediateMount: "pki_int"}
	for _, mount := range []string{pki.RootMount, pki.IntermediateMount} {
		if _, err := tc.VaultRequest(ctx, http.MethodPost, "sys/mounts/"+mount, map[string]any{
			"type":   "pki",
			"config": map[string]any{"max_lease_ttl": "87600h"},
		}); err != nil {
			return nil, err
		}
	}

	root, err := tc.VaultRequest(ctx, http.MethodPost, pki.RootMount+"/root/generate/internal", map[string]any{
		"common_name": "svckit test root",
		"key_type":    "ec",
		"key_bits":    256,
		"ttl":         "87600h",
	})
	if err != nil {
		return nil, fmt.Errorf("generate the root CA: %w", err)
	}
	pki.RootPEM, _ = root["certificate"].(string)

	csr, err := tc.VaultRequest(ctx, http.MethodPost, pki.IntermediateMount+"/intermediate/generate/internal", map[string]any{
		"common_name": "svckit test intermediate",
		"key_type":    "ec",
		"key_bits":    256,
	})
	if err != nil {
		return nil, fmt.Errorf("generate the intermediate CSR: %w", err)
	}
	signed, err := tc.VaultRequest(ctx, http.MethodPost, pki.RootMount+"/root/sign-intermediate", map[string]any{
		"csr":    csr["csr"],
		"format": "pem",
		"ttl":    "43800h",
	})
	if err != nil {
		return nil, fmt.Errorf("sign the intermediate: %w", err)
	}
	pki.IntermediatePEM, _ = signed["certificate"].(string)
	if _, err := tc.VaultRequest(ctx, http.MethodPost, pki.IntermediateMount+"/intermediate/set-signed", map[string]any{
		"certificate": pki.IntermediatePEM,
	}); err != nil {
		return nil, fmt.Errorf("install the intermediate: %w", err)
	}
	return pki, nil
}
