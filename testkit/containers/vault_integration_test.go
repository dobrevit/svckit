//go:build integration

package containers_test

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"net/http"
	"testing"
	"time"

	"github.com/dobrevit/svckit/testkit/containers"
)

func TestVaultContainer(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	tc, err := containers.NewTestContainersWithConfig(ctx, &containers.TestContainerConfig{Vault: true})
	if err != nil {
		t.Fatalf("start containers with Vault: %v", err)
	}
	defer tc.Close()

	addr, err := tc.GetVaultAddress()
	if err != nil || addr == "" {
		t.Fatalf("GetVaultAddress = %q, %v", addr, err)
	}

	t.Run("transit encrypts and decrypts", func(t *testing.T) {
		mount, err := tc.EnableTransit(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tc.VaultRequest(ctx, http.MethodPost, mount+"/keys/orders", map[string]any{}); err != nil {
			t.Fatal(err)
		}
		plaintext := base64.StdEncoding.EncodeToString([]byte("a member's data"))
		enc, err := tc.VaultRequest(ctx, http.MethodPost, mount+"/encrypt/orders", map[string]any{"plaintext": plaintext})
		if err != nil {
			t.Fatal(err)
		}
		dec, err := tc.VaultRequest(ctx, http.MethodPost, mount+"/decrypt/orders", map[string]any{"ciphertext": enc["ciphertext"]})
		if err != nil {
			t.Fatal(err)
		}
		if dec["plaintext"] != plaintext {
			t.Errorf("decrypted %v, want %v", dec["plaintext"], plaintext)
		}
	})

	t.Run("the intermediate issues certificates that chain to the root", func(t *testing.T) {
		pki, err := tc.EnablePKI(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tc.VaultRequest(ctx, http.MethodPost, pki.IntermediateMount+"/roles/orders", map[string]any{
			"allowed_uri_sans": "spiffe://test/service/orders",
			"allow_any_name":   true,
			"key_type":         "ec",
			"key_bits":         256,
			"ttl":              "1h",
		}); err != nil {
			t.Fatal(err)
		}
		issued, err := tc.VaultRequest(ctx, http.MethodPost, pki.IntermediateMount+"/issue/orders", map[string]any{
			"common_name": "orders",
			"uri_sans":    "spiffe://test/service/orders",
		})
		if err != nil {
			t.Fatal(err)
		}

		leaf := parsePEM(t, issued["certificate"].(string))
		roots, intermediates := x509.NewCertPool(), x509.NewCertPool()
		roots.AddCert(parsePEM(t, pki.RootPEM))
		intermediates.AddCert(parsePEM(t, pki.IntermediatePEM))
		if _, err := leaf.Verify(x509.VerifyOptions{
			Roots: roots, Intermediates: intermediates,
			CurrentTime: time.Now(), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		}); err != nil {
			t.Errorf("the issued certificate does not chain to the root: %v", err)
		}
		if len(leaf.URIs) != 1 || leaf.URIs[0].String() != "spiffe://test/service/orders" {
			t.Errorf("URI SANs = %v", leaf.URIs)
		}
	})
}

func parsePEM(t *testing.T, s string) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode([]byte(s))
	if block == nil {
		t.Fatalf("not PEM: %q", s)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}
