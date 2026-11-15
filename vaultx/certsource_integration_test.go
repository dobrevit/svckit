//go:build integration

package vaultx

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	"github.com/dobrevit/svckit/eventbus/x509sig"
	"github.com/dobrevit/svckit/middleware"
	"github.com/dobrevit/svckit/testkit/containers"
)

const ordersURI = "spiffe://test/service/orders"

// recorder keeps every response body Vault sends.
type recorder struct {
	mu     sync.Mutex
	bodies [][]byte
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(body))
	r.mu.Lock()
	r.bodies = append(r.bodies, body)
	r.mu.Unlock()
	return resp, nil
}

// testPKI starts Vault with a two-tier PKI and a role "orders" that issues
// certificates for the orders service, and returns a client on it, the
// recorder of its responses, and the root CA.
func testPKI(t *testing.T) (*containers.TestContainers, *Client, *recorder, *x509.Certificate) {
	t.Helper()
	ctx := context.Background()
	tc, addr := testVault(t)
	pki, err := tc.EnablePKI(ctx)
	if err != nil {
		t.Fatal(err)
	}
	createOrdersRole(t, tc)

	rec := &recorder{}
	c, err := New(ctx, Config{Address: addr, Token: containers.VaultRootToken, HTTPClient: &http.Client{Transport: rec}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	block, _ := pem.Decode([]byte(pki.RootPEM))
	root, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return tc, c, rec, root
}

func createOrdersRole(t *testing.T, tc *containers.TestContainers) {
	t.Helper()
	if _, err := tc.VaultRequest(context.Background(), http.MethodPost, "pki_int/roles/orders", map[string]any{
		"allowed_domains":    "orders",
		"allow_bare_domains": true,
		"allowed_uri_sans":   ordersURI,
		"key_type":           "ec",
		"key_bits":           256,
		"ttl":                "1m",
		"max_ttl":            "10m",
	}); err != nil {
		t.Fatal(err)
	}
}

func ordersSource(t *testing.T, c *Client, renewAt float64) *CertSource {
	t.Helper()
	src, err := NewCertSource(context.Background(), c, CertSourceConfig{
		Role: "orders", CommonName: "orders", URISANs: []string{ordersURI}, TTL: time.Minute, RenewAt: renewAt,
	})
	if err != nil {
		t.Fatalf("NewCertSource: %v", err)
	}
	t.Cleanup(src.Close)
	return src
}

func TestCertSourceIssuesTheServicesCertificate(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	_, c, rec, root := testPKI(t)
	src := ordersSource(t, c, 0)

	cert, err := src.Certificate()
	if err != nil {
		t.Fatal(err)
	}
	if len(cert.Leaf.URIs) != 1 || cert.Leaf.URIs[0].String() != ordersURI {
		t.Errorf("URI SANs = %v, want %s", cert.Leaf.URIs, ordersURI)
	}
	roots, intermediates := x509.NewCertPool(), x509.NewCertPool()
	roots.AddCert(root)
	for _, ca := range src.IssuingChain() {
		intermediates.AddCert(ca)
	}
	if _, err := cert.Leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}); err != nil {
		t.Errorf("the certificate does not chain to the root through IssuingChain: %v", err)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, body := range rec.bodies {
		if bytes.Contains(body, []byte("PRIVATE KEY")) || bytes.Contains(body, []byte("private_key")) {
			t.Errorf("a Vault response carries a private key:\n%s", body)
		}
	}
}

// With a one-minute certificate renewed at a tenth of its life, the source
// renews long before expiry, and a signer reading from it signs with the new
// certificate.
func TestCertSourceRenewsAndTheSignerUsesTheNewCertificate(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	_, c, _, root := testPKI(t)
	src := ordersSource(t, c, 0.1)
	signer := x509sig.NewSigner(src.Certificate, 0)

	roots, intermediates := x509.NewCertPool(), x509.NewCertPool()
	roots.AddCert(root)
	for _, ca := range src.IssuingChain() {
		intermediates.AddCert(ca)
	}
	verifier, err := x509sig.NewVerifier(x509sig.Config{
		Roots: roots, Intermediates: intermediates, URIPrefix: "spiffe://test/service/",
		Publishers: map[string][]string{"order.created": {"orders"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	before, err := signer.Sign(context.Background(), eventbus.NewEvent("order.created", "orders", nil))
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		src.mu.RLock()
		renewed := src.renewals
		src.mu.RUnlock()
		if renewed > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no renewal within 30 seconds of a one-minute certificate")
		}
		time.Sleep(200 * time.Millisecond)
	}

	after, err := signer.Sign(context.Background(), eventbus.NewEvent("order.created", "orders", nil))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(before.Certificate, after.Certificate) {
		t.Error("the signer still signs with the first certificate after a renewal")
	}
	for name, signed := range map[string]*eventbus.SignedEvent{"before": before, "after": after} {
		if err := verifier.Verify(context.Background(), signed); err != nil {
			t.Errorf("message signed %s the renewal: %v", name, err)
		}
	}
}

func TestAFailedRenewalIsRetriedAndReported(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	tc, c, _, _ := testPKI(t)
	src := ordersSource(t, c, 0.1)
	health := src.HealthChecker("")

	if _, status, msg := health.Check(); status != middleware.StatusHealthy {
		t.Fatalf("a fresh source reports %s: %s", status, msg)
	}

	if _, err := tc.VaultRequest(context.Background(), http.MethodDelete, "pki_int/roles/orders", nil); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, health, middleware.StatusUnhealthy, 30*time.Second)

	createOrdersRole(t, tc)
	waitForStatus(t, health, middleware.StatusHealthy, 40*time.Second)
}

func waitForStatus(t *testing.T, h middleware.HealthChecker, want string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		_, status, msg := h.Check()
		if status == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("health stayed %s (%s), want %s within %v", status, msg, want, within)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
