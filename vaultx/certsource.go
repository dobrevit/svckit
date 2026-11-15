package vaultx

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/dobrevit/svckit/logging"
	"github.com/dobrevit/svckit/middleware"
)

// CertSourceConfig configures a CertSource.
type CertSourceConfig struct {
	// Mount and Role select the PKI engine and role that sign (Mount
	// defaults to "pki_int"). Role is required.
	Mount string
	Role  string

	// CommonName and URISANs go into each certificate request. A service
	// identity for x509sig is a URI SAN, such as
	// "spiffe://example.org/service/orders".
	CommonName string
	URISANs    []string

	// TTL is the lifetime asked for; zero leaves it to the role.
	TTL time.Duration

	// RenewAt is the fraction of a certificate's lifetime after which it is
	// renewed; zero means 2/3.
	RenewAt float64
}

// CertSource holds a short-lived certificate from Vault PKI and renews it.
// It makes the key pair itself and sends Vault only a certificate signing
// request, so the private key never leaves the process; each renewal uses a
// fresh key. It feeds a tls.Config (GetCertificate, GetClientCertificate)
// and an x509sig.Signer (Certificate).
type CertSource struct {
	client *Client
	config CertSourceConfig

	mu       sync.RWMutex
	cert     *tls.Certificate
	chain    []*x509.Certificate // the issuing CA chain, leaf excluded
	lastErr  error               // the last renewal's failure, nil after a success
	renewals int                 // successful renewals after the first issue

	stop chan struct{}
	done chan struct{}
}

// NewCertSource issues the first certificate, failing if Vault will not,
// and starts renewing it; Close stops that.
func NewCertSource(ctx context.Context, client *Client, config CertSourceConfig) (*CertSource, error) {
	if config.Role == "" {
		return nil, errors.New("vaultx: CertSourceConfig.Role is required")
	}
	if config.Mount == "" {
		config.Mount = "pki_int"
	}
	if config.RenewAt <= 0 || config.RenewAt >= 1 {
		config.RenewAt = 2.0 / 3.0
	}
	s := &CertSource{client: client, config: config, stop: make(chan struct{}), done: make(chan struct{})}
	if err := s.issue(ctx); err != nil {
		return nil, err
	}
	go s.renewLoop()
	return s, nil
}

// Close stops renewing. The current certificate stays usable until it
// expires.
func (s *CertSource) Close() {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	<-s.done
}

// Certificate returns the current certificate and its key, with the issuing
// chain. Its method value is an x509sig.CertificateSource.
func (s *CertSource) Certificate() (*tls.Certificate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.cert == nil {
		return nil, errors.New("vaultx: no certificate yet")
	}
	return s.cert, nil
}

// GetCertificate is for tls.Config.GetCertificate, on a server.
func (s *CertSource) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return s.Certificate()
}

// GetClientCertificate is for tls.Config.GetClientCertificate, on a client:
// asked on every handshake, so a reconnect presents the renewed
// certificate.
func (s *CertSource) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return s.Certificate()
}

// IssuingChain returns the CA certificates that issued the current
// certificate, nearest first: what a verifier passes as intermediates, with
// the PKI's root as its root.
func (s *CertSource) IssuingChain() []*x509.Certificate {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]*x509.Certificate(nil), s.chain...)
}

// issue makes a key and a request, has Vault sign it, and installs the
// result.
func (s *CertSource) issue(ctx context.Context) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	uris := make([]*url.URL, 0, len(s.config.URISANs))
	for _, raw := range s.config.URISANs {
		u, err := url.Parse(raw)
		if err != nil {
			return fmt.Errorf("vaultx: URI SAN %q: %w", raw, err)
		}
		uris = append(uris, u)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: s.config.CommonName},
		URIs:    uris,
	}, key)
	if err != nil {
		return fmt.Errorf("vaultx: certificate request: %w", err)
	}

	body := map[string]any{
		"csr":         string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})),
		"common_name": s.config.CommonName,
		"format":      "pem",
	}
	if len(s.config.URISANs) > 0 {
		body["uri_sans"] = strings.Join(s.config.URISANs, ",")
	}
	if s.config.TTL > 0 {
		body["ttl"] = s.config.TTL.String()
	}
	resp, err := s.client.request(ctx, http.MethodPost, s.config.Mount+"/sign/"+s.config.Role, body, true)
	if err != nil {
		return fmt.Errorf("vaultx: sign the certificate: %w", err)
	}

	leaf, err := parseCertificatePEM(resp.Data["certificate"])
	if err != nil {
		return fmt.Errorf("vaultx: issued certificate: %w", err)
	}
	if pub, ok := leaf.PublicKey.(*ecdsa.PublicKey); !ok || !pub.Equal(&key.PublicKey) {
		return errors.New("vaultx: Vault returned a certificate for another key")
	}
	var chain []*x509.Certificate
	if list, ok := resp.Data["ca_chain"].([]any); ok && len(list) > 0 {
		for _, item := range list {
			ca, err := parseCertificatePEM(item)
			if err != nil {
				return fmt.Errorf("vaultx: CA chain: %w", err)
			}
			chain = append(chain, ca)
		}
	} else if ca, err := parseCertificatePEM(resp.Data["issuing_ca"]); err == nil {
		chain = append(chain, ca)
	}

	cert := &tls.Certificate{Certificate: [][]byte{leaf.Raw}, PrivateKey: key, Leaf: leaf}
	for _, ca := range chain {
		cert.Certificate = append(cert.Certificate, ca.Raw)
	}
	s.mu.Lock()
	s.cert, s.chain = cert, chain
	s.mu.Unlock()
	return nil
}

// renewLoop renews at RenewAt of the current certificate's lifetime, and
// after a failure retries with a backoff from one second up to a minute.
func (s *CertSource) renewLoop() {
	defer close(s.done)
	backoff := time.Second
	for {
		s.mu.RLock()
		leaf := s.cert.Leaf
		failing := s.lastErr != nil
		s.mu.RUnlock()

		wait := time.Until(leaf.NotBefore.Add(time.Duration(float64(leaf.NotAfter.Sub(leaf.NotBefore)) * s.config.RenewAt)))
		if failing {
			wait = backoff
		}
		select {
		case <-s.stop:
			return
		case <-time.After(wait):
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := s.issue(ctx)
		cancel()

		s.mu.Lock()
		s.lastErr = err
		if err == nil {
			s.renewals++
		}
		s.mu.Unlock()
		if err != nil {
			logging.Error("vaultx: renewing the %s certificate failed, retrying in %v: %v", s.config.CommonName, backoff, err)
			backoff = min(backoff*2, time.Minute)
		} else {
			backoff = time.Second
		}
	}
}

// HealthChecker reports the certificate source unhealthy while its last
// renewal failed or its certificate has expired.
func (s *CertSource) HealthChecker(name string) middleware.HealthChecker {
	if name == "" {
		name = "vault-pki"
	}
	return certHealth{s: s, name: name}
}

type certHealth struct {
	s    *CertSource
	name string
}

func (h certHealth) Check() (string, string, string) {
	h.s.mu.RLock()
	defer h.s.mu.RUnlock()
	switch {
	case h.s.cert == nil:
		return h.name, middleware.StatusUnhealthy, "no certificate"
	case time.Now().After(h.s.cert.Leaf.NotAfter):
		return h.name, middleware.StatusUnhealthy, "the certificate has expired"
	case h.s.lastErr != nil:
		return h.name, middleware.StatusUnhealthy, "renewal failing: " + h.s.lastErr.Error()
	}
	return h.name, middleware.StatusHealthy, "certificate valid until " + h.s.cert.Leaf.NotAfter.UTC().Format(time.RFC3339)
}

func parseCertificatePEM(v any) (*x509.Certificate, error) {
	s, _ := v.(string)
	block, _ := pem.Decode([]byte(s))
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("not a PEM certificate")
	}
	return x509.ParseCertificate(block.Bytes)
}
