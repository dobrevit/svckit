// Package x509sig signs events with a service's own ECDSA P-256 key and
// certificate, and verifies them against a certificate authority, so a
// consumer knows which service published a message and can refuse one
// service's messages without rotating a key every service shares, as HMAC
// would need.
//
// The signer attaches its leaf certificate to every message. The verifier
// checks, in order: the scheme; that the certificate is ECDSA P-256 (an RSA
// or Ed25519 leaf is refused); that it chains to the configured roots and
// was valid when the message was signed; that the message was not signed
// in the future beyond the clock skew, nor longer ago than the maximum
// age; the signature over eventbus.CanonicalSigningData; and that the
// service the certificate names (in a URI SAN with the configured prefix)
// may publish the event's type.
//
// Revocation is by short certificate lifetime, not by CRL or OCSP: a stolen
// key can sign messages that verify until its certificate expires, and
// such a message is accepted for up to MaxAge after it was signed. Keep
// certificates short-lived (vaultx's CertSource renews them) and MaxAge as
// short as the slowest legitimate delivery allows.
//
// Only the standard library is used.
package x509sig

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/dobrevit/svckit/eventbus"
)

// Scheme is the SignedEvent.Scheme of this package's signatures.
const Scheme = "x509-ecdsa-p256-sha256"

// CertificateSource returns the current key and certificate to sign with. A
// renewing source (vaultx.CertSource) returns the newest one each time.
type CertificateSource func() (*tls.Certificate, error)

// Signer signs events with the key of the certificate its source returns.
type Signer struct {
	source CertificateSource
	window time.Duration
	now    func() time.Time
}

// NewSigner returns a Signer over source. window sets each message's
// ExpiresAt (eventbus.DefaultSignatureWindow when zero); verifiers decide
// the age they accept with Config.MaxAge.
func NewSigner(source CertificateSource, window time.Duration) *Signer {
	if window <= 0 {
		window = eventbus.DefaultSignatureWindow
	}
	return &Signer{source: source, window: window}
}

func (s *Signer) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// Sign signs event with the current certificate's key and attaches the
// certificate.
func (s *Signer) Sign(_ context.Context, event *eventbus.BaseEvent) (*eventbus.SignedEvent, error) {
	cert, err := s.source()
	if err != nil {
		return nil, fmt.Errorf("x509sig: get the signing certificate: %w", err)
	}
	if cert == nil || len(cert.Certificate) == 0 {
		return nil, errors.New("x509sig: the certificate source returned no certificate")
	}
	key, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
	if !ok || key.Curve != elliptic.P256() {
		return nil, errors.New("x509sig: the signing key must be ECDSA P-256")
	}
	leaf := cert.Leaf
	if leaf == nil {
		if leaf, err = x509.ParseCertificate(cert.Certificate[0]); err != nil {
			return nil, fmt.Errorf("x509sig: parse the signing certificate: %w", err)
		}
	}
	if pub, ok := leaf.PublicKey.(*ecdsa.PublicKey); !ok || !pub.Equal(&key.PublicKey) {
		return nil, errors.New("x509sig: the certificate is not for the signing key")
	}

	signedAt := s.clock()
	expiresAt := signedAt.Add(s.window)
	canonical, err := eventbus.CanonicalSigningData(event, signedAt, expiresAt)
	if err != nil {
		return nil, fmt.Errorf("x509sig: %w", err)
	}
	digest := sha256.Sum256(canonical)
	sig, err := ecdsa.SignASN1(rand.Reader, key, digest[:])
	if err != nil {
		return nil, fmt.Errorf("x509sig: sign: %w", err)
	}

	return &eventbus.SignedEvent{
		Event:       event,
		Signature:   base64.StdEncoding.EncodeToString(sig),
		SignedAt:    signedAt,
		ExpiresAt:   expiresAt,
		Scheme:      Scheme,
		Certificate: slices.Clone(cert.Certificate[0]),
	}, nil
}

// Config configures a Verifier.
type Config struct {
	// Roots are the certificate authorities a signer's certificate must
	// chain to; Intermediates may complete the chain. Roots is required.
	Roots         *x509.CertPool
	Intermediates *x509.CertPool

	// ClockSkew is how far in the future a SignedAt may be (default one
	// minute).
	ClockSkew time.Duration

	// MaxAge is the oldest message accepted, measured from SignedAt
	// (default eventbus.DefaultSignatureWindow).
	MaxAge time.Duration

	// URIPrefix precedes the service name in a certificate's URI SAN, for
	// instance "spiffe://example.org/service/". Required.
	URIPrefix string

	// Publishers lists, per event type, the services that may publish it. A
	// type that is not listed is refused. Required unless
	// AllowAnyPublisher, so that leaving it out does not mean "anyone".
	Publishers map[string][]string

	// AllowAnyPublisher lets every service with a valid certificate publish
	// every type.
	AllowAnyPublisher bool
}

// Verifier verifies events signed by Signer.
type Verifier struct {
	config Config
	now    func() time.Time
}

// NewVerifier returns a Verifier, or an error for an incomplete Config.
func NewVerifier(config Config) (*Verifier, error) {
	if config.Roots == nil {
		return nil, errors.New("x509sig: Roots is required")
	}
	if config.URIPrefix == "" {
		return nil, errors.New("x509sig: URIPrefix is required")
	}
	if len(config.Publishers) == 0 && !config.AllowAnyPublisher {
		return nil, errors.New("x509sig: Publishers is required (or set AllowAnyPublisher)")
	}
	if config.ClockSkew <= 0 {
		config.ClockSkew = time.Minute
	}
	if config.MaxAge <= 0 {
		config.MaxAge = eventbus.DefaultSignatureWindow
	}
	return &Verifier{config: config}, nil
}

func (v *Verifier) clock() time.Time {
	if v.now != nil {
		return v.now()
	}
	return time.Now()
}

// ErrRefused is wrapped by every verification failure.
var ErrRefused = errors.New("x509sig: signature refused")

func refuse(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrRefused, fmt.Sprintf(format, args...))
}

// Verify checks a signed event; see the package documentation for what is
// checked.
func (v *Verifier) Verify(_ context.Context, signed *eventbus.SignedEvent) error {
	if signed == nil || signed.Event == nil {
		return refuse("no event")
	}
	if signed.Scheme != Scheme {
		return refuse("scheme %q is not %s", signed.Scheme, Scheme)
	}

	leaf, err := x509.ParseCertificate(signed.Certificate)
	if err != nil {
		return refuse("certificate: %v", err)
	}
	pub, ok := leaf.PublicKey.(*ecdsa.PublicKey)
	if !ok || pub.Curve != elliptic.P256() {
		return refuse("certificate key is not ECDSA P-256")
	}
	if leaf.KeyUsage != 0 && leaf.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		return refuse("certificate is not for digital signatures")
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:         v.config.Roots,
		Intermediates: v.config.Intermediates,
		CurrentTime:   signed.SignedAt,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	}); err != nil {
		return refuse("certificate at the time of signing: %v", err)
	}

	now := v.clock()
	if signed.SignedAt.After(now.Add(v.config.ClockSkew)) {
		return refuse("signed %v in the future", signed.SignedAt.Sub(now))
	}
	if now.Sub(signed.SignedAt) > v.config.MaxAge {
		return refuse("signed %v ago, more than the %v accepted", now.Sub(signed.SignedAt).Round(time.Second), v.config.MaxAge)
	}

	sig, err := base64.StdEncoding.DecodeString(signed.Signature)
	if err != nil {
		return refuse("signature encoding: %v", err)
	}
	canonical, err := eventbus.CanonicalSigningData(signed.Event, signed.SignedAt, signed.ExpiresAt)
	if err != nil {
		return refuse("%v", err)
	}
	digest := sha256.Sum256(canonical)
	if !ecdsa.VerifyASN1(pub, digest[:], sig) {
		return refuse("signature does not match")
	}

	service, err := v.serviceOf(leaf)
	if err != nil {
		return err
	}
	if !v.config.AllowAnyPublisher && !slices.Contains(v.config.Publishers[signed.Event.Type], service) {
		return refuse("service %q may not publish %q", service, signed.Event.Type)
	}
	return nil
}

// ServiceOf returns the service a certificate names: the rest of its first
// URI SAN that starts with URIPrefix.
func (v *Verifier) serviceOf(leaf *x509.Certificate) (string, error) {
	for _, uri := range leaf.URIs {
		if name, ok := strings.CutPrefix(uri.String(), v.config.URIPrefix); ok && name != "" {
			return name, nil
		}
	}
	return "", refuse("certificate names no service under %q", v.config.URIPrefix)
}
