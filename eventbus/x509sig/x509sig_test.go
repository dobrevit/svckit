package x509sig

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"math/big"
	"net/url"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	"github.com/dobrevit/svckit/eventbus/seal"
)

const prefix = "spiffe://test/service/"

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newCA(t *testing.T) *testCA {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test root"},
		NotBefore:             time.Now().Add(-24 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &testCA{cert: cert, key: key, pool: pool}
}

var serial int64 = 100

// issue makes a leaf for service, with key (a fresh P-256 key when nil),
// valid between notBefore and notAfter.
func (ca *testCA) issue(t *testing.T, service string, key crypto.Signer, notBefore, notAfter time.Time) *tls.Certificate {
	t.Helper()
	if key == nil {
		key, _ = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	}
	uri, _ := url.Parse(prefix + service)
	serial++
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: service},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:         []*url.URL{uri},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, key.Public(), ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func (ca *testCA) valid(t *testing.T, service string) *tls.Certificate {
	return ca.issue(t, service, nil, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
}

func source(cert *tls.Certificate) CertificateSource {
	return func() (*tls.Certificate, error) { return cert, nil }
}

func verifierFor(t *testing.T, ca *testCA) *Verifier {
	t.Helper()
	v, err := NewVerifier(Config{
		Roots:      ca.pool,
		URIPrefix:  prefix,
		MaxAge:     time.Hour,
		Publishers: map[string][]string{"order.created": {"orders"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func sign(t *testing.T, s *Signer) *eventbus.SignedEvent {
	t.Helper()
	signed, err := s.Sign(context.Background(), eventbus.NewEvent("order.created", "orders", map[string]any{"total": 42}))
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func refused(t *testing.T, v *Verifier, signed *eventbus.SignedEvent, why string) {
	t.Helper()
	if err := v.Verify(context.Background(), signed); !errors.Is(err, ErrRefused) {
		t.Errorf("%s: Verify = %v, want it refused", why, err)
	}
}

func TestAValidMessageVerifies(t *testing.T) {
	ca := newCA(t)
	var signer eventbus.Signer = NewSigner(source(ca.valid(t, "orders")), 0)
	var verifier eventbus.Verifier = verifierFor(t, ca)

	signed, err := signer.Sign(context.Background(), eventbus.NewEvent("order.created", "orders", nil))
	if err != nil {
		t.Fatal(err)
	}
	if signed.Scheme != Scheme || len(signed.Certificate) == 0 {
		t.Errorf("scheme %q, certificate %d bytes", signed.Scheme, len(signed.Certificate))
	}
	if err := verifier.Verify(context.Background(), signed); err != nil {
		t.Errorf("Verify: %v", err)
	}
}

func TestAChangedFieldIsRefused(t *testing.T) {
	ca := newCA(t)
	v := verifierFor(t, ca)
	for name, change := range map[string]func(*eventbus.SignedEvent){
		"source":    func(s *eventbus.SignedEvent) { s.Event.Source = "billing" },
		"data":      func(s *eventbus.SignedEvent) { s.Event.Data["total"] = 43 },
		"signed at": func(s *eventbus.SignedEvent) { s.SignedAt = s.SignedAt.Add(-time.Minute) },
		"signature": func(s *eventbus.SignedEvent) {
			raw, _ := base64.StdEncoding.DecodeString(s.Signature)
			raw[len(raw)-1] ^= 1
			s.Signature = base64.StdEncoding.EncodeToString(raw)
		},
	} {
		signed := sign(t, NewSigner(source(ca.valid(t, "orders")), 0))
		change(signed)
		refused(t, v, signed, name)
	}
}

func TestACertificateFromAnotherCAIsRefused(t *testing.T) {
	ca, other := newCA(t), newCA(t)
	refused(t, verifierFor(t, ca), sign(t, NewSigner(source(other.valid(t, "orders")), 0)), "another CA")
}

func TestACertificateExpiredWhenTheMessageWasSignedIsRefused(t *testing.T) {
	ca := newCA(t)
	expired := ca.issue(t, "orders", nil, time.Now().Add(-2*time.Hour), time.Now().Add(-time.Minute))
	refused(t, verifierFor(t, ca), sign(t, NewSigner(source(expired), 0)), "expired at signing")
}

func TestSignedInTheFutureIsRefused(t *testing.T) {
	ca := newCA(t)
	s := NewSigner(source(ca.valid(t, "orders")), 0)
	s.now = func() time.Time { return time.Now().Add(5 * time.Minute) }
	refused(t, verifierFor(t, ca), sign(t, s), "beyond the clock skew")

	s.now = func() time.Time { return time.Now().Add(30 * time.Second) }
	if err := verifierFor(t, ca).Verify(context.Background(), sign(t, s)); err != nil {
		t.Errorf("30 seconds ahead, within the default skew: %v", err)
	}
}

func TestAMessageOlderThanMaxAgeIsRefused(t *testing.T) {
	ca := newCA(t)
	signed := sign(t, NewSigner(source(ca.valid(t, "orders")), 0))
	v := verifierFor(t, ca)
	v.now = func() time.Time { return signed.SignedAt.Add(61 * time.Minute) }
	refused(t, v, signed, "older than MaxAge")
}

func TestATypeTheServiceMayNotPublishIsRefused(t *testing.T) {
	ca := newCA(t)
	refused(t, verifierFor(t, ca), sign(t, NewSigner(source(ca.valid(t, "billing")), 0)), "billing publishing order.created")
}

func TestATamperedCertificateIsRefused(t *testing.T) {
	ca := newCA(t)
	v := verifierFor(t, ca)

	signed := sign(t, NewSigner(source(ca.valid(t, "orders")), 0))
	signed.Certificate[len(signed.Certificate)/2] ^= 0xff
	refused(t, v, signed, "a flipped byte")

	// A genuine certificate of the same CA, but not the signing key's.
	signed = sign(t, NewSigner(source(ca.valid(t, "orders")), 0))
	signed.Certificate = ca.valid(t, "orders").Certificate[0]
	refused(t, v, signed, "another key's certificate")
}

func TestOnlyECDSAP256IsAccepted(t *testing.T) {
	ca := newCA(t)
	_, edKey, _ := ed25519.GenerateKey(rand.Reader)
	rsaKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	p384, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)

	for name, key := range map[string]crypto.Signer{"Ed25519": edKey, "RSA": rsaKey, "P-384": p384} {
		cert := ca.issue(t, "orders", key, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
		if _, err := NewSigner(source(cert), 0).Sign(context.Background(), eventbus.NewEvent("order.created", "orders", nil)); err == nil {
			t.Errorf("%s: the signer signed", name)
		}

		// A message carrying such a certificate, however it was made.
		good := sign(t, NewSigner(source(ca.valid(t, "orders")), 0))
		good.Certificate = cert.Certificate[0]
		refused(t, verifierFor(t, ca), good, name+" certificate")
	}
}

func TestTheSchemeIsChecked(t *testing.T) {
	ca := newCA(t)
	signed := sign(t, NewSigner(source(ca.valid(t, "orders")), 0))
	signed.Scheme = eventbus.SchemeHMACSHA256
	refused(t, verifierFor(t, ca), signed, "an HMAC scheme")
}

func TestNewVerifierRequiresAnAllowList(t *testing.T) {
	ca := newCA(t)
	if _, err := NewVerifier(Config{Roots: ca.pool, URIPrefix: prefix}); err == nil {
		t.Error("a verifier without Publishers was built")
	}
	if _, err := NewVerifier(Config{Roots: ca.pool, URIPrefix: prefix, AllowAnyPublisher: true}); err != nil {
		t.Errorf("AllowAnyPublisher was refused: %v", err)
	}
}

// A certificate signature covers a sealed payload as an HMAC one does.
func TestACertificateSignatureCoversTheSealedPayload(t *testing.T) {
	ca := newCA(t)
	kek := make([]byte, 32)
	_, _ = rand.Read(kek)
	wrapper, err := seal.NewStaticWrapper(kek, "1")
	if err != nil {
		t.Fatal(err)
	}
	event := eventbus.NewEvent("order.created", "orders", map[string]any{"card": "4111 1111 1111 1111"})
	if err := seal.NewSealer(wrapper, "orders", "order.v1").Seal(context.Background(), event); err != nil {
		t.Fatal(err)
	}

	signed, err := NewSigner(source(ca.valid(t, "orders")), 0).Sign(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	v := verifierFor(t, ca)
	if err := v.Verify(context.Background(), signed); err != nil {
		t.Fatalf("Verify of a sealed event: %v", err)
	}
	signed.Event.Sealed.WrappedKey[0] ^= 1
	refused(t, v, signed, "a changed sealed payload")
}
