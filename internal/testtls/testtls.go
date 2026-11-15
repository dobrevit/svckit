// Package testtls starts compose's rabbitmq-tls-test broker for the
// integration tests of TLS and certificate (EXTERNAL) authentication, with
// a certificate authority made fresh for the run, and issues client
// certificates from it. Only tests use it.
package testtls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// Broker is a running rabbitmq-tls-test and the CA its certificates are
// from.
type Broker struct {
	URL  string // amqps:// URL, with no credentials
	Pool *x509.CertPool

	root   string
	ca     *x509.Certificate
	caKey  *ecdsa.PrivateKey
	serial atomic.Int64
}

// Start makes a CA and a server certificate, writes them where compose
// mounts them, starts rabbitmq-tls-test, and removes it when the test ends.
func Start(t *testing.T) *Broker {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	b := &Broker{URL: "amqps://localhost:5675/", root: root}
	if url := os.Getenv("RABBITMQ_TLS_URL"); url != "" {
		b.URL = url
	}
	b.serial.Store(1)
	b.ca, b.caKey = newCA(t, "svckit test CA")
	b.Pool = x509.NewCertPool()
	b.Pool.AddCert(b.ca)

	dir := filepath.Join(root, ".compose-tls")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	server := b.issue(t, "localhost", b.ca, b.caKey, "", x509.ExtKeyUsageServerAuth)
	write := func(name string, block *pem.Block) {
		// Readable by the broker's own user inside the container; the key
		// is a throwaway made for this run.
		if err := os.WriteFile(filepath.Join(dir, name), pem.EncodeToMemory(block), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("ca.pem", &pem.Block{Type: "CERTIFICATE", Bytes: b.ca.Raw})
	write("server.pem", &pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate[0]})
	keyDER, _ := x509.MarshalECPrivateKey(server.PrivateKey.(*ecdsa.PrivateKey))
	write("server-key.pem", &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	b.compose(t, "--profile", "tls", "up", "-d", "--wait", "--force-recreate", "rabbitmq-tls-test")
	t.Cleanup(func() { _, _ = b.composeOutput("--profile", "tls", "rm", "-sf", "rabbitmq-tls-test") })
	return b
}

// AddUser creates a broker user, with every permission on the default
// vhost, that a certificate with name as its common name logs in as.
func (b *Broker) AddUser(t *testing.T, name string) {
	t.Helper()
	password := make([]byte, 16)
	_, _ = rand.Read(password)
	b.ctl(t, "add_user", name, hex.EncodeToString(password))
	b.ctl(t, "set_permissions", "-p", "/", name, ".*", ".*", ".*")
}

// Issue returns a client certificate from the broker's CA for commonName;
// unit, when not empty, is its organisational unit, which tells two
// certificates for one name apart in the broker's connection list.
func (b *Broker) Issue(t *testing.T, commonName, unit string) *tls.Certificate {
	t.Helper()
	return b.issue(t, commonName, b.ca, b.caKey, unit, x509.ExtKeyUsageClientAuth)
}

// IssueForeign returns a client certificate for commonName from a CA the
// broker does not trust.
func (b *Broker) IssueForeign(t *testing.T, commonName string) *tls.Certificate {
	t.Helper()
	ca, key := newCA(t, "an untrusted CA")
	return b.issue(t, commonName, ca, key, "", x509.ExtKeyUsageClientAuth)
}

// ClientConfig trusts the broker's CA and presents what certificate
// returns, asking again on every handshake.
func (b *Broker) ClientConfig(certificate func() *tls.Certificate) *tls.Config {
	return &tls.Config{
		RootCAs:    b.Pool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			if cert := certificate(); cert != nil {
				return cert, nil
			}
			return &tls.Certificate{}, nil // present none
		},
	}
}

// Connection is a client connection as the broker lists it.
type Connection struct {
	User        string `json:"user"`
	PeerSubject string `json:"peer_cert_subject"`
}

// Connections lists the broker's client connections.
func (b *Broker) Connections(t *testing.T) []Connection {
	t.Helper()
	out := b.ctl(t, "list_connections", "user", "peer_cert_subject", "--formatter", "json", "--quiet")
	var conns []Connection
	if err := json.Unmarshal(out, &conns); err != nil {
		t.Fatalf("parse list_connections: %v\n%s", err, out)
	}
	return conns
}

// CloseAll makes the broker close every client connection.
func (b *Broker) CloseAll(t *testing.T) {
	t.Helper()
	b.ctl(t, "close_all_connections", "svckit tls test")
}

func (b *Broker) ctl(t *testing.T, args ...string) []byte {
	t.Helper()
	return b.compose(t, append([]string{"--profile", "tls", "exec", "-T", "rabbitmq-tls-test", "rabbitmqctl"}, args...)...)
}

func (b *Broker) compose(t *testing.T, args ...string) []byte {
	t.Helper()
	out, err := b.composeOutput(args...)
	if err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}
	return out
}

func (b *Broker) composeOutput(args ...string) ([]byte, error) {
	cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
	cmd.Dir = b.root
	out, err := cmd.Output()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return append(out, exitErr.Stderr...), err
	}
	return out, err
}

func (b *Broker) issue(t *testing.T, commonName string, ca *x509.Certificate, caKey *ecdsa.PrivateKey, unit string, usage x509.ExtKeyUsage) *tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	subject := pkix.Name{CommonName: commonName}
	if unit != "" {
		subject.OrganizationalUnit = []string{unit}
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(b.serial.Add(1)),
		Subject:      subject,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
	if usage == x509.ExtKeyUsageServerAuth {
		tmpl.DNSNames = []string{"localhost"}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func newCA(t *testing.T, name string) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return cert, key
}

// repoRoot is the directory holding compose.yaml, above the test's working
// directory.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "compose.yaml")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("compose.yaml not found above the working directory")
		}
		dir = parent
	}
}
