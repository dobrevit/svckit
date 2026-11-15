//go:build integration

package eventbus_test

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	"github.com/dobrevit/svckit/internal/testtls"
)

// Every connection used to be amqp.Dial(url): an amqps:// URL got the
// default TLS config, with no client certificate and no EXTERNAL
// authentication, so certificate-based broker access was impossible.
func TestTLSWithCertificateAuthentication(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	broker := testtls.Start(t)
	broker.AddUser(t, "orders")

	connect := func(cert *tls.Certificate) (*eventbus.Publisher, error) {
		return eventbus.NewPublisherWithService(broker.URL, "orders",
			eventbus.WithTLS(broker.ClientConfig(func() *tls.Certificate { return cert }), true),
			eventbus.WithConfirms(5*time.Second))
	}

	t.Run("a certificate from the CA connects as the user it names", func(t *testing.T) {
		pub, err := connect(broker.Issue(t, "orders", ""))
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer pub.Close()
		// Confirmed, so the broker took it, through the exchange the
		// publisher declared over this connection.
		err = pub.Publish(context.Background(), eventbus.NewEvent("it.tls", "orders", nil))
		if err != nil && !isUnroutable(err) {
			t.Fatalf("publish: %v", err)
		}
		conns := broker.Connections(t)
		if len(conns) != 1 || conns[0].User != "orders" {
			t.Errorf("broker connections = %+v, want one, logged in as orders from its certificate", conns)
		}
	})

	t.Run("no certificate is refused", func(t *testing.T) {
		if pub, err := connect(nil); err == nil {
			_ = pub.Close()
			t.Fatal("connected without a client certificate")
		}
	})

	t.Run("another CA's certificate is refused", func(t *testing.T) {
		if pub, err := connect(broker.IssueForeign(t, "orders")); err == nil {
			_ = pub.Close()
			t.Fatal("connected with a certificate from an untrusted CA")
		}
	})

	t.Run("a certificate for no user is refused", func(t *testing.T) {
		if pub, err := connect(broker.Issue(t, "nobody", "")); err == nil {
			_ = pub.Close()
			t.Fatal("connected as a user the broker does not have")
		}
	})
}

// isUnroutable is true for a publish the broker returned: it was accepted
// over the connection, just not routed, as nothing is bound here.
func isUnroutable(err error) bool { return errors.Is(err, eventbus.ErrUnroutable) }
