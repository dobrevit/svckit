//go:build integration

package amqpcluster

import (
	"crypto/tls"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dobrevit/svckit/internal/testtls"
	amqp "github.com/rabbitmq/amqp091-go"
)

// A certificate source that renews returns the new certificate on the next
// handshake, so the adapter's reconnect presents it.
func TestAReconnectPresentsTheRenewedCertificate(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	broker := testtls.Start(t)
	broker.AddUser(t, "orders")

	var current atomic.Pointer[tls.Certificate]
	current.Store(broker.Issue(t, "orders", "gen1"))

	ca, err := NewClusterAdapter(ClusterConfig{
		Nodes:               []string{broker.URL},
		ServiceName:         "amqpcluster-it",
		HealthCheckInterval: 250 * time.Millisecond,
		TLS:                 broker.ClientConfig(current.Load),
		ExternalAuth:        true,
	})
	if err != nil {
		t.Fatalf("NewClusterAdapter over TLS with EXTERNAL: %v", err)
	}
	defer ca.Close()

	presenting := func(unit string) bool {
		for _, c := range broker.Connections(t) {
			if c.User == "orders" && strings.Contains(c.PeerSubject, "OU="+unit) {
				return true
			}
		}
		return false
	}
	if !presenting("gen1") {
		t.Fatalf("the first connection does not present gen1: %+v", broker.Connections(t))
	}

	current.Store(broker.Issue(t, "orders", "gen2"))
	broker.CloseAll(t)

	deadline := time.Now().Add(20 * time.Second)
	for {
		if err := ca.Publish("", "nowhere", false, false, amqp.Publishing{Body: []byte("x")}); err == nil && presenting("gen2") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no reconnect with the renewed certificate within 20s: %+v", broker.Connections(t))
		}
		time.Sleep(200 * time.Millisecond)
	}
	if presenting("gen1") {
		t.Error("a connection with the old certificate is still open")
	}
}
