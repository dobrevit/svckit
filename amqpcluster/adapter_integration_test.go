//go:build integration

// The tests in this file need the RabbitMQ from compose.yaml (`docker compose
// up -d --wait`). Point them at another broker with RABBITMQ_URL; the tests
// that make the broker drop connections run rabbitmqctl in the rabbitmq-test
// service through `docker compose exec`, so they need that service in
// particular (set COMPOSE_PROJECT_NAME if the stack runs under another name).

package amqpcluster

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func testRabbitURL() string {
	if url := os.Getenv("RABBITMQ_URL"); url != "" {
		return url
	}
	return "amqp://test_user:test_password@localhost:5673/"
}

// closeBrokerConnections makes the broker close every client connection, as a
// broker restart or a network partition would.
func closeBrokerConnections(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "exec", "-T", "rabbitmq-test",
		"rabbitmqctl", "close_all_connections", "svckit integration test")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("rabbitmqctl close_all_connections: %v\n%s", err, out)
	}
}

// withBroker opens a connection of the test's own, for setting up and
// inspecting queues independently of the adapter under test.
func withBroker(t *testing.T, f func(ch *amqp.Channel)) {
	t.Helper()
	conn, err := amqp.Dial(testRabbitURL())
	if err != nil {
		t.Fatalf("dial %s: %v", maskCredentials(testRabbitURL()), err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	defer ch.Close()
	f(ch)
}

func TestPublishSucceedsAfterTheBrokerDropsTheConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	queue := fmt.Sprintf("svckit.it.reconnect.%d", time.Now().UnixNano())
	withBroker(t, func(ch *amqp.Channel) {
		if _, err := ch.QueueDeclare(queue, false, false, false, false, nil); err != nil {
			t.Fatalf("declare %s: %v", queue, err)
		}
	})
	t.Cleanup(func() {
		withBroker(t, func(ch *amqp.Channel) { _, _ = ch.QueueDelete(queue, false, false, false) })
	})

	ca, err := NewClusterAdapter(ClusterConfig{
		Nodes:               []string{testRabbitURL()},
		ServiceName:         "amqpcluster-it",
		HealthCheckInterval: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClusterAdapter: %v", err)
	}
	defer ca.Close()

	closeBrokerConnections(t)

	// The health check reconnects the node; until it has, publishing fails.
	// Before the fix the health check deadlocked here, and every publish after
	// it blocked.
	body := []byte("published after the reconnect")
	deadline := time.Now().Add(30 * time.Second)
	for {
		err = ca.Publish("", queue, false, false, amqp.Publishing{Body: body})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no publish succeeded within 30s of the broker closing the connection: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	withBroker(t, func(ch *amqp.Channel) {
		deadline := time.Now().Add(5 * time.Second)
		for {
			msg, ok, err := ch.Get(queue, true)
			if err != nil {
				t.Fatalf("get from %s: %v", queue, err)
			}
			if ok {
				if string(msg.Body) != string(body) {
					t.Errorf("got %q, want %q", msg.Body, body)
				}
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("the publish reported success but the message never reached the queue")
			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	if stats := ca.GetNodeStats()["node-0"]; stats.State != NodeHealthy {
		t.Errorf("node state after the reconnect = %v, want NodeHealthy", stats.State)
	}
}
