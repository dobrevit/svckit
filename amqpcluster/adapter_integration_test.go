//go:build integration

// The tests in this file need the RabbitMQ from compose.yaml (`docker compose
// up -d --wait`). Point them at another broker with RABBITMQ_URL; the tests
// that make the broker drop connections run rabbitmqctl in the rabbitmq-test
// service through `docker compose exec`, so they need that service in
// particular (set COMPOSE_PROJECT_NAME if the stack runs under another name).

package amqpcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	amqp "github.com/rabbitmq/amqp091-go"
)

func testRabbitURL() string {
	if url := os.Getenv("RABBITMQ_URL"); url != "" {
		return url
	}
	return "amqp://test_user:test_password@localhost:5673/"
}

// testRabbitURL2 is a second broker, independent of the first (compose's
// rabbitmq-test-2), for tests that need one node down while another is up.
func testRabbitURL2() string {
	if url := os.Getenv("RABBITMQ_URL_2"); url != "" {
		return url
	}
	return "amqp://test_user:test_password@localhost:5674/"
}

// compose runs a docker compose command against the integration stack.
func compose(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}
}

// closeBrokerConnections makes the broker close every client connection, as a
// broker restart or a network partition would.
func closeBrokerConnections(t *testing.T) {
	t.Helper()
	compose(t, "exec", "-T", "rabbitmq-test", "rabbitmqctl", "close_all_connections", "svckit integration test")
}

// withBroker opens a connection of the test's own, for setting up and
// inspecting queues independently of the adapter under test.
func withBroker(t *testing.T, f func(ch *amqp.Channel)) {
	t.Helper()
	withBrokerAt(t, testRabbitURL(), f)
}

func withBrokerAt(t *testing.T, url string, f func(ch *amqp.Channel)) {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dial %s: %v", maskCredentials(url), err)
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

// A publish to an exchange that does not exist makes the broker close the
// channel (404 NOT_FOUND) but not the connection. The next publish fails and
// degrades the node; with one node, that used to end publishing until the
// process restarted.
func TestPublishSucceedsAfterTheBrokerClosesTheChannel(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	queue := fmt.Sprintf("svckit.it.recover.%d", time.Now().UnixNano())
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

	missing := fmt.Sprintf("svckit.it.missing.%d", time.Now().UnixNano())
	_ = ca.Publish(missing, "anything", false, false, amqp.Publishing{Body: []byte("lost")})

	// The channel closes asynchronously, so a publish just after the bad one
	// can be accepted by the client and discarded by the broker. Publish and
	// look for the message until one arrives.
	body := []byte("published after the channel closed")
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for {
		if lastErr = ca.Publish("", queue, false, false, amqp.Publishing{Body: body}); lastErr == nil {
			var got bool
			withBroker(t, func(ch *amqp.Channel) {
				msg, ok, err := ch.Get(queue, true)
				if err != nil {
					t.Fatalf("get from %s: %v", queue, err)
				}
				got = ok && string(msg.Body) == string(body)
			})
			if got {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no message reached %s within 15s of the channel closing; last publish error: %v", queue, lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}

	if stats := ca.GetNodeStats()["node-0"]; stats.State != NodeHealthy || stats.ErrorCount == 0 {
		t.Errorf("node state %v with %d errors; want healthy again, with the failure still counted", stats.State, stats.ErrorCount)
	}
}

// receiveWhilePublishing publishes body to queue on the broker at url, through
// the default exchange, until it arrives on out. A publish before the adapter
// has restored the queue on that broker is dropped, so one attempt is not
// enough.
func receiveWhilePublishing(t *testing.T, url, queue string, out <-chan amqp.Delivery, body string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		withBrokerAt(t, url, func(ch *amqp.Channel) {
			_ = ch.Publish("", queue, false, false, amqp.Publishing{Body: []byte(body)})
		})
		select {
		case msg, ok := <-out:
			if !ok {
				t.Fatal("the subscription closed")
			}
			if string(msg.Body) == body {
				return
			}
		case <-time.After(250 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("%q did not arrive within %v", body, within)
		}
	}
}

func TestSubscriptionResumesAfterTheBrokerDropsTheConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ca, err := NewClusterAdapter(ClusterConfig{
		Nodes:               []string{testRabbitURL()},
		ServiceName:         "amqpcluster-it",
		HealthCheckInterval: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClusterAdapter: %v", err)
	}
	defer ca.Close()

	queue := fmt.Sprintf("svckit.it.resume.%d", time.Now().UnixNano())
	if err := ca.DeclareQueue(queue, false, false, false, false, nil); err != nil {
		t.Fatalf("declare: %v", err)
	}
	t.Cleanup(func() {
		withBroker(t, func(ch *amqp.Channel) { _, _ = ch.QueueDelete(queue, false, false, false) })
	})

	out, err := ca.Subscribe(queue, "it", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	receiveWhilePublishing(t, testRabbitURL(), queue, out, "before", 5*time.Second)

	closeBrokerConnections(t)

	receiveWhilePublishing(t, testRabbitURL(), queue, out, "after the reconnect", 15*time.Second)
}

// Two independent brokers stand in for two nodes. The second is down when
// the subscription starts; once it is up, the adapter connects to it, declares
// the queue there and consumes from it.
func TestSubscriptionStartsOnANodeThatComesUpLater(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	compose(t, "stop", "rabbitmq-test-2")
	t.Cleanup(func() { compose(t, "up", "-d", "--wait", "rabbitmq-test-2") })

	ca, err := NewClusterAdapter(ClusterConfig{
		Nodes:               []string{testRabbitURL(), testRabbitURL2()},
		ServiceName:         "amqpcluster-it",
		HealthCheckInterval: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClusterAdapter: %v", err)
	}
	defer ca.Close()

	queue := fmt.Sprintf("svckit.it.latejoin.%d", time.Now().UnixNano())
	if err := ca.DeclareQueue(queue, false, false, false, false, nil); err != nil {
		t.Fatalf("declare: %v", err)
	}
	t.Cleanup(func() {
		withBroker(t, func(ch *amqp.Channel) { _, _ = ch.QueueDelete(queue, false, false, false) })
		withBrokerAt(t, testRabbitURL2(), func(ch *amqp.Channel) { _, _ = ch.QueueDelete(queue, false, false, false) })
	})

	out, err := ca.Subscribe(queue, "it", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Subscribe with one node down: %v", err)
	}

	compose(t, "up", "-d", "--wait", "rabbitmq-test-2")

	receiveWhilePublishing(t, testRabbitURL2(), queue, out, "from the second node", 30*time.Second)
}

// gatedHandler blocks every Handle until the gate is closed, and reports
// each call on started.
type gatedHandler struct {
	gate    chan struct{}
	started chan struct{}
}

func (h gatedHandler) Handle(ctx context.Context, _ *eventbus.BaseEvent) error {
	h.started <- struct{}{}
	<-h.gate
	return nil
}

// With PrefetchCount 1 and the handler holding the first message, the broker
// must keep the rest: one unacknowledged, four ready. Without a Qos call the
// broker sent all five at once.
func TestPrefetchLimitsUnacknowledgedMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	withBroker(t, func(ch *amqp.Channel) {
		if err := ch.ExchangeDeclare(eventbus.DefaultExchange, "topic", true, false, false, false, nil); err != nil {
			t.Fatalf("declare exchange: %v", err)
		}
	})

	sub, err := NewClusterSubscriber([]string{testRabbitURL()}, "amqpcluster-it", nil)
	if err != nil {
		t.Fatalf("NewClusterSubscriber: %v", err)
	}

	queue := fmt.Sprintf("svckit.it.prefetch.%d", time.Now().UnixNano())
	t.Cleanup(func() {
		withBroker(t, func(ch *amqp.Channel) {
			_, _ = ch.QueueDelete(queue, false, false, false)
			_, _ = ch.QueueDelete(queue+".dlq", false, false, false)
		})
	})

	handler := gatedHandler{gate: make(chan struct{}), started: make(chan struct{}, 10)}
	if err := sub.SubscribeWithOptions(queue, queue, handler, SubscriptionOptions{
		Durable:       false,
		PrefetchCount: 1,
	}); err != nil {
		t.Fatalf("SubscribeWithOptions: %v", err)
	}
	defer func() {
		select {
		case <-handler.gate:
		default:
			close(handler.gate)
		}
		_ = sub.Close()
	}()

	body, err := json.Marshal(eventbus.NewEvent("it.prefetch", "amqpcluster-it", nil))
	if err != nil {
		t.Fatal(err)
	}
	withBroker(t, func(ch *amqp.Channel) {
		for range 5 {
			if err := ch.Publish(eventbus.DefaultExchange, queue, false, false, amqp.Publishing{Body: body}); err != nil {
				t.Fatalf("publish: %v", err)
			}
		}
	})

	select {
	case <-handler.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler was never called")
	}

	// Give the broker time to push more, if it is going to.
	time.Sleep(time.Second)
	withBroker(t, func(ch *amqp.Channel) {
		q, err := ch.QueueDeclarePassive(queue, false, false, false, false, nil)
		if err != nil {
			t.Fatalf("inspect %s: %v", queue, err)
		}
		if q.Messages != 4 {
			t.Errorf("%d messages ready with one handler busy, want 4 (prefetch 1 leaves one unacknowledged)", q.Messages)
		}
	})

	close(handler.gate)
	for i := 1; i < 5; i++ {
		select {
		case <-handler.started:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of 5 messages were handled after the gate opened", i)
		}
	}
}
