//go:build integration

// The tests in this file need the RabbitMQ from compose.yaml (`docker compose
// up -d --wait`); point them at another broker with RABBITMQ_URL.

package eventbus_test

import (
	"context"
	"fmt"
	"os"
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

// withBroker opens a connection of the test's own, for setting up and
// inspecting queues independently of the publisher under test.
func withBroker(t *testing.T, f func(ch *amqp.Channel)) {
	t.Helper()
	conn, err := amqp.Dial(testRabbitURL())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	defer ch.Close()
	f(ch)
}

// boundQueue declares a queue bound to the default exchange for one event
// type, deleted when the test ends.
func boundQueue(t *testing.T, eventType string) string {
	t.Helper()
	queue := fmt.Sprintf("svckit.it.%s.%d", eventType, time.Now().UnixNano())
	withBroker(t, func(ch *amqp.Channel) {
		if err := ch.ExchangeDeclare(eventbus.DefaultExchange, "topic", true, false, false, false, nil); err != nil {
			t.Fatalf("declare exchange: %v", err)
		}
		if _, err := ch.QueueDeclare(queue, false, false, false, false, nil); err != nil {
			t.Fatalf("declare queue: %v", err)
		}
		if err := ch.QueueBind(queue, eventType, eventbus.DefaultExchange, false, nil); err != nil {
			t.Fatalf("bind queue: %v", err)
		}
	})
	t.Cleanup(func() {
		withBroker(t, func(ch *amqp.Channel) { _, _ = ch.QueueDelete(queue, false, false, false) })
	})
	return queue
}

// next waits for one message on queue.
func next(t *testing.T, queue string) amqp.Delivery {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var msg amqp.Delivery
		var ok bool
		withBroker(t, func(ch *amqp.Channel) {
			var err error
			msg, ok, err = ch.Get(queue, true)
			if err != nil {
				t.Fatalf("get from %s: %v", queue, err)
			}
		})
		if ok {
			return msg
		}
		if time.Now().After(deadline) {
			t.Fatalf("nothing arrived on %s", queue)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

var integrationSigning = &eventbus.SignatureConfig{
	SigningKey:     []byte("integration-test-signing-key"),
	ExpiryWindow:   30 * time.Minute,
	RequiredClaims: []string{"id", "type", "timestamp", "source"},
}

// A consumer that deduplicates reads the AMQP message id; eventbus.Publisher
// used to set only an event_id header.
func TestPublisherSetsTheMessageIDToTheEventID(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	signing, err := eventbus.NewPublisherWithSigning(testRabbitURL(), "eventbus-it", integrationSigning, false)
	if err != nil {
		t.Fatalf("NewPublisherWithSigning: %v", err)
	}
	defer signing.Close()
	plain, err := eventbus.NewPublisherWithService(testRabbitURL(), "eventbus-it")
	if err != nil {
		t.Fatalf("NewPublisherWithService: %v", err)
	}
	defer plain.Close()

	const id = "0b7f5d2c-1f4e-4c39-8f3e-2a6d9e1b4c55"
	for name, publish := range map[string]func(context.Context, *eventbus.BaseEvent) error{
		"unsigned": plain.Publish,
		"signed":   signing.Publish,
	} {
		t.Run(name, func(t *testing.T) {
			eventType := "it.messageid." + name
			queue := boundQueue(t, eventType)
			event := eventbus.NewEventWithID(id, eventType, "eventbus-it", nil)

			if err := publish(context.Background(), event); err != nil {
				t.Fatalf("publish: %v", err)
			}

			if got := next(t, queue).MessageId; got != id {
				t.Errorf("MessageId = %q, want the event id %q", got, id)
			}
		})
	}
}
