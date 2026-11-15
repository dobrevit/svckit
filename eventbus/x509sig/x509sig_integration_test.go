//go:build integration

// Needs the RabbitMQ from compose.yaml; RABBITMQ_URL points elsewhere.

package x509sig

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	amqp "github.com/rabbitmq/amqp091-go"
)

func rabbitURL() string {
	if url := os.Getenv("RABBITMQ_URL"); url != "" {
		return url
	}
	return "amqp://test_user:test_password@localhost:5673/"
}

type received chan *eventbus.BaseEvent

func (r received) Handle(_ context.Context, e *eventbus.BaseEvent) error {
	r <- e
	return nil
}

// One service's certificate signs a type it may publish and the subscriber
// takes it; another service's certificate, for a type outside its list, is
// dead-lettered.
func TestCertificateSignedMessagesThroughTheBroker(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ca := newCA(t)
	verifier := verifierFor(t, ca)
	queue := fmt.Sprintf("svckit.it.x509.%d", time.Now().UnixNano())

	conn, err := amqp.Dial(rabbitURL())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	defer func() {
		_, _ = ch.QueueDelete(queue, false, false, false)
		_, _ = ch.QueueDelete(queue+".dlq", false, false, false)
	}()
	if err := ch.ExchangeDeclare(eventbus.DefaultExchange, "topic", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}

	got := make(received, 2)
	sub, err := eventbus.NewSubscriberWithService(rabbitURL(), "billing", eventbus.WithVerifier(verifier))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.Subscribe(queue, "order.created", got); err != nil {
		t.Fatal(err)
	}

	publish := func(service string) string {
		pub, err := eventbus.NewPublisherWithService(rabbitURL(), service,
			eventbus.WithSigner(NewSigner(source(ca.valid(t, service)), 0)), eventbus.WithConfirms(5*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		defer pub.Close()
		event := eventbus.NewEvent("order.created", service, nil)
		if err := pub.Publish(context.Background(), event); err != nil {
			t.Fatalf("publish as %s: %v", service, err)
		}
		return event.ID
	}

	ordersID := publish("orders")
	select {
	case e := <-got:
		if e.ID != ordersID {
			t.Errorf("received %s, want the orders service's %s", e.ID, ordersID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the message signed by orders was not handled")
	}

	billingID := publish("billing")
	deadline := time.Now().Add(5 * time.Second)
	for {
		msg, ok, err := ch.Get(queue+".dlq", true)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if msg.MessageId != billingID {
				t.Errorf("dead letter %s, want billing's %s", msg.MessageId, billingID)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("billing's order.created was not dead-lettered")
		}
		time.Sleep(50 * time.Millisecond)
	}
	select {
	case e := <-got:
		t.Errorf("the handler received %s, signed by a service that may not publish it", e.ID)
	default:
	}
}
