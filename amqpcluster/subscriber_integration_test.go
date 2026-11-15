//go:build integration

package amqpcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	amqp "github.com/rabbitmq/amqp091-go"
)

type countingHandler struct{ calls atomic.Int32 }

func (h *countingHandler) Handle(context.Context, *eventbus.BaseEvent) error {
	h.calls.Add(1)
	return nil
}

// An unsigned message published to a subscriber that verifies signatures is
// dead-lettered, not handled.
func TestVerifyingSubscriberDeadLettersAnUnsignedMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	withBroker(t, func(ch *amqp.Channel) {
		if err := ch.ExchangeDeclare(eventbus.DefaultExchange, "topic", true, false, false, false, nil); err != nil {
			t.Fatalf("declare exchange: %v", err)
		}
	})

	sub, err := NewClusterSubscriber([]string{testRabbitURL()}, "amqpcluster-it", testSigning)
	if err != nil {
		t.Fatalf("NewClusterSubscriber: %v", err)
	}
	defer sub.Close()

	queue := fmt.Sprintf("svckit.it.unsigned.%d", time.Now().UnixNano())
	t.Cleanup(func() {
		withBroker(t, func(ch *amqp.Channel) {
			_, _ = ch.QueueDelete(queue, false, false, false)
			_, _ = ch.QueueDelete(queue+".dlq", false, false, false)
		})
	})

	handler := &countingHandler{}
	if err := sub.SubscribeWithOptions(queue, queue, handler, SubscriptionOptions{PrefetchCount: 1}); err != nil {
		t.Fatalf("SubscribeWithOptions: %v", err)
	}

	event := eventbus.NewEvent("it.unsigned", "intruder", nil)
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	withBroker(t, func(ch *amqp.Channel) {
		if err := ch.Publish(eventbus.DefaultExchange, queue, false, false, amqp.Publishing{Body: body}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	})

	deadline := time.Now().Add(10 * time.Second)
	for {
		var found bool
		withBroker(t, func(ch *amqp.Channel) {
			msg, ok, err := ch.Get(queue+".dlq", true)
			if err != nil {
				t.Fatalf("get from %s.dlq: %v", queue, err)
			}
			found = ok && string(msg.Body) == string(body)
		})
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the unsigned message did not reach %s.dlq within 10s", queue)
		}
		time.Sleep(100 * time.Millisecond)
	}

	if n := handler.calls.Load(); n != 0 {
		t.Errorf("the handler was called %d times for an unsigned message", n)
	}
}

// The cluster publisher sets the AMQP message id to the event id, signed or
// not, so a consumer can deduplicate on it.
func TestClusterPublisherSetsTheMessageIDToTheEventID(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	const id = "6c1e9a3b-55d2-4a7f-b0e8-91c4d2f7a316"
	for name, signing := range map[string]*eventbus.SignatureConfig{"unsigned": nil, "signed": testSigning} {
		t.Run(name, func(t *testing.T) {
			pub, err := NewClusterPublisher([]string{testRabbitURL()}, "amqpcluster-it", signing)
			if err != nil {
				t.Fatalf("NewClusterPublisher: %v", err)
			}
			defer pub.Close()

			eventType := "it.messageid." + name
			queue := fmt.Sprintf("svckit.it.cluster-messageid.%s.%d", name, time.Now().UnixNano())
			withBroker(t, func(ch *amqp.Channel) {
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

			if err := pub.Publish(context.Background(), eventbus.NewEventWithID(id, eventType, "amqpcluster-it", nil)); err != nil {
				t.Fatalf("Publish: %v", err)
			}

			deadline := time.Now().Add(5 * time.Second)
			for {
				var msg amqp.Delivery
				var ok bool
				withBroker(t, func(ch *amqp.Channel) {
					msg, ok, err = ch.Get(queue, true)
					if err != nil {
						t.Fatalf("get: %v", err)
					}
				})
				if ok {
					if msg.MessageId != id {
						t.Errorf("MessageId = %q, want the event id %q", msg.MessageId, id)
					}
					return
				}
				if time.Now().After(deadline) {
					t.Fatal("the published message never arrived")
				}
				time.Sleep(50 * time.Millisecond)
			}
		})
	}
}
