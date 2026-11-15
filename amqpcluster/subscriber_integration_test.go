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
