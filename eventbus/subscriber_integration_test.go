//go:build integration

package eventbus_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	amqp "github.com/rabbitmq/amqp091-go"
)

// queueArguments reads a queue's declared arguments from the broker.
func queueArguments(t *testing.T, queue string) map[string]any {
	t.Helper()
	out, err := exec.Command("docker", "compose", "exec", "-T", "rabbitmq-test",
		"rabbitmqctl", "list_queues", "name", "arguments", "--formatter", "json", "--quiet").Output()
	if err != nil {
		t.Fatalf("rabbitmqctl list_queues: %v", err)
	}
	var queues []struct {
		Name      string  `json:"name"`
		Arguments [][]any `json:"arguments"` // [key, type, value]
	}
	if err := json.Unmarshal(out, &queues); err != nil {
		t.Fatalf("parse rabbitmqctl output: %v\n%s", err, out)
	}
	for _, q := range queues {
		if q.Name == queue {
			args := make(map[string]any, len(q.Arguments))
			for _, a := range q.Arguments {
				args[a[0].(string)] = a[2]
			}
			return args
		}
	}
	t.Fatalf("queue %s not found", queue)
	return nil
}

func messagesIn(t *testing.T, queue string) int {
	t.Helper()
	var n int
	withBroker(t, func(ch *amqp.Channel) {
		state, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
		if err != nil {
			t.Fatalf("inspect %s: %v", queue, err)
		}
		n = state.Messages
	})
	return n
}

type nopHandler struct{}

func (nopHandler) Handle(context.Context, *eventbus.BaseEvent) error { return nil }

// A queue's TTL is chosen per subscription. The default stays five minutes,
// because a queue that exists already cannot be redeclared with other
// arguments.
func TestQueueTTL(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	stamp := time.Now().UnixNano()
	name := func(kind string) string { return fmt.Sprintf("svckit.it.ttl.%s.%d", kind, stamp) }
	none, short, deflt, existing := name("none"), name("short"), name("default"), name("existing")

	withBroker(t, func(ch *amqp.Channel) {
		if err := ch.ExchangeDeclare(eventbus.DefaultExchange, "topic", true, false, false, false, nil); err != nil {
			t.Fatalf("declare exchange: %v", err)
		}
		// A queue as every subscriber before this change declared it.
		if _, err := ch.QueueDeclare(existing, true, false, false, false, amqp.Table{
			"x-dead-letter-exchange":    eventbus.DefaultExchange,
			"x-dead-letter-routing-key": existing + ".dlq",
			"x-message-ttl":             300000,
		}); err != nil {
			t.Fatalf("declare %s: %v", existing, err)
		}
	})
	t.Cleanup(func() {
		withBroker(t, func(ch *amqp.Channel) {
			for _, q := range []string{none, short, deflt, existing} {
				_, _ = ch.QueueDelete(q, false, false, false)
				_, _ = ch.QueueDelete(q+".dlq", false, false, false)
			}
		})
	})

	sub, err := eventbus.NewSubscriberWithService(testRabbitURL(), "eventbus-it")
	if err != nil {
		t.Fatalf("NewSubscriberWithService: %v", err)
	}
	for queue, ttl := range map[string]time.Duration{none: eventbus.NoQueueTTL, short: 2 * time.Second, deflt: 0} {
		if err := sub.SubscribeWithOptions(queue, queue, nopHandler{}, eventbus.SubscribeOptions{QueueTTL: ttl}); err != nil {
			t.Fatalf("subscribe %s: %v", queue, err)
		}
	}
	if err := sub.Subscribe(existing, existing, nopHandler{}); err != nil {
		t.Fatalf("redeclaring an existing five-minute queue with the default: %v", err)
	}
	// Stop consuming, so published messages wait in their queues.
	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, ok := queueArguments(t, none)["x-message-ttl"]; ok {
		t.Errorf("%s was declared with an x-message-ttl", none)
	}
	if got := queueArguments(t, deflt)["x-message-ttl"]; got != float64(300000) {
		t.Errorf("the default queue's x-message-ttl = %v, want 300000", got)
	}

	withBroker(t, func(ch *amqp.Channel) {
		for _, q := range []string{none, short} {
			if err := ch.Publish(eventbus.DefaultExchange, q, false, false, amqp.Publishing{Body: []byte("{}")}); err != nil {
				t.Fatalf("publish to %s: %v", q, err)
			}
		}
	})

	time.Sleep(3 * time.Second)
	if n := messagesIn(t, short); n != 0 {
		t.Errorf("%d messages still in the 2-second queue after 3 seconds", n)
	}
	if n := messagesIn(t, short+".dlq"); n != 1 {
		t.Errorf("%d messages dead-lettered from the 2-second queue, want 1", n)
	}
	if n := messagesIn(t, none); n != 1 {
		t.Errorf("%d messages in the queue without a TTL after 3 seconds, want 1", n)
	}
}
