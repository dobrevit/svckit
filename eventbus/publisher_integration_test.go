//go:build integration

// The tests in this file need the RabbitMQ from compose.yaml (`docker compose
// up -d --wait`); point them at another broker with RABBITMQ_URL.

package eventbus_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
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

// restartBroker restarts compose's rabbitmq-test and waits until it accepts
// connections again.
func restartBroker(t *testing.T) {
	t.Helper()
	out, err := exec.Command("docker", "compose", "restart", "rabbitmq-test").CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose restart rabbitmq-test: %v\n%s", err, out)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		conn, err := amqp.Dial(testRabbitURL())
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the broker did not come back within 60s: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// durableQueue declares a durable queue bound to the default exchange for one
// event type, deleted when the test ends.
func durableQueue(t *testing.T, eventType string) string {
	t.Helper()
	queue := fmt.Sprintf("svckit.it.durable.%s.%d", eventType, time.Now().UnixNano())
	withBroker(t, func(ch *amqp.Channel) {
		if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
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

// Messages were transient, so a broker restart lost them even from a durable
// queue. Persistent is now the default; WithTransient keeps the old behaviour.
func TestPersistentMessagesSurviveABrokerRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	persistentPub, err := eventbus.NewPublisherWithService(testRabbitURL(), "eventbus-it")
	if err != nil {
		t.Fatalf("NewPublisherWithService: %v", err)
	}
	defer persistentPub.Close()
	transientPub, err := eventbus.NewPublisherWithService(testRabbitURL(), "eventbus-it", eventbus.WithTransient())
	if err != nil {
		t.Fatalf("NewPublisherWithService(WithTransient): %v", err)
	}
	defer transientPub.Close()

	persistentQueue := durableQueue(t, "it.persistent")
	transientQueue := durableQueue(t, "it.transient")

	if err := persistentPub.Publish(context.Background(), eventbus.NewEvent("it.persistent", "eventbus-it", nil)); err != nil {
		t.Fatalf("publish persistent: %v", err)
	}
	if err := transientPub.Publish(context.Background(), eventbus.NewEvent("it.transient", "eventbus-it", nil)); err != nil {
		t.Fatalf("publish transient: %v", err)
	}
	// Both must be in their queues before the restart, or the test proves nothing.
	withBroker(t, func(ch *amqp.Channel) {
		for _, q := range []string{persistentQueue, transientQueue} {
			deadline := time.Now().Add(5 * time.Second)
			for {
				state, err := ch.QueueDeclarePassive(q, true, false, false, false, nil)
				if err != nil {
					t.Fatalf("inspect %s: %v", q, err)
				}
				if state.Messages == 1 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("%s holds %d messages before the restart, want 1", q, state.Messages)
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	})

	restartBroker(t)

	withBroker(t, func(ch *amqp.Channel) {
		if _, ok, err := ch.Get(persistentQueue, true); err != nil || !ok {
			t.Errorf("the persistent message was lost in the restart (found %v, err %v)", ok, err)
		}
		if _, ok, err := ch.Get(transientQueue, true); err != nil || ok {
			t.Errorf("the transient message survived the restart (found %v, err %v)", ok, err)
		}
	})
}

// confirmingPublisher is a publisher with confirms on rabbitmq-test.
func confirmingPublisher(t *testing.T, timeout time.Duration) *eventbus.Publisher {
	t.Helper()
	pub, err := eventbus.NewPublisherWithService(testRabbitURL(), "eventbus-it", eventbus.WithConfirms(timeout))
	if err != nil {
		t.Fatalf("NewPublisherWithService(WithConfirms): %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	return pub
}

// rejectingQueue declares a queue bound for one event type that refuses
// every message: a length limit of zero with reject-publish overflow, so the
// broker nacks a publish routed to it.
func rejectingQueue(t *testing.T, eventType string) {
	t.Helper()
	queue := fmt.Sprintf("svckit.it.reject.%d", time.Now().UnixNano())
	withBroker(t, func(ch *amqp.Channel) {
		if _, err := ch.QueueDeclare(queue, false, false, false, false, amqp.Table{
			"x-max-length": int32(0),
			"x-overflow":   "reject-publish",
		}); err != nil {
			t.Fatalf("declare %s: %v", queue, err)
		}
		if err := ch.QueueBind(queue, eventType, eventbus.DefaultExchange, false, nil); err != nil {
			t.Fatalf("bind %s: %v", queue, err)
		}
	})
	t.Cleanup(func() {
		withBroker(t, func(ch *amqp.Channel) { _, _ = ch.QueueDelete(queue, false, false, false) })
	})
}

func uniqueType(prefix string) string {
	return fmt.Sprintf("%s.%d", prefix, time.Now().UnixNano())
}

func TestConfirmedPublishOutcomes(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	pub := confirmingPublisher(t, 5*time.Second)
	ctx := context.Background()

	t.Run("a bound queue confirms", func(t *testing.T) {
		eventType := uniqueType("it.confirm.bound")
		queue := boundQueue(t, eventType)
		if err := pub.Publish(ctx, eventbus.NewEvent(eventType, "eventbus-it", nil)); err != nil {
			t.Fatalf("Publish = %v, want nil after the broker's ack", err)
		}
		next(t, queue)
	})

	t.Run("no binding is unroutable", func(t *testing.T) {
		err := pub.Publish(ctx, eventbus.NewEvent(uniqueType("it.confirm.nobody"), "eventbus-it", nil))
		var returned *eventbus.ReturnedError
		if !errors.Is(err, eventbus.ErrUnroutable) || !errors.As(err, &returned) {
			t.Fatalf("Publish = %v, want ErrUnroutable", err)
		}
		if returned.ReplyCode != 312 {
			t.Errorf("reply code = %d, want 312 (NO_ROUTE)", returned.ReplyCode)
		}
	})

	t.Run("a queue that rejects publishes nacks", func(t *testing.T) {
		eventType := uniqueType("it.confirm.reject")
		rejectingQueue(t, eventType)
		if err := pub.Publish(ctx, eventbus.NewEvent(eventType, "eventbus-it", nil)); !errors.Is(err, eventbus.ErrNacked) {
			t.Fatalf("Publish = %v, want ErrNacked", err)
		}
	})
}

// A thousand publishes on one publisher at once, half routable and half not:
// each gets its own outcome.
func TestConcurrentConfirmedPublishes(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	pub := confirmingPublisher(t, 30*time.Second)
	routable := uniqueType("it.confirm.many")
	queue := boundQueue(t, routable)
	unroutable := uniqueType("it.confirm.many.nobody")

	const n = 1000
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			eventType := routable
			if i%2 == 1 {
				eventType = unroutable
			}
			errs[i] = pub.Publish(context.Background(), eventbus.NewEvent(eventType, "eventbus-it", nil))
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if i%2 == 0 && err != nil {
			t.Fatalf("routable publish %d: %v", i, err)
		}
		if i%2 == 1 && !errors.Is(err, eventbus.ErrUnroutable) {
			t.Fatalf("unroutable publish %d: %v, want ErrUnroutable", i, err)
		}
	}
	withBroker(t, func(ch *amqp.Channel) {
		state, err := ch.QueueDeclarePassive(queue, false, false, false, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		if state.Messages != n/2 {
			t.Errorf("%d messages in the bound queue, want %d", state.Messages, n/2)
		}
	})
}

// With the broker frozen nothing is confirmed: the publish returns after its
// timeout, or when its context ends, whichever is first.
func TestConfirmedPublishToAPausedBroker(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	const timeout = 2 * time.Second
	pub := confirmingPublisher(t, timeout)
	eventType := uniqueType("it.confirm.paused")
	boundQueue(t, eventType)

	pause := func() {
		if out, err := exec.Command("docker", "compose", "pause", "rabbitmq-test").CombinedOutput(); err != nil {
			t.Fatalf("docker compose pause: %v\n%s", err, out)
		}
	}
	unpause := func() { _ = exec.Command("docker", "compose", "unpause", "rabbitmq-test").Run() }
	pause()
	t.Cleanup(unpause)

	start := time.Now()
	err := pub.Publish(context.Background(), eventbus.NewEvent(eventType, "eventbus-it", nil))
	elapsed := time.Since(start)
	if !errors.Is(err, eventbus.ErrConfirmTimeout) {
		t.Fatalf("Publish = %v, want ErrConfirmTimeout", err)
	}
	if elapsed < timeout || elapsed > timeout+time.Second {
		t.Errorf("returned after %v, want the %v timeout plus at most a second", elapsed, timeout)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := pub.Publish(ctx, eventbus.NewEvent(eventType, "eventbus-it", nil)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish with a 300ms context = %v, want context.DeadlineExceeded", err)
	}

	unpause()
}
