//go:build integration

package eventbus_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
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

// attemptRecorder fails the way it is told and records each call's attempt
// and time.
type attemptRecorder struct {
	mu    sync.Mutex
	calls []call
	fail  func(attempt int) error
}

type call struct {
	attempt int
	at      time.Time
}

func (h *attemptRecorder) Handle(ctx context.Context, _ *eventbus.BaseEvent) error {
	attempt := eventbus.Attempt(ctx)
	h.mu.Lock()
	h.calls = append(h.calls, call{attempt, time.Now()})
	h.mu.Unlock()
	return h.fail(attempt)
}

func (h *attemptRecorder) recorded() []call {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]call(nil), h.calls...)
}

// waitFor polls until cond holds or the time is up.
func waitFor(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %v", what, within)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestRetryPolicy(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	policy := &eventbus.RetryPolicy{Delays: []time.Duration{time.Second, 2 * time.Second}, MaxAttempts: 3}
	stamp := time.Now().UnixNano()
	name := func(kind string) string { return fmt.Sprintf("svckit.it.retry.%s.%d", kind, stamp) }
	exhausted, permanent, recovers, tap := name("exhausted"), name("permanent"), name("recovers"), name("tap")

	withBroker(t, func(ch *amqp.Channel) {
		if err := ch.ExchangeDeclare(eventbus.DefaultExchange, "topic", true, false, false, false, nil); err != nil {
			t.Fatalf("declare exchange: %v", err)
		}
		// An audit tap: everything on the topic exchange, and so any dead
		// letter or retry that passed through it.
		if _, err := ch.QueueDeclare(tap, false, false, false, false, nil); err != nil {
			t.Fatal(err)
		}
		if err := ch.QueueBind(tap, "#", eventbus.DefaultExchange, false, nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Cleanup(func() {
		withBroker(t, func(ch *amqp.Channel) {
			_, _ = ch.QueueDelete(tap, false, false, false)
			for _, q := range []string{exhausted, permanent, recovers} {
				for _, suffix := range []string{"", ".dlq", ".retry.1s", ".retry.2s"} {
					_, _ = ch.QueueDelete(q+suffix, false, false, false)
				}
			}
		})
	})

	sub, err := eventbus.NewSubscriberWithService(testRabbitURL(), "eventbus-it")
	if err != nil {
		t.Fatalf("NewSubscriberWithService: %v", err)
	}
	defer sub.Close()

	handlers := map[string]*attemptRecorder{
		exhausted: {fail: func(int) error { return eventbus.Retryable(errors.New("stock service down")) }},
		permanent: {fail: func(int) error { return eventbus.Permanent(errors.New("order has no lines")) }},
		recovers: {fail: func(attempt int) error {
			if attempt == 1 {
				return eventbus.Retryable(errors.New("stock service down"))
			}
			return nil
		}},
	}
	for queue, h := range handlers {
		if err := sub.SubscribeWithOptions(queue, queue, h, eventbus.SubscribeOptions{Retry: policy}); err != nil {
			t.Fatalf("subscribe %s: %v", queue, err)
		}
	}

	pub, err := eventbus.NewPublisherWithService(testRabbitURL(), "eventbus-it", eventbus.WithConfirms(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	for queue := range handlers {
		if err := pub.Publish(context.Background(), eventbus.NewEvent(queue, "eventbus-it", nil)); err != nil {
			t.Fatalf("publish to %s: %v", queue, err)
		}
	}

	t.Run("retried with delays, then dead-lettered", func(t *testing.T) {
		waitFor(t, 15*time.Second, "the exhausted message reaching its DLQ", func() bool { return messagesIn(t, exhausted+".dlq") == 1 })

		calls := handlers[exhausted].recorded()
		if len(calls) != 3 {
			t.Fatalf("the handler was called %d times, want 3", len(calls))
		}
		for i, c := range calls {
			if c.attempt != i+1 {
				t.Errorf("call %d saw attempt %d", i+1, c.attempt)
			}
		}
		if gap := calls[1].at.Sub(calls[0].at); gap < time.Second {
			t.Errorf("the first retry came %v after the first attempt, want at least 1s", gap)
		}
		if gap := calls[2].at.Sub(calls[1].at); gap < 2*time.Second {
			t.Errorf("the second retry came %v after the first, want at least 2s", gap)
		}

		withBroker(t, func(ch *amqp.Channel) {
			msg, ok, err := ch.Get(exhausted+".dlq", true)
			if err != nil || !ok {
				t.Fatalf("get the dead letter: %v %v", ok, err)
			}
			if got := msg.Headers["x-svckit-attempt"]; got != int32(3) {
				t.Errorf("x-svckit-attempt = %#v, want 3", got)
			}
			if got := msg.Headers["x-svckit-reason"]; got != "attempts_exhausted" {
				t.Errorf("x-svckit-reason = %#v", got)
			}
		})
	})

	t.Run("a permanent failure is dead-lettered after one call", func(t *testing.T) {
		waitFor(t, 5*time.Second, "the permanent failure reaching its DLQ", func() bool { return messagesIn(t, permanent+".dlq") == 1 })
		if n := len(handlers[permanent].recorded()); n != 1 {
			t.Errorf("the handler was called %d times, want 1", n)
		}
	})

	t.Run("a message that succeeds on a retry leaves nothing behind", func(t *testing.T) {
		waitFor(t, 10*time.Second, "the second attempt", func() bool { return len(handlers[recovers].recorded()) == 2 })
		time.Sleep(500 * time.Millisecond)
		for _, q := range []string{recovers + ".dlq", recovers + ".retry.1s", recovers + ".retry.2s"} {
			if n := messagesIn(t, q); n != 0 {
				t.Errorf("%d messages left in %s", n, q)
			}
		}
	})

	t.Run("no retry or dead letter passes through the topic exchange", func(t *testing.T) {
		withBroker(t, func(ch *amqp.Channel) {
			for {
				msg, ok, err := ch.Get(tap, true)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					return
				}
				if _, isRetry := msg.Headers["x-svckit-attempt"]; isRetry {
					t.Errorf("the # binding received a retry or dead letter: %v", msg.Headers)
				}
			}
		})
	})
}

// subscribeOne declares a fresh queue for its own routing key, subscribes h
// to it on a new subscriber, and publishes one event there.
func subscribeOne(t *testing.T, h eventbus.EventHandler, opts eventbus.SubscribeOptions, closeTimeout time.Duration) (*eventbus.Subscriber, string) {
	t.Helper()
	queue := fmt.Sprintf("svckit.it.close.%d", time.Now().UnixNano())
	withBroker(t, func(ch *amqp.Channel) {
		if err := ch.ExchangeDeclare(eventbus.DefaultExchange, "topic", true, false, false, false, nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Cleanup(func() {
		withBroker(t, func(ch *amqp.Channel) {
			_, _ = ch.QueueDelete(queue, false, false, false)
			_, _ = ch.QueueDelete(queue+".dlq", false, false, false)
		})
	})

	sub, err := eventbus.NewSubscriberWithService(testRabbitURL(), "eventbus-it", eventbus.WithCloseTimeout(closeTimeout))
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.SubscribeWithOptions(queue, queue, h, opts); err != nil {
		t.Fatal(err)
	}
	withBroker(t, func(ch *amqp.Channel) {
		body, _ := json.Marshal(eventbus.NewEvent(queue, "eventbus-it", nil))
		if err := ch.Publish(eventbus.DefaultExchange, queue, false, false, amqp.Publishing{Body: body}); err != nil {
			t.Fatal(err)
		}
	})
	return sub, queue
}

// Handlers ran on context.Background() with a fixed 30 s timeout, and Close
// neither cancelled them nor waited for them.
func TestCloseCancelsARunningHandlerAndItsMessageIsRedelivered(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	started := make(chan struct{}, 1)
	returned := make(chan error, 1)
	handler := handlerFunc(func(ctx context.Context, _ *eventbus.BaseEvent) error {
		started <- struct{}{}
		<-ctx.Done()
		returned <- ctx.Err()
		return ctx.Err()
	})
	sub, queue := subscribeOne(t, handler, eventbus.SubscribeOptions{}, 200*time.Millisecond)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never started")
	}
	start := time.Now()
	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond+2*time.Second {
		t.Errorf("Close took %v", elapsed)
	}
	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("the handler saw %v, want its context cancelled", err)
		}
	default:
		t.Fatal("the handler had not returned when Close did")
	}

	waitFor(t, 5*time.Second, "the unfinished message back in its queue", func() bool { return messagesIn(t, queue) == 1 })
	if n := messagesIn(t, queue+".dlq"); n != 0 {
		t.Errorf("the cancelled message was dead-lettered (%d in the DLQ)", n)
	}
}

func TestCloseIsBoundedWhenAHandlerIgnoresItsContext(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	handler := handlerFunc(func(context.Context, *eventbus.BaseEvent) error {
		started <- struct{}{}
		<-release
		return nil
	})
	sub, queue := subscribeOne(t, handler, eventbus.SubscribeOptions{}, 200*time.Millisecond)
	<-started

	start := time.Now()
	_ = sub.Close()
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond+1500*time.Millisecond {
		t.Errorf("Close took %v with a handler that ignores its context", elapsed)
	}
	// Closing the channel requeued the message nobody acknowledged.
	waitFor(t, 5*time.Second, "the abandoned message back in its queue", func() bool { return messagesIn(t, queue) == 1 })
}

func TestHandlerTimeoutBoundsAHandlerCall(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	got := make(chan error, 1)
	handler := handlerFunc(func(ctx context.Context, _ *eventbus.BaseEvent) error {
		<-ctx.Done()
		got <- ctx.Err()
		return eventbus.Permanent(ctx.Err())
	})
	sub, _ := subscribeOne(t, handler, eventbus.SubscribeOptions{HandlerTimeout: 100 * time.Millisecond}, time.Second)
	defer sub.Close()

	select {
	case err := <-got:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("the handler saw %v, want its deadline", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler was not cancelled by its timeout")
	}
}

// captureLogs sends slog's default logger, which the logging helpers write
// through, to a buffer at level, until the test ends.
func captureLogs(t *testing.T, level slog.Level) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: level})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return buf
}

// syncBuffer is a bytes.Buffer safe for the subscriber's goroutine to write
// while the test reads.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// The subscriber logged "Processing event: <type> for user: <user id>" at
// Info for every message.
func TestMessageLogs(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	const userID = "user-7f3a-personal"
	queue := fmt.Sprintf("svckit.it.logs.%d", time.Now().UnixNano())
	withBroker(t, func(ch *amqp.Channel) {
		if err := ch.ExchangeDeclare(eventbus.DefaultExchange, "topic", true, false, false, false, nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Cleanup(func() {
		withBroker(t, func(ch *amqp.Channel) {
			_, _ = ch.QueueDelete(queue, false, false, false)
			_, _ = ch.QueueDelete(queue+".dlq", false, false, false)
		})
	})

	handled := make(chan struct{}, 4)
	sub, err := eventbus.NewSubscriberWithSigning(testRabbitURL(), "eventbus-it", integrationSigning, false)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.Subscribe(queue, queue, handlerFunc(func(context.Context, *eventbus.BaseEvent) error {
		handled <- struct{}{}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	pub, err := eventbus.NewPublisherWithSigning(testRabbitURL(), "eventbus-it", integrationSigning, false)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	send := func() {
		t.Helper()
		event := eventbus.NewEvent(queue, "eventbus-it", nil)
		event.SetUserID(userID)
		if err := pub.Publish(context.Background(), event); err != nil {
			t.Fatal(err)
		}
		select {
		case <-handled:
		case <-time.After(5 * time.Second):
			t.Fatal("the message was not handled")
		}
		time.Sleep(100 * time.Millisecond) // the ack and its log line follow the handler
	}

	info := captureLogs(t, slog.LevelInfo)
	send()
	if logged := info.String(); logged != "" {
		t.Errorf("a published, verified and handled message logged at Info:\n%s", logged)
	}

	debug := captureLogs(t, slog.LevelDebug)
	send()
	logged := debug.String()
	if logged == "" {
		t.Fatal("nothing was logged at Debug; the test would prove nothing")
	}
	if strings.Contains(logged, userID) {
		t.Errorf("a log line names the user:\n%s", logged)
	}
}
