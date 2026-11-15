package retry

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var policy = Policy{Delays: []time.Duration{time.Second, 2 * time.Second}, MaxAttempts: 3}

func TestValidate(t *testing.T) {
	if err := policy.Validate(); err != nil {
		t.Fatalf("a usable policy was refused: %v", err)
	}
	for name, p := range map[string]Policy{
		"no attempts":           {Delays: []time.Duration{time.Second}},
		"retries without delay": {MaxAttempts: 2},
		"sub-millisecond delay": {Delays: []time.Duration{time.Microsecond}, MaxAttempts: 2},
	} {
		if p.Validate() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := (Policy{MaxAttempts: 1}).Validate(); err != nil {
		t.Errorf("a policy that never retries needs no delays: %v", err)
	}
}

// The retry after attempt n waits Delays[n-1], and the last delay repeats.
func TestDelayAfter(t *testing.T) {
	for attempt, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 2 * time.Second, 9: 2 * time.Second} {
		if got := policy.DelayAfter(attempt); got != want {
			t.Errorf("DelayAfter(%d) = %v, want %v", attempt, got, want)
		}
	}
}

func delivery(attempt any) amqp.Delivery {
	headers := amqp.Table{"signed": true}
	if attempt != nil {
		headers[AttemptHeader] = attempt
	}
	return amqp.Delivery{Headers: headers, Body: []byte(`{"id":"e-1"}`), MessageId: "e-1", ContentType: "application/json"}
}

func TestARetryableFailureGoesToTheRetryQueueForItsAttempt(t *testing.T) {
	key, msg := policy.Next(delivery(nil), "orders", true)
	if key != "orders.retry.1s" {
		t.Errorf("first retry routed to %q, want orders.retry.1s", key)
	}
	if msg.Headers[AttemptHeader] != int32(2) {
		t.Errorf("attempt header = %#v, want 2", msg.Headers[AttemptHeader])
	}

	key, _ = policy.Next(delivery(int32(2)), "orders", true)
	if key != "orders.retry.2s" {
		t.Errorf("second retry routed to %q, want orders.retry.2s", key)
	}
}

func TestTheLastAttemptIsDeadLettered(t *testing.T) {
	key, msg := policy.Next(delivery(int64(3)), "orders", true)
	if key != "orders.dlq" {
		t.Fatalf("attempt 3 of 3 routed to %q, want orders.dlq", key)
	}
	if msg.Headers[AttemptHeader] != int32(3) || msg.Headers[ReasonHeader] != ReasonAttemptsExhausted {
		t.Errorf("dead letter headers = %v", msg.Headers)
	}
}

func TestAPermanentFailureIsDeadLetteredAtOnce(t *testing.T) {
	key, msg := policy.Next(delivery(nil), "orders", false)
	if key != "orders.dlq" || msg.Headers[ReasonHeader] != ReasonPermanent || msg.Headers[AttemptHeader] != int32(1) {
		t.Errorf("routed to %q with %v, want the DLQ, permanent, attempt 1", key, msg.Headers)
	}
}

// A retried message is the message: its body, id and headers (a signature
// among them) go with it, and it stays persistent.
func TestTheRepublishKeepsTheMessage(t *testing.T) {
	d := delivery(nil)
	_, msg := policy.Next(d, "orders", true)
	if string(msg.Body) != string(d.Body) || msg.MessageId != "e-1" || msg.ContentType != "application/json" {
		t.Errorf("republished %+v, want the original body, id and content type", msg)
	}
	if msg.Headers["signed"] != true {
		t.Error("the original headers were dropped")
	}
	if msg.DeliveryMode != amqp.Persistent {
		t.Error("the republish is not persistent")
	}
	if _, touched := d.Headers[AttemptHeader]; touched {
		t.Error("Next changed the delivery's own headers")
	}
}

func TestRetryQueues(t *testing.T) {
	queues := RetryQueues("orders", Policy{Delays: []time.Duration{time.Second, time.Second, 500 * time.Millisecond}, MaxAttempts: 4})
	if len(queues) != 2 {
		t.Fatalf("%d retry queues, want one per distinct delay: %v", len(queues), queues)
	}
	args := queues["orders.retry.500ms"]
	if args["x-message-ttl"] != int64(500) || args["x-dead-letter-exchange"] != "" || args["x-dead-letter-routing-key"] != "orders" {
		t.Errorf("orders.retry.500ms arguments = %v", args)
	}
}

func TestMainQueueArgsDeadLetterIntoTheDLQByName(t *testing.T) {
	args := MainQueueArgs("orders", amqp.Table{"x-dead-letter-exchange": "events", "x-message-ttl": int64(1000)})
	if args["x-dead-letter-exchange"] != "" || args["x-dead-letter-routing-key"] != "orders.dlq" || args["x-message-ttl"] != int64(1000) {
		t.Errorf("main queue arguments = %v", args)
	}
}
