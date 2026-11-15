package outbox

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPublishingIsTheStoredMessage(t *testing.T) {
	now := time.Now()
	msg := Message{ID: "m-1", ContentType: "application/json", Body: []byte(`{"a":1}`),
		Headers: map[string]any{"signed": true, "nested": map[string]any{"k": "v"}}}

	p := publishing(msg, now)
	if p.MessageId != "m-1" || p.DeliveryMode != amqp.Persistent || string(p.Body) != `{"a":1}` || p.Expiration != "" {
		t.Errorf("publishing = %+v", p)
	}
	if _, ok := p.Headers["nested"].(amqp.Table); !ok {
		t.Error("a nested header was not converted to an amqp.Table")
	}

	msg.ExpiresAt = now.Add(2 * time.Second)
	ms, _ := strconv.Atoi(publishing(msg, now).Expiration)
	if ms <= 0 || ms > 2000 {
		t.Errorf("expiration with 2s left = %d ms", ms)
	}
	msg.ExpiresAt = now.Add(time.Microsecond)
	if got := publishing(msg, now).Expiration; got != "1" {
		t.Errorf("expiration with under a millisecond left = %q, want 1", got)
	}
}

func TestBackoffDoublesUpToTheMaximum(t *testing.T) {
	r, _ := NewRelay(nil, nil, RelayConfig{BackoffBase: time.Second, BackoffMax: 10 * time.Second})
	for attempt, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 8 * time.Second, 5: 10 * time.Second, 30: 10 * time.Second} {
		if got := r.backoff(attempt); got != want {
			t.Errorf("backoff(%d) = %v, want %v", attempt, got, want)
		}
	}
}

func TestFailureLabels(t *testing.T) {
	for want, err := range map[string]error{
		"returned": &eventbus.ReturnedError{ReplyCode: 312},
		"nacked":   fmt.Errorf("publish: %w", eventbus.ErrNacked),
		"timeout":  eventbus.ErrConfirmTimeout,
		"failed":   errors.New("channel closed"),
	} {
		if got := failureLabel(err); got != want {
			t.Errorf("failureLabel(%v) = %q, want %q", err, got, want)
		}
	}
}
