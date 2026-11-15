package amqpcluster

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	amqp "github.com/rabbitmq/amqp091-go"
)

var testSigning = &eventbus.SignatureConfig{
	SigningKey:     []byte("decode-test-key-0123456789"),
	ExpiryWindow:   30 * time.Minute,
	RequiredClaims: []string{"id", "type", "timestamp", "source"},
}

// acknowledger records how a delivery was settled.
type acknowledger struct {
	mu       sync.Mutex
	acked    bool
	nacked   bool
	requeue  bool
	onSettle func(how string) // "ack" or "nack"
}

func (a *acknowledger) Ack(uint64, bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.acked = true
	if a.onSettle != nil {
		a.onSettle("ack")
	}
	return nil
}

func (a *acknowledger) Nack(_ uint64, _ bool, requeue bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nacked, a.requeue = true, requeue
	if a.onSettle != nil {
		a.onSettle("nack")
	}
	return nil
}

func (a *acknowledger) Reject(_ uint64, requeue bool) error { return a.Nack(0, false, requeue) }

func unsignedDelivery(t *testing.T, headers amqp.Table) amqp.Delivery {
	t.Helper()
	body, err := json.Marshal(eventbus.NewEvent("order.created", "orders", map[string]any{"id": "o-1"}))
	if err != nil {
		t.Fatal(err)
	}
	return amqp.Delivery{Body: body, Headers: headers, Acknowledger: &acknowledger{}}
}

func signedDelivery(t *testing.T, tamper bool) amqp.Delivery {
	t.Helper()
	signed, err := eventbus.SignEvent(eventbus.NewEvent("order.created", "orders", map[string]any{"id": "o-1"}), testSigning)
	if err != nil {
		t.Fatal(err)
	}
	if tamper {
		signed.Event.Source = "someone-else"
	}
	body, err := json.Marshal(signed)
	if err != nil {
		t.Fatal(err)
	}
	return amqp.Delivery{Body: body, Headers: amqp.Table{"signed": true}, Acknowledger: &acknowledger{}}
}

// The same table as eventbus's TestDecodeDelivery: the two subscribers must
// accept and reject the same messages.
func TestDecodeDelivery(t *testing.T) {
	verifier := eventbus.NewHMAC(testSigning)
	cases := []struct {
		name            string
		delivery        amqp.Delivery
		verifier        eventbus.Verifier
		signingRequired bool
		allowUnsigned   bool
		wantReason      string // "" means accepted
	}{
		{"verifier, no header", unsignedDelivery(t, nil), verifier, false, false, reasonUnsigned},
		{"verifier, signed false", unsignedDelivery(t, amqp.Table{"signed": false}), verifier, false, false, reasonUnsigned},
		{"verifier, bad signature", signedDelivery(t, true), verifier, false, false, reasonInvalidSignature},
		{"verifier, good signature", signedDelivery(t, false), verifier, false, false, ""},
		{"verifier, AllowUnsigned, no header", unsignedDelivery(t, nil), verifier, false, true, ""},
		{"verifier, AllowUnsigned, signed false", unsignedDelivery(t, amqp.Table{"signed": false}), verifier, false, true, ""},
		{"verifier, AllowUnsigned, bad signature", signedDelivery(t, true), verifier, false, true, reasonInvalidSignature},
		{"no verifier, signed true", signedDelivery(t, false), nil, false, false, reasonNoVerifier},
		{"signing required, no verifier, unsigned", unsignedDelivery(t, nil), nil, true, false, reasonUnsigned},
		{"no verifier, not required, unsigned", unsignedDelivery(t, nil), nil, false, false, ""},
		{"verifier, garbage", amqp.Delivery{Body: []byte("{"), Headers: amqp.Table{"signed": true}}, verifier, false, false, reasonParseError},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event, err := decodeDelivery(context.Background(), tc.delivery, tc.verifier, tc.signingRequired, tc.allowUnsigned)
			if tc.wantReason == "" {
				if err != nil {
					t.Fatalf("rejected: %v", err)
				}
				if event == nil || event.Type != "order.created" || event.ID == "" {
					t.Fatalf("accepted as %+v, want the published event", event)
				}
				return
			}
			var r *rejection
			if !errors.As(err, &r) {
				t.Fatalf("got event %+v, err %v; want a rejection for %s", event, err, tc.wantReason)
			}
			if r.reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", r.reason, tc.wantReason)
			}
		})
	}
}

type recordingHandler struct{ calls int }

func (h *recordingHandler) Handle(context.Context, *eventbus.BaseEvent) error {
	h.calls++
	return nil
}

// A subscriber built with a verifier used to hand a message with no "signed"
// header to its handler, so anyone who could publish to the exchange skipped
// verification by leaving the header out. app.New builds this subscriber.
func TestVerifyingSubscriberDeadLettersUnsignedMessages(t *testing.T) {
	cs := subscriberOn(nil, "billing", testSigning, false)

	for name, d := range map[string]amqp.Delivery{
		"no header":     unsignedDelivery(t, nil),
		"signed false":  unsignedDelivery(t, amqp.Table{"signed": false}),
		"bad signature": signedDelivery(t, true),
	} {
		t.Run(name, func(t *testing.T) {
			h := &recordingHandler{}
			cs.handleMessage("orders", d, h, SubscriptionOptions{})
			if h.calls != 0 {
				t.Error("the handler received a message that was not signed and verified")
			}
			a := d.Acknowledger.(*acknowledger)
			if !a.nacked || a.requeue {
				t.Errorf("nacked %v, requeue %v; want dead-lettered", a.nacked, a.requeue)
			}
		})
	}

	t.Run("good signature", func(t *testing.T) {
		d := signedDelivery(t, false)
		h := &recordingHandler{}
		cs.handleMessage("orders", d, h, SubscriptionOptions{})
		if h.calls != 1 || !d.Acknowledger.(*acknowledger).acked {
			t.Error("a correctly signed message was not handled and acked")
		}
	})
}

func TestAllowUnsignedLetsUnsignedMessagesThrough(t *testing.T) {
	cs := subscriberOn(nil, "billing", testSigning, false)
	d := unsignedDelivery(t, nil)
	h := &recordingHandler{}

	cs.handleMessage("orders", d, h, SubscriptionOptions{AllowUnsigned: true})

	if h.calls != 1 || !d.Acknowledger.(*acknowledger).acked {
		t.Error("with AllowUnsigned an unsigned message was not handled")
	}
}

// Config.SigningRequired is a deployment's policy; code must not be able to
// relax it per subscription.
func TestAllowUnsignedIsRefusedWhenSigningIsRequired(t *testing.T) {
	cs := &ClusterSubscriber{signingRequired: true}
	err := cs.SubscribeWithOptions("orders", "order.*", &recordingHandler{}, SubscriptionOptions{AllowUnsigned: true})
	if err == nil || !strings.Contains(err.Error(), "AllowUnsigned") {
		t.Errorf("err = %v, want AllowUnsigned refused", err)
	}
}

func TestNewClusterSubscriberWithConfigRequiresAKeyWhenSigningIsRequired(t *testing.T) {
	_, err := NewClusterSubscriberWithConfig(Config{
		Nodes:               []string{"amqp://guest:guest@localhost:1/"},
		ServiceName:         "billing",
		HealthCheckInterval: time.Second,
		RetryAttempts:       1,
		LoadBalanceStrategy: "round_robin",
		SigningRequired:     true,
	})
	if err == nil || !strings.Contains(err.Error(), "no signing key") {
		t.Errorf("err = %v, want a missing key refused before dialling", err)
	}
}
