package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var testSigning = &SignatureConfig{
	SigningKey:     []byte("decode-test-key-0123456789"),
	ExpiryWindow:   30 * time.Minute,
	RequiredClaims: []string{"id", "type", "timestamp", "source"},
}

func unsignedDelivery(t *testing.T, headers amqp.Table) amqp.Delivery {
	t.Helper()
	body, err := json.Marshal(NewEvent("order.created", "orders", map[string]any{"id": "o-1"}))
	if err != nil {
		t.Fatal(err)
	}
	return amqp.Delivery{Body: body, Headers: headers}
}

func signedDelivery(t *testing.T, config *SignatureConfig, tamper bool) amqp.Delivery {
	t.Helper()
	signed, err := SignEvent(NewEvent("order.created", "orders", map[string]any{"id": "o-1"}), config)
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
	return amqp.Delivery{Body: body, Headers: amqp.Table{"signed": true}}
}

// The "signed" header is set by whoever publishes, so it can only ever make a
// message harder to accept. These cases are the parse path of
// Subscriber.Subscribe; amqpcluster's decode_test.go runs the same table.
func TestDecodeDelivery(t *testing.T) {
	verifier := NewHMAC(testSigning)
	cases := []struct {
		name            string
		delivery        amqp.Delivery
		verifier        Verifier
		signingRequired bool
		allowUnsigned   bool
		wantReason      string // "" means accepted
	}{
		{"verifier, no header", unsignedDelivery(t, nil), verifier, false, false, reasonUnsigned},
		{"verifier, signed false", unsignedDelivery(t, amqp.Table{"signed": false}), verifier, false, false, reasonUnsigned},
		{"verifier, bad signature", signedDelivery(t, testSigning, true), verifier, false, false, reasonInvalidSignature},
		{"verifier, good signature", signedDelivery(t, testSigning, false), verifier, false, false, ""},
		{"verifier, AllowUnsigned, no header", unsignedDelivery(t, nil), verifier, false, true, ""},
		{"verifier, AllowUnsigned, signed false", unsignedDelivery(t, amqp.Table{"signed": false}), verifier, false, true, ""},
		{"verifier, AllowUnsigned, bad signature", signedDelivery(t, testSigning, true), verifier, false, true, reasonInvalidSignature},
		{"no verifier, signed true", signedDelivery(t, testSigning, false), nil, false, false, reasonNoVerifier},
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

// A subscriber that requires signing must not be relaxed per subscription.
func TestAllowUnsignedIsRefusedWhenSigningIsRequired(t *testing.T) {
	s := &Subscriber{requireSigning: true}
	err := s.SubscribeWithOptions("orders", "order.*", nil, SubscribeOptions{AllowUnsigned: true})
	if err == nil || !strings.Contains(err.Error(), "AllowUnsigned") {
		t.Errorf("err = %v, want AllowUnsigned refused", err)
	}
}
