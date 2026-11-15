package amqpcluster

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dobrevit/svckit/eventbus"
)

type failingHandler struct{ err error }

func (h failingHandler) Handle(context.Context, *eventbus.BaseEvent) error { return h.err }

// What the handler says decides, not the words in its error.
func TestHandlerMarksDecideRequeue(t *testing.T) {
	cs := subscriberOn(nil, "billing", nil, false)
	for name, tc := range map[string]struct {
		err     error
		requeue bool
	}{
		"Retryable that says invalid": {eventbus.Retryable(errors.New("invalid: the stock service did not answer")), true},
		"Permanent that says timeout": {eventbus.Permanent(errors.New("timeout field out of range")), false},
		"Retryable wrapped":           {fmt.Errorf("handle: %w", eventbus.Retryable(errors.New("busy"))), true},
	} {
		t.Run(name, func(t *testing.T) {
			d := unsignedDelivery(t, nil)
			cs.handleMessage("orders", d, failingHandler{tc.err}, SubscriptionOptions{})
			a := d.Acknowledger.(*acknowledger)
			if !a.nacked || a.requeue != tc.requeue {
				t.Errorf("nacked %v, requeue %v; want requeue %v", a.nacked, a.requeue, tc.requeue)
			}
		})
	}
}

// Today's word list, kept for unmarked errors until the fallback goes: it
// differs from eventbus's, and is case-sensitive.
func TestUnmarkedErrorsFollowTheOldWordList(t *testing.T) {
	cs := &ClusterSubscriber{}
	cases := map[string]bool{
		"connection refused":         true,
		"read timeout":               true,
		"read TIMEOUT":               false, // case-sensitive, as it always was
		"service temporary failure":  true,
		"context deadline exceeded":  true,
		"service unavailable":        true,
		"invalid order id":           false,
		"failed to unmarshal":        false,
		"something else went wrong":  false,
		"invalid connection setting": true, // retryable words are checked first
	}
	for text, want := range cases {
		if got := cs.isRetryableError(errors.New(text)); got != want {
			t.Errorf("isRetryableError(%q) = %v, want %v", text, got, want)
		}
	}
}
