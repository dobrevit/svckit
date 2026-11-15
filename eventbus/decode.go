package eventbus

import (
	"encoding/json"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dobrevit/svckit/logging"
)

// The reasons a delivery is dead-lettered before it reaches a handler. They
// are the reason label of RecordEventDLQ.
const (
	reasonParseError       = "parse_error"
	reasonInvalidSignature = "invalid_signature"
	reasonUnsigned         = "unsigned_event_rejected"
	reasonNoVerifier       = "no_verifier"
)

// rejection is a delivery refused before its handler: it is dead-lettered,
// never requeued, because redelivering it cannot change the outcome.
type rejection struct {
	reason    string // the RecordEventDLQ label
	eventType string // "unknown" when the event could not be read
	err       error
}

func (r *rejection) Error() string { return r.err.Error() }
func (r *rejection) Unwrap() error { return r.err }

// decodeDelivery turns a delivery into the event its handler receives, or a
// *rejection.
//
// Whether a message is signed is said by its "signed" header, which the
// sender controls, so the header can only ever make a message stricter to
// accept: a subscriber with a verifier, or one that requires signing, rejects
// a message without it rather than taking the header's word that no
// signature was needed. allowUnsigned relaxes that for a subscription during
// a migration to signing. A message that says it is signed is never accepted
// without verification, and is rejected when there is no verifier to check
// it, rather than parsed as an unsigned event (which yields a zero event).
func decodeDelivery(d amqp.Delivery, verifier *EventVerifier, signingRequired, allowUnsigned bool) (*BaseEvent, error) {
	signed, _ := d.Headers["signed"].(bool)

	if signed {
		if verifier == nil {
			return nil, &rejection{reasonNoVerifier, "unknown",
				errors.New("received a signed event but this subscriber has no verifier")}
		}
		var signedEvent SignedEvent
		if err := json.Unmarshal(d.Body, &signedEvent); err != nil {
			return nil, &rejection{reasonParseError, "unknown", fmt.Errorf("parse signed event: %w", err)}
		}
		if signedEvent.Event == nil {
			return nil, &rejection{reasonParseError, "unknown", errors.New("signed event has no event")}
		}
		if err := verifier.Verify(&signedEvent); err != nil {
			return nil, &rejection{reasonInvalidSignature, signedEvent.Event.Type,
				fmt.Errorf("signature verification failed for event %s from %s: %w",
					signedEvent.Event.Type, signedEvent.Event.Source, err)}
		}
		return signedEvent.Event, nil
	}

	if (verifier != nil || signingRequired) && !allowUnsigned {
		return nil, &rejection{reasonUnsigned, "unknown",
			errors.New("unsigned event rejected: this subscriber verifies signatures")}
	}

	event, err := FromJSON(d.Body)
	if err != nil {
		return nil, &rejection{reasonParseError, "unknown", fmt.Errorf("parse event: %w", err)}
	}
	logging.Warn("⚠️  Processing unsigned event: %s (ID: %s)", event.Type, event.ID)
	return event, nil
}
