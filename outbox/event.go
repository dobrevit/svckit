package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dobrevit/svckit/eventbus"
)

// EventMessage builds the Message for an event as eventbus's publishers
// would send it: routed by its type, with its id as the message id, and
// signed by signer when one is given (seal the event first if it needs
// sealing). Set ExpiresAt and SubjectKey on the result as needed.
func EventMessage(ctx context.Context, event *eventbus.BaseEvent, signer eventbus.Signer, exchange string) (Message, error) {
	if exchange == "" {
		exchange = eventbus.DefaultExchange
	}
	msg := Message{
		ID:         event.ID,
		Exchange:   exchange,
		RoutingKey: event.Type,
		Headers: map[string]any{
			"signed":     false,
			"source":     event.Source,
			"event_type": event.Type,
			"event_id":   event.ID,
		},
	}

	if signer == nil {
		body, err := json.Marshal(event)
		if err != nil {
			return Message{}, fmt.Errorf("outbox: encode event: %w", err)
		}
		msg.Body = body
		return msg, nil
	}

	signed, err := signer.Sign(ctx, event)
	if err != nil {
		return Message{}, fmt.Errorf("outbox: sign event: %w", err)
	}
	body, err := json.Marshal(signed)
	if err != nil {
		return Message{}, fmt.Errorf("outbox: encode signed event: %w", err)
	}
	msg.Body = body
	msg.Headers["signed"] = true
	msg.Headers["signature"] = signed.Signature
	msg.Headers["signed_at"] = signed.SignedAt.Unix()
	msg.Headers["expires_at"] = signed.ExpiresAt.Unix()
	return msg, nil
}
