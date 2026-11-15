// Package eventbus is the generic RabbitMQ event system: the BaseEvent
// envelope, topic publisher/subscriber with signing, fanout broadcast,
// per-type dispatch with real outcome metrics, and DLQ support. It carries no
// domain event registry — applications define their own event-type constants
// and, if they want, thin constructors on top of NewEvent.
package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// PublisherInterface defines the common interface for event publishers
type PublisherInterface interface {
	Publish(ctx context.Context, event *BaseEvent) error
	TestConnection() error
	Close() error
}

// SubscriberInterface defines the common interface for event subscribers
type SubscriberInterface interface {
	Subscribe(queueName string, routingKey string, handler EventHandler) error
	Close() error
}

// BaseEvent represents the common structure for all events
type BaseEvent struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	Timestamp time.Time      `json:"timestamp"`
	Source    string         `json:"source"`
	UserID    string         `json:"user_id,omitempty"`
	Data      map[string]any `json:"data"`
}

// NewEvent creates a new base event with a fresh random (version 4) UUID as
// its id. The id is the AMQP message id of every publish of the event, and so
// what a consumer deduplicates on.
func NewEvent(eventType, source string, data map[string]any) *BaseEvent {
	return NewEventWithID(generateEventID(), eventType, source, data)
}

// NewEventWithID creates an event with an id the caller chose. Use it when
// the id must be known before the event exists or must survive a retry: an
// outbox row's key, so that a consumer deduplicating by message id sees one
// message however often the row is relayed. Any non-empty string is valid;
// a UUID is the expected form.
func NewEventWithID(id, eventType, source string, data map[string]any) *BaseEvent {
	return &BaseEvent{
		ID:        id,
		Type:      eventType,
		Timestamp: time.Now(),
		Source:    source,
		Data:      data,
	}
}

// SetUserID sets the user ID for the event
func (e *BaseEvent) SetUserID(userID string) {
	e.UserID = userID
}

// ToJSON converts the event to JSON
func (e *BaseEvent) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

// FromJSON creates an event from JSON
func FromJSON(data []byte) (*BaseEvent, error) {
	var event BaseEvent
	err := json.Unmarshal(data, &event)
	if err != nil {
		return nil, err
	}
	return &event, nil
}

// ValidateEvent validates that the event has the required fields
func (e *BaseEvent) ValidateEvent() error {
	if e.ID == "" {
		return fmt.Errorf("event ID is required")
	}
	if e.Type == "" {
		return fmt.Errorf("event type is required")
	}
	if e.Source == "" {
		return fmt.Errorf("event source is required")
	}
	if e.Timestamp.IsZero() {
		return fmt.Errorf("event timestamp is required")
	}
	return nil
}

// GetEventDomain extracts the domain from event type (e.g., "user" from "user.registered")
func (e *BaseEvent) GetEventDomain() string {
	parts := strings.Split(e.Type, ".")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

// GetEventAction extracts the action from event type (e.g., "registered" from "user.registered")
func (e *BaseEvent) GetEventAction() string {
	parts := strings.Split(e.Type, ".")
	if len(parts) > 1 {
		return parts[1]
	}
	return ""
}

// generateEventID returns a random (version 4) UUID. It replaced a
// nanosecond timestamp with eight characters from math/rand, which was not
// built to be unique across processes and could not serve as a database key.
func generateEventID() string {
	return uuid.NewString()
}
