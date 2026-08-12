package eventtest

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dobrevit/svckit/eventbus"
)

type EventAssertions struct {
	t assert.TestingT
}

// NewEventAssertions creates new event assertion helper
func NewEventAssertions(t assert.TestingT) *EventAssertions {
	return &EventAssertions{t: t}
}

// AssertEventPublished verifies that an event was published
func (a *EventAssertions) AssertEventPublished(publisher *MockEventPublisher, eventType string, msgAndArgs ...any) bool {
	events := publisher.GetPublishedEventsOfType(eventType)
	return assert.NotEmpty(a.t, events, msgAndArgs...)
}

// AssertEventNotPublished verifies that an event was not published
func (a *EventAssertions) AssertEventNotPublished(publisher *MockEventPublisher, eventType string, msgAndArgs ...any) bool {
	events := publisher.GetPublishedEventsOfType(eventType)
	return assert.Empty(a.t, events, msgAndArgs...)
}

// AssertEventData verifies event data contains expected fields
func (a *EventAssertions) AssertEventData(event *eventbus.BaseEvent, expectedData map[string]any, msgAndArgs ...any) bool {
	for key, expectedValue := range expectedData {
		actualValue, exists := event.Data[key]
		if !assert.True(a.t, exists, "Event data missing key: %s", key) {
			return false
		}
		if !assert.Equal(a.t, expectedValue, actualValue, "Event data mismatch for key: %s", key) {
			return false
		}
	}
	return true
}

// AssertEventStructure verifies basic event structure
func (a *EventAssertions) AssertEventStructure(event *eventbus.BaseEvent, msgAndArgs ...any) bool {
	return assert.NotEmpty(a.t, event.ID, "Event ID should not be empty") &&
		assert.NotEmpty(a.t, event.Type, "Event type should not be empty") &&
		assert.NotEmpty(a.t, event.Source, "Event source should not be empty") &&
		assert.False(a.t, event.Timestamp.IsZero(), "Event timestamp should not be zero")
}

// AssertEventOrder verifies events were published in correct order
func (a *EventAssertions) AssertEventOrder(publisher *MockEventPublisher, expectedOrder []string, msgAndArgs ...any) bool {
	events := publisher.GetPublishedEvents()

	if !assert.GreaterOrEqual(a.t, len(events), len(expectedOrder), "Not enough events published") {
		return false
	}

	for i, expectedType := range expectedOrder {
		if i >= len(events) {
			return assert.Fail(a.t, fmt.Sprintf("Expected event %s at position %d, but only %d events published", expectedType, i, len(events)))
		}
		if !assert.Equal(a.t, expectedType, events[i].Type, "Event order mismatch at position %d", i) {
			return false
		}
	}

	return true
}

// AssertEventTimestamps verifies event timestamps are in chronological order
func (a *EventAssertions) AssertEventTimestamps(events []eventbus.BaseEvent, msgAndArgs ...any) bool {
	for i := 1; i < len(events); i++ {
		if !assert.True(a.t, events[i].Timestamp.After(events[i-1].Timestamp) || events[i].Timestamp.Equal(events[i-1].Timestamp),
			"Event timestamps not in chronological order at index %d", i) {
			return false
		}
	}
	return true
}

// SecurityAssertions provides specialized assertions for security testing
type SecurityAssertions struct {
	t assert.TestingT
}

// NewSecurityAssertions creates new security assertion helper
func NewSecurityAssertions(t assert.TestingT) *SecurityAssertions {
	return &SecurityAssertions{t: t}
}

// AssertSignatureValid verifies event signature is valid
func (a *SecurityAssertions) AssertSignatureValid(signedEvent *eventbus.SignedEvent, config *eventbus.SignatureConfig, msgAndArgs ...any) bool {
	err := eventbus.VerifyEvent(signedEvent, config)
	return assert.NoError(a.t, err, msgAndArgs...)
}

// AssertSignatureInvalid verifies event signature is invalid
func (a *SecurityAssertions) AssertSignatureInvalid(signedEvent *eventbus.SignedEvent, config *eventbus.SignatureConfig, msgAndArgs ...any) bool {
	err := eventbus.VerifyEvent(signedEvent, config)
	return assert.Error(a.t, err, msgAndArgs...)
}

// AssertEventNotExpired verifies event is not expired
func (a *SecurityAssertions) AssertEventNotExpired(event *eventbus.BaseEvent, expiryWindow time.Duration, msgAndArgs ...any) bool {
	expiryTime := event.Timestamp.Add(expiryWindow)
	return assert.True(a.t, time.Now().Before(expiryTime), "Event should not be expired")
}

// AssertEventExpired verifies event is expired
func (a *SecurityAssertions) AssertEventExpired(event *eventbus.BaseEvent, expiryWindow time.Duration, msgAndArgs ...any) bool {
	expiryTime := event.Timestamp.Add(expiryWindow)
	return assert.True(a.t, time.Now().After(expiryTime), "Event should be expired")
}

// AssertRequiredClaims verifies event contains all required claims
func (a *SecurityAssertions) AssertRequiredClaims(event *eventbus.BaseEvent, requiredClaims []string, msgAndArgs ...any) bool {
	for _, claim := range requiredClaims {
		switch claim {
		case "id":
			if !assert.NotEmpty(a.t, event.ID, "Event ID claim is required") {
				return false
			}
		case "type":
			if !assert.NotEmpty(a.t, event.Type, "Event type claim is required") {
				return false
			}
		case "timestamp":
			if !assert.False(a.t, event.Timestamp.IsZero(), "Event timestamp claim is required") {
				return false
			}
		case "source":
			if !assert.NotEmpty(a.t, event.Source, "Event source claim is required") {
				return false
			}
		}
	}
	return true
}

// HTTPAssertions provides specialized assertions for HTTP testing
