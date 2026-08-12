// Package eventtest provides event-bus test doubles and assertions for the
// generic eventbus package: mock publisher/subscriber, a collecting event
// handler, signing helpers and event/security assertions.
package eventtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/dobrevit/svckit/eventbus"
)

type MockEventPublisher struct {
	mock.Mock
	PublishedEvents []eventbus.BaseEvent
	mutex           sync.RWMutex
}

// NewMockEventPublisher creates a new mock event publisher
func NewMockEventPublisher() *MockEventPublisher {
	return &MockEventPublisher{
		PublishedEvents: make([]eventbus.BaseEvent, 0),
	}
}

// Publish mocks event publishing
func (m *MockEventPublisher) Publish(ctx context.Context, event *eventbus.BaseEvent) error {
	args := m.Called(ctx, event)

	// Store the event for verification
	m.mutex.Lock()
	m.PublishedEvents = append(m.PublishedEvents, *event)
	m.mutex.Unlock()

	return args.Error(0)
}

// TestConnection mocks connection testing
func (m *MockEventPublisher) TestConnection() error {
	args := m.Called()
	return args.Error(0)
}

// Close mocks publisher cleanup
func (m *MockEventPublisher) Close() error {
	args := m.Called()
	return args.Error(0)
}

// GetPublishedEvents returns all published events thread-safely
func (m *MockEventPublisher) GetPublishedEvents() []eventbus.BaseEvent {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// Return a copy to prevent race conditions
	events := make([]eventbus.BaseEvent, len(m.PublishedEvents))
	copy(events, m.PublishedEvents)
	return events
}

// GetPublishedEventsOfType returns published events of a specific type
func (m *MockEventPublisher) GetPublishedEventsOfType(eventType string) []eventbus.BaseEvent {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var filtered []eventbus.BaseEvent
	for _, event := range m.PublishedEvents {
		if event.Type == eventType {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

// ClearPublishedEvents clears the published events list
func (m *MockEventPublisher) ClearPublishedEvents() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.PublishedEvents = make([]eventbus.BaseEvent, 0)
}

// MockEventSubscriber is a mock implementation of eventbus.Subscriber for testing
type MockEventSubscriber struct {
	mock.Mock
	subscriptions map[string]eventbus.EventHandler
	mutex         sync.RWMutex
}

// NewMockEventSubscriber creates a new mock event subscriber
func NewMockEventSubscriber() *MockEventSubscriber {
	return &MockEventSubscriber{
		subscriptions: make(map[string]eventbus.EventHandler),
	}
}

// Subscribe mocks event subscription
func (m *MockEventSubscriber) Subscribe(eventType string, queueName string, handler eventbus.EventHandler) error {
	args := m.Called(eventType, queueName, handler)

	m.mutex.Lock()
	m.subscriptions[eventType] = handler
	m.mutex.Unlock()

	return args.Error(0)
}

// Close mocks subscriber cleanup
func (m *MockEventSubscriber) Close() error {
	args := m.Called()
	return args.Error(0)
}

// SimulateEvent simulates receiving an event for testing
func (m *MockEventSubscriber) SimulateEvent(event *eventbus.BaseEvent) error {
	m.mutex.RLock()
	handler, exists := m.subscriptions[event.Type]
	m.mutex.RUnlock()

	if !exists {
		return errors.New("no handler subscribed for event type")
	}

	return handler.Handle(context.Background(), event)
}

// MockHTTPClient is a mock implementation of httpclient.Client for testing
type TestEventHandler struct {
	Events    []eventbus.BaseEvent
	EventChan chan *eventbus.BaseEvent
	EventType string
	ErrorChan chan error
	mutex     sync.RWMutex
}

// Handle processes incoming test events
func (h *TestEventHandler) Handle(ctx context.Context, event *eventbus.BaseEvent) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.Events = append(h.Events, *event)
	if event.Type == h.EventType {
		select {
		case h.EventChan <- event:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			return fmt.Errorf("event channel full")
		}
	}
	return nil
}

// NewTestEventHandler creates a new test event handler
func NewTestEventHandler() *TestEventHandler {
	return &TestEventHandler{
		Events: make([]eventbus.BaseEvent, 0),
	}
}

// GetEvents returns all collected events
func (h *TestEventHandler) GetEvents() []eventbus.BaseEvent {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	events := make([]eventbus.BaseEvent, len(h.Events))
	copy(events, h.Events)
	return events
}

// GetEventsOfType returns events of a specific type
func (h *TestEventHandler) GetEventsOfType(eventType string) []eventbus.BaseEvent {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	var filtered []eventbus.BaseEvent
	for _, event := range h.Events {
		if event.Type == eventType {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

// Clear clears all collected events
func (h *TestEventHandler) Clear() {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.Events = make([]eventbus.BaseEvent, 0)
}

// MockTime allows time-based testing
type SecurityTestHelper struct {
	ValidSigningKey   []byte
	InvalidSigningKey []byte
	ExpiredEvent      *eventbus.BaseEvent
}

// NewSecurityTestHelper creates security testing utilities
func NewSecurityTestHelper() *SecurityTestHelper {
	return &SecurityTestHelper{
		ValidSigningKey:   []byte("valid-32-character-signing-key!!"),
		InvalidSigningKey: []byte("invalid-key"),
	}
}

// CreateExpiredEvent creates an event with expired timestamp
func (h *SecurityTestHelper) CreateExpiredEvent() *eventbus.BaseEvent {
	return &eventbus.BaseEvent{
		ID:        "expired-event",
		Type:      "test.expired",
		Timestamp: time.Now().Add(-2 * time.Hour), // 2 hours ago
		Source:    "test-service",
		UserID:    "test-user",
		Data:      map[string]any{"test": "expired"},
	}
}

// CreateTamperedEvent creates an event with tampered data
func (h *SecurityTestHelper) CreateTamperedEvent(originalEvent *eventbus.BaseEvent) *eventbus.BaseEvent {
	tampered := *originalEvent
	tampered.Data = map[string]any{
		"tampered":  true,
		"malicious": "data",
	}
	return &tampered
}

// PerformanceTestHelper provides utilities for performance testing
