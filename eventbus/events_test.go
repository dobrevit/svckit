package eventbus_test

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	"github.com/google/uuid"
)

func TestNewEventPopulatesTheEnvelope(t *testing.T) {
	before := time.Now()
	event := eventbus.NewEvent("order.created", "orders", map[string]any{"order_id": "o-1"})

	if event.ID == "" {
		t.Error("no event ID was assigned")
	}
	if event.Type != "order.created" || event.Source != "orders" {
		t.Errorf("event = %+v", event)
	}
	if event.Timestamp.Before(before) {
		t.Errorf("Timestamp = %v, want at or after %v", event.Timestamp, before)
	}
	if event.Data["order_id"] != "o-1" {
		t.Errorf("Data = %v", event.Data)
	}
	if event.UserID != "" {
		t.Errorf("UserID = %q, want empty until set", event.UserID)
	}
}

// Deduplication downstream depends on IDs being unique, including for events
// created in the same instant on many goroutines.
func TestEventIDsAreUnique(t *testing.T) {
	const workers, each = 10, 1000
	ids := make(chan string, workers*each)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				ids <- eventbus.NewEvent("order.created", "orders", nil).ID
			}
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[string]bool, workers*each)
	for id := range ids {
		if seen[id] {
			t.Fatalf("event ID %q was generated twice", id)
		}
		seen[id] = true
	}
	if len(seen) != workers*each {
		t.Fatalf("%d distinct ids, want %d", len(seen), workers*each)
	}
}

// An id is a database key (an outbox row, an inbox claim), so it has a
// standard form rather than a timestamp and a few random letters.
func TestEventIDsAreUUIDs(t *testing.T) {
	id := eventbus.NewEvent("order.created", "orders", nil).ID
	parsed, err := uuid.Parse(id)
	if err != nil {
		t.Fatalf("event id %q is not a UUID: %v", id, err)
	}
	if parsed.Version() != 4 {
		t.Errorf("event id %q is a version %d UUID, want a random (v4) one", id, parsed.Version())
	}
}

func TestNewEventWithIDKeepsTheID(t *testing.T) {
	const id = "4b1f0f5e-7c0e-4e8a-9a51-3f2f1d0c9b77"
	event := eventbus.NewEventWithID(id, "order.created", "orders", map[string]any{"order": "o-1"})

	if event.ID != id {
		t.Errorf("ID = %q, want the caller's %q", event.ID, id)
	}
	if event.Type != "order.created" || event.Source != "orders" || event.Data["order"] != "o-1" || event.Timestamp.IsZero() {
		t.Errorf("the rest of the envelope was not populated: %+v", event)
	}
	if err := event.ValidateEvent(); err != nil {
		t.Errorf("an event with a caller-supplied id does not validate: %v", err)
	}
}

func TestJSONRoundTripPreservesTheEvent(t *testing.T) {
	original := eventbus.NewEvent("order.created", "orders", map[string]any{
		"order_id": "o-1",
		"total":    42.5,
		"items":    []any{"a", "b"},
	})
	original.SetUserID("u-1")

	encoded, err := original.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}

	decoded, err := eventbus.FromJSON(encoded)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}

	if decoded.ID != original.ID || decoded.Type != original.Type || decoded.Source != original.Source {
		t.Errorf("envelope changed: %+v vs %+v", decoded, original)
	}
	if decoded.UserID != "u-1" {
		t.Errorf("UserID = %q, want u-1", decoded.UserID)
	}
	if !decoded.Timestamp.Equal(original.Timestamp) {
		t.Errorf("Timestamp = %v, want %v", decoded.Timestamp, original.Timestamp)
	}
	if decoded.Data["order_id"] != "o-1" || decoded.Data["total"] != 42.5 {
		t.Errorf("Data = %v", decoded.Data)
	}
}

func TestFromJSONRejectsGarbage(t *testing.T) {
	if _, err := eventbus.FromJSON([]byte("{not json")); err == nil {
		t.Error("malformed JSON was accepted")
	}
}

// An event with no user is common, so user_id must not appear as an empty
// string in the wire format.
func TestUserIDIsOmittedWhenUnset(t *testing.T) {
	encoded, err := eventbus.NewEvent("order.created", "orders", nil).ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshalling: %v", err)
	}
	if _, present := decoded["user_id"]; present {
		t.Errorf("user_id present on an event with no user: %s", encoded)
	}
}

func TestValidateEventRequiresTheIdentifyingFields(t *testing.T) {
	valid := func() *eventbus.BaseEvent {
		return &eventbus.BaseEvent{
			ID: "e-1", Type: "order.created", Source: "orders", Timestamp: time.Now(),
		}
	}

	if err := valid().ValidateEvent(); err != nil {
		t.Fatalf("a complete event was rejected: %v", err)
	}

	cases := map[string]func(*eventbus.BaseEvent){
		"no ID":        func(e *eventbus.BaseEvent) { e.ID = "" },
		"no type":      func(e *eventbus.BaseEvent) { e.Type = "" },
		"no source":    func(e *eventbus.BaseEvent) { e.Source = "" },
		"no timestamp": func(e *eventbus.BaseEvent) { e.Timestamp = time.Time{} },
	}
	for name, break_ := range cases {
		t.Run(name, func(t *testing.T) {
			event := valid()
			break_(event)
			if err := event.ValidateEvent(); err == nil {
				t.Errorf("an event with %s was accepted", name)
			}
		})
	}
}

func TestEventTypeSplitsIntoDomainAndAction(t *testing.T) {
	cases := []struct {
		eventType      string
		domain, action string
	}{
		{"order.created", "order", "created"},
		{"order.line.added", "order", "line"},
		{"heartbeat", "heartbeat", ""},
		{"", "", ""},
	}

	for _, tc := range cases {
		event := &eventbus.BaseEvent{Type: tc.eventType}
		if got := event.GetEventDomain(); got != tc.domain {
			t.Errorf("%q: domain = %q, want %q", tc.eventType, got, tc.domain)
		}
		if got := event.GetEventAction(); got != tc.action {
			t.Errorf("%q: action = %q, want %q", tc.eventType, got, tc.action)
		}
	}
}

func TestContextCarriesTheUserAndBroadcastEnvelope(t *testing.T) {
	ctx := t.Context()

	if _, ok := eventbus.UserID(ctx); ok {
		t.Error("a bare context reported a user")
	}
	if _, ok := eventbus.Broadcast(ctx); ok {
		t.Error("a bare context reported a broadcast envelope")
	}

	withUser := eventbus.WithUserID(ctx, "u-1")
	if id, ok := eventbus.UserID(withUser); !ok || id != "u-1" {
		t.Errorf("UserID = (%q, %v), want (u-1, true)", id, ok)
	}

	envelope := &eventbus.BroadcastEvent{Exchange: "fanout", InstanceID: "i-1"}
	withBroadcast := eventbus.WithBroadcast(withUser, envelope)
	got, ok := eventbus.Broadcast(withBroadcast)
	if !ok || got.Exchange != "fanout" {
		t.Errorf("Broadcast = (%+v, %v)", got, ok)
	}
	// The user must survive alongside it.
	if id, _ := eventbus.UserID(withBroadcast); id != "u-1" {
		t.Errorf("UserID after adding a broadcast = %q, want u-1", id)
	}
}

// An empty user must not read as "a user called empty string".
func TestAnEmptyUserIDIsNotAUser(t *testing.T) {
	if _, ok := eventbus.UserID(eventbus.WithUserID(t.Context(), "")); ok {
		t.Error("an empty user ID was reported as present")
	}
}
