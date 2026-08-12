package audit_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dobrevit/svckit/audit"
	"github.com/dobrevit/svckit/eventbus"
)

// fakePublisher records what the client publishes instead of reaching a broker.
type fakePublisher struct {
	mu       sync.Mutex
	events   []*eventbus.BaseEvent
	closed   bool
	publErr  error
	closeErr error
}

func (f *fakePublisher) Publish(_ context.Context, event *eventbus.BaseEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.publErr != nil {
		return f.publErr
	}
	f.events = append(f.events, event)
	return nil
}

func (f *fakePublisher) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return f.closeErr
}

func (f *fakePublisher) only(t *testing.T) *eventbus.BaseEvent {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.events) != 1 {
		t.Fatalf("published %d events, want exactly 1", len(f.events))
	}
	return f.events[0]
}

// auditDataOf pulls the audit payload back out of the published event, going
// through JSON so the test sees what a consumer would receive rather than the
// in-process struct.
func auditDataOf(t *testing.T, event *eventbus.BaseEvent) audit.AuditEventData {
	t.Helper()
	raw, ok := event.Data["audit_data"]
	if !ok {
		t.Fatalf("event carries no audit_data: %+v", event.Data)
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshalling audit_data: %v", err)
	}
	var data audit.AuditEventData
	if err := json.Unmarshal(encoded, &data); err != nil {
		t.Fatalf("unmarshalling audit_data: %v", err)
	}
	return data
}

func newTestClient() (*audit.AuditClient, *fakePublisher) {
	pub := &fakePublisher{}
	return audit.NewAuditClientWithPublisher(pub, "orders"), pub
}

func TestLogPublishesAnAuditEvent(t *testing.T) {
	client, pub := newTestClient()

	err := client.Log(context.Background(), "user_action", "user", "u-1", "create", "order", "success", nil)
	if err != nil {
		t.Fatalf("Log: %v", err)
	}

	event := pub.only(t)
	if event.Type != "audit.log" {
		t.Errorf("event type = %q, want audit.log", event.Type)
	}
	if event.Source != "orders" {
		t.Errorf("event source = %q, want orders", event.Source)
	}

	data := auditDataOf(t, event)
	if data.ActorID != "u-1" || data.Action != "create" || data.Resource != "order" {
		t.Errorf("audit data = %+v", data)
	}
	if data.Status != "success" {
		t.Errorf("status = %q, want success", data.Status)
	}
	if data.EventID == "" {
		t.Error("no event ID was assigned")
	}
	if data.Version == "" {
		t.Error("no schema version was recorded — a consumer cannot tell the shape it is reading")
	}
}

// Every audit record must be individually identifiable, or two identical
// actions cannot be told apart.
func TestEachEventGetsItsOwnID(t *testing.T) {
	client, pub := newTestClient()
	ctx := context.Background()

	for range 3 {
		if err := client.Log(ctx, "user_action", "user", "u-1", "read", "order", "success", nil); err != nil {
			t.Fatalf("Log: %v", err)
		}
	}

	seen := map[string]bool{}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	for _, event := range pub.events {
		id := auditDataOf(t, event).EventID
		if seen[id] {
			t.Fatalf("event ID %q was reused", id)
		}
		seen[id] = true
	}
}

// A user's action must be attributable to them on the event itself, not only
// inside the payload, since that is what downstream routing reads.
func TestAUserActionCarriesTheUserOnTheEvent(t *testing.T) {
	client, pub := newTestClient()

	if err := client.LogUserAction(context.Background(), "u-1", "Ada", "update", "order", "success", nil); err != nil {
		t.Fatalf("LogUserAction: %v", err)
	}

	event := pub.only(t)
	if event.UserID != "u-1" {
		t.Errorf("event UserID = %q, want u-1", event.UserID)
	}

	data := auditDataOf(t, event)
	if data.EventType != "user_action" || data.ActorType != "user" {
		t.Errorf("audit data = %+v, want a user_action by a user", data)
	}
	if data.Metadata["actor_name"] != "Ada" {
		t.Errorf("actor_name = %v, want Ada", data.Metadata["actor_name"])
	}
}

// A service acting on its own behalf is not a user, and must not be recorded
// as one.
func TestAServiceActionIsAttributedToTheService(t *testing.T) {
	client, pub := newTestClient()

	if err := client.LogServiceAction(context.Background(), "reconcile", "ledger", "success", nil); err != nil {
		t.Fatalf("LogServiceAction: %v", err)
	}

	event := pub.only(t)
	if event.UserID != "" {
		t.Errorf("event UserID = %q, want empty for a service action", event.UserID)
	}

	data := auditDataOf(t, event)
	if data.ActorType != "service" || data.ActorID != "orders" {
		t.Errorf("audit data = %+v, want a service action by orders", data)
	}
}

// Security events are the ones most likely to be leaked onward, so they carry
// the strictest sensitivity unless the caller chose one deliberately.
func TestSecurityEventsDefaultToRestricted(t *testing.T) {
	client, pub := newTestClient()

	err := client.LogSecurityEvent(context.Background(), "user", "u-1", "login_failed", "session", "failure", nil)
	if err != nil {
		t.Fatalf("LogSecurityEvent: %v", err)
	}

	data := auditDataOf(t, pub.only(t))
	if data.Sensitivity != "restricted" {
		t.Errorf("sensitivity = %q, want restricted", data.Sensitivity)
	}
	if data.EventType != "security_event" {
		t.Errorf("event type = %q, want security_event", data.EventType)
	}
}

func TestAnExplicitSensitivitySurvives(t *testing.T) {
	client, pub := newTestClient()

	err := client.LogSecurityEvent(context.Background(), "user", "u-1", "login", "session", "success",
		&audit.LogOptions{Sensitivity: "internal"})
	if err != nil {
		t.Fatalf("LogSecurityEvent: %v", err)
	}

	if got := auditDataOf(t, pub.only(t)).Sensitivity; got != "internal" {
		t.Errorf("sensitivity = %q, want the caller's choice of internal", got)
	}
}

func TestDataAccessRecordsWhatWasRead(t *testing.T) {
	client, pub := newTestClient()

	if err := client.LogDataAccess(context.Background(), "u-1", "customer", "c-99", nil); err != nil {
		t.Fatalf("LogDataAccess: %v", err)
	}

	data := auditDataOf(t, pub.only(t))
	if data.Action != "read" || data.Resource != "customer" {
		t.Errorf("audit data = %+v, want a read of customer", data)
	}
	if data.ResourceID != "c-99" {
		t.Errorf("ResourceID = %q, want c-99 — the record does not say which one was read", data.ResourceID)
	}
}

// A modification record is only useful if it says what changed.
func TestDataModificationRecordsTheChanges(t *testing.T) {
	client, pub := newTestClient()

	changes := map[string]any{"email": "new@example.com"}
	err := client.LogDataModification(context.Background(), "u-1", "update", "customer", "c-99", changes, nil)
	if err != nil {
		t.Fatalf("LogDataModification: %v", err)
	}

	data := auditDataOf(t, pub.only(t))
	if data.ResourceID != "c-99" || data.Action != "update" {
		t.Errorf("audit data = %+v", data)
	}
	if data.Changes["email"] != "new@example.com" {
		t.Errorf("changes = %v, want the new email", data.Changes)
	}
}

func TestOptionalContextIsCarriedThrough(t *testing.T) {
	client, pub := newTestClient()

	err := client.Log(context.Background(), "user_action", "user", "u-1", "delete", "order", "failure",
		&audit.LogOptions{
			TraceID:    "trace-1",
			SessionID:  "session-1",
			IPAddress:  "203.0.113.7",
			UserAgent:  "curl/8",
			Platform:   "api",
			StatusCode: 500,
			ErrorMsg:   "downstream unavailable",
			Duration:   1500 * time.Millisecond,
			Purpose:    "support request",
		})
	if err != nil {
		t.Fatalf("Log: %v", err)
	}

	data := auditDataOf(t, pub.only(t))
	if data.TraceID != "trace-1" || data.SessionID != "session-1" {
		t.Errorf("correlation fields lost: %+v", data)
	}
	if data.IPAddress != "203.0.113.7" || data.UserAgent != "curl/8" || data.Platform != "api" {
		t.Errorf("request context lost: %+v", data)
	}
	if data.StatusCode != 500 || data.ErrorMsg != "downstream unavailable" {
		t.Errorf("failure detail lost: %+v", data)
	}
	if data.Duration != 1500 {
		t.Errorf("Duration = %d, want 1500 milliseconds", data.Duration)
	}
	if data.Purpose != "support request" {
		t.Errorf("Purpose = %q", data.Purpose)
	}
}

// A failure to record an audit event must reach the caller: silently losing
// one defeats the point of auditing.
func TestAPublishFailureIsReported(t *testing.T) {
	pub := &fakePublisher{publErr: errors.New("broker unreachable")}
	client := audit.NewAuditClientWithPublisher(pub, "orders")

	err := client.Log(context.Background(), "user_action", "user", "u-1", "create", "order", "success", nil)
	if err == nil {
		t.Fatal("a failed publish was reported as success")
	}
	if !errors.Is(err, pub.publErr) {
		t.Errorf("error %v does not wrap the publisher's failure", err)
	}
}

func TestCloseClosesThePublisher(t *testing.T) {
	client, pub := newTestClient()

	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !pub.closed {
		t.Error("the publisher was not closed")
	}
}

// appkit leaves Audit nil when audit was optional and the broker was
// unreachable, so every method has to treat a nil client as "switched off".
// Before this was fixed only LogSecurityEvent did, and the rest panicked.
func TestANilClientIsAuditingSwitchedOff(t *testing.T) {
	var client *audit.AuditClient
	ctx := context.Background()

	calls := map[string]func() error{
		"Log": func() error {
			return client.Log(ctx, "user_action", "user", "u-1", "create", "order", "success", nil)
		},
		"LogUserAction": func() error {
			return client.LogUserAction(ctx, "u-1", "Ada", "create", "order", "success", nil)
		},
		"LogServiceAction": func() error {
			return client.LogServiceAction(ctx, "reconcile", "ledger", "success", nil)
		},
		"LogSecurityEvent": func() error {
			return client.LogSecurityEvent(ctx, "user", "u-1", "login", "session", "success", nil)
		},
		"LogDataAccess": func() error {
			return client.LogDataAccess(ctx, "u-1", "customer", "c-99", nil)
		},
		"LogDataModification": func() error {
			return client.LogDataModification(ctx, "u-1", "update", "customer", "c-99", nil, nil)
		},
		"Close": client.Close,
	}

	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked on a nil client: %v", r)
				}
			}()
			if err := call(); err != nil {
				t.Errorf("returned %v, want nil", err)
			}
		})
	}
}
