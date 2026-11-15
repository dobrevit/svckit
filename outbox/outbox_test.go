package outbox

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
)

type recordingExecer struct {
	query string
	args  []any
}

func (r *recordingExecer) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	r.query, r.args = query, args
	return nil, nil
}

func TestWriteSendsNullsForWhatIsNotSet(t *testing.T) {
	w, _ := NewWriter("")
	ex := &recordingExecer{}
	if err := w.Write(context.Background(), ex, Message{ID: "m-1", Exchange: "events", RoutingKey: "order.created", Body: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(ex.query, "INSERT INTO svckit_outbox") {
		t.Errorf("query = %q", ex.query)
	}
	if ex.args[3] != "{}" || ex.args[4] != "application/json" {
		t.Errorf("headers %v, content type %v", ex.args[3], ex.args[4])
	}
	if subject := ex.args[6].(sql.NullString); subject.Valid {
		t.Error("an empty subject key was written as a value, not NULL")
	}
	if expires := ex.args[7].(sql.NullTime); expires.Valid {
		t.Error("a zero ExpiresAt was written as a value, not NULL")
	}

	at := time.Now().Add(time.Minute)
	_ = w.Write(context.Background(), ex, Message{ID: "m-2", RoutingKey: "q", Body: []byte("x"), SubjectKey: "person-7", ExpiresAt: at})
	if subject := ex.args[6].(sql.NullString); !subject.Valid || subject.String != "person-7" {
		t.Errorf("subject = %+v", subject)
	}
	if expires := ex.args[7].(sql.NullTime); !expires.Valid || !expires.Time.Equal(at) {
		t.Errorf("expires = %+v", expires)
	}
}

func TestWriteRefusesAMessageWithoutIDOrDestination(t *testing.T) {
	w, _ := NewWriter("")
	for _, msg := range []Message{{Exchange: "events", RoutingKey: "x"}, {ID: "m-1"}} {
		if err := w.Write(context.Background(), &recordingExecer{}, msg); err == nil {
			t.Errorf("wrote %+v", msg)
		}
	}
}

func TestTableNamesAreIdentifiers(t *testing.T) {
	for _, ok := range []string{"", "outbox", "messaging.outbox", "_o2"} {
		if _, err := NewWriter(ok); err != nil {
			t.Errorf("%q refused: %v", ok, err)
		}
	}
	for _, bad := range []string{"outbox; DROP TABLE x", "a.b.c", "1outbox", `"quoted"`, "out box"} {
		if _, err := NewWriter(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestEventMessage(t *testing.T) {
	event := eventbus.NewEvent("order.created", "orders", map[string]any{"total": 42})

	plain, err := EventMessage(context.Background(), event, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if plain.ID != event.ID || plain.Exchange != eventbus.DefaultExchange || plain.RoutingKey != "order.created" || plain.Headers["signed"] != false {
		t.Errorf("unsigned message = %+v", plain)
	}

	h := eventbus.NewHMAC(&eventbus.SignatureConfig{SigningKey: []byte("outbox-test-key-0123456789"), ExpiryWindow: time.Hour,
		RequiredClaims: []string{"id", "type", "timestamp", "source"}})
	signed, err := EventMessage(context.Background(), event, h, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if signed.Headers["signed"] != true || signed.Headers["signature"] == "" || signed.Exchange != "orders" {
		t.Errorf("signed message headers = %v", signed.Headers)
	}
	if !strings.Contains(string(signed.Body), `"signature"`) {
		t.Error("the body of a signed message is not the signed envelope")
	}
}
