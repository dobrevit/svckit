//go:build integration

// Needs the Postgres from compose.yaml, addressed by the DB_* variables it
// documents. These run on pgx; testkit runs the same writes on lib/pq.

package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	get := func(key, fallback string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return fallback
	}
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		get("DB_USER", "test_user"), get("DB_PASSWORD", "test_password"),
		get("DB_HOST", "localhost"), get("DB_PORT", "5433"), get("DB_NAME", "test_db"))
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// freshTable creates an outbox table from the documented DDL, dropped when
// the test ends.
func freshTable(t *testing.T, db *sql.DB) string {
	t.Helper()
	table := fmt.Sprintf("outbox_it_%d", time.Now().UnixNano())
	if _, err := db.Exec(Schema(table)); err != nil {
		t.Fatalf("the documented DDL failed: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + table) })
	return table
}

func count(t *testing.T, db *sql.DB, table, where string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT count(*) FROM "+table+" WHERE "+where, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestWriteCommitsAndRollsBackWithTheCallersTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db := testDB(t)
	table := freshTable(t, db)
	w, err := NewWriter(table)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	body := []byte{0x7b, 0x00, 0xff, 0x22, 0x7d} // not valid UTF-8: stored as bytes, not text

	tx, _ := db.BeginTx(ctx, nil)
	if err := w.Write(ctx, tx, Message{ID: "rolled-back", Exchange: "events", RoutingKey: "x", Body: body}); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback()
	if n := count(t, db, table, "true"); n != 0 {
		t.Fatalf("%d rows after a rollback, want 0", n)
	}

	tx, _ = db.BeginTx(ctx, nil)
	expires := time.Now().Add(time.Hour).Truncate(time.Microsecond)
	if err := w.Write(ctx, tx, Message{ID: "committed", Exchange: "events", RoutingKey: "order.created",
		Headers: map[string]any{"signed": true}, Body: body, SubjectKey: "person-7", ExpiresAt: expires}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var gotBody []byte
	var subject string
	var gotExpires time.Time
	var headers string
	if err := db.QueryRow("SELECT body, subject_key, expires_at, headers::text FROM "+table+" WHERE id = 'committed'").
		Scan(&gotBody, &subject, &gotExpires, &headers); err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, table, "true"); n != 1 {
		t.Errorf("%d rows after a commit, want exactly 1", n)
	}
	if !bytes.Equal(gotBody, body) {
		t.Errorf("body = %x, want %x byte for byte", gotBody, body)
	}
	if subject != "person-7" || !gotExpires.Equal(expires) || headers != `{"signed": true}` {
		t.Errorf("subject %q, expires %v, headers %s", subject, gotExpires, headers)
	}
}

// An erasure removes what was not yet sent about one subject, and nothing
// else.
func TestDeleteUnpublishedRemovesOneSubjectsPendingMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db := testDB(t)
	table := freshTable(t, db)
	w, _ := NewWriter(table)
	ctx := context.Background()

	for id, subject := range map[string]string{"a1": "alice", "a2": "alice", "a3": "alice", "b1": "bob", "n1": ""} {
		if err := w.Write(ctx, db, Message{ID: id, RoutingKey: "q", Body: []byte("x"), SubjectKey: subject}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("UPDATE " + table + " SET published_at = now() WHERE id = 'a3'"); err != nil {
		t.Fatal(err)
	}

	tx, _ := db.BeginTx(ctx, nil)
	n, err := w.DeleteUnpublished(ctx, tx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("deleted %d, want alice's two unpublished messages", n)
	}
	if left := count(t, db, table, "subject_key = 'alice'"); left != 1 {
		t.Errorf("%d of alice's rows left, want her published one", left)
	}
	if left := count(t, db, table, "subject_key IS DISTINCT FROM 'alice'"); left != 2 {
		t.Errorf("%d other rows left, want bob's and the unattributed one", left)
	}
}
