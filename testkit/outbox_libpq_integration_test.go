//go:build integration

package testkit_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/dobrevit/svckit/outbox"
	_ "github.com/lib/pq"
)

// The outbox writer on lib/pq, as outbox's own tests run it on pgx: a write
// rolls back and commits with the caller's transaction, the body is stored
// byte for byte, and an erasure removes one subject's pending rows.
func TestOutboxWithLibPQ(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db, err := sql.Open("postgres", testDatabaseURL())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	table := fmt.Sprintf("outbox_pq_it_%d", time.Now().UnixNano())
	if _, err := db.Exec(outbox.Schema(table)); err != nil {
		t.Fatalf("the documented DDL failed on lib/pq: %v", err)
	}
	defer db.Exec("DROP TABLE IF EXISTS " + table) //nolint:errcheck // best-effort cleanup
	w, err := outbox.NewWriter(table)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte{0x7b, 0x00, 0xff, 0x7d}

	tx, _ := db.BeginTx(ctx, nil)
	if err := w.Write(ctx, tx, outbox.Message{ID: "rolled-back", RoutingKey: "q", Body: body}); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback()

	tx, _ = db.BeginTx(ctx, nil)
	for _, m := range []outbox.Message{
		{ID: "a1", RoutingKey: "q", Body: body, SubjectKey: "alice"},
		{ID: "b1", RoutingKey: "q", Body: body, SubjectKey: "bob"},
	} {
		if err := w.Write(ctx, tx, m); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var n int
	_ = db.QueryRow("SELECT count(*) FROM " + table).Scan(&n)
	if n != 2 {
		t.Fatalf("%d rows, want the two committed ones", n)
	}
	var got []byte
	_ = db.QueryRow("SELECT body FROM " + table + " WHERE id = 'a1'").Scan(&got)
	if !bytes.Equal(got, body) {
		t.Errorf("body = %x, want %x", got, body)
	}

	deleted, err := w.DeleteUnpublished(ctx, db, "alice")
	if err != nil || deleted != 1 {
		t.Errorf("DeleteUnpublished = %d, %v; want alice's one row", deleted, err)
	}
}
