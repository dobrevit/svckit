//go:build integration

package testkit_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/dobrevit/svckit/inbox"
	_ "github.com/lib/pq"
)

// The inbox on lib/pq, as inbox's own tests run it on pgx.
func TestInboxWithLibPQ(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db, err := sql.Open("postgres", testDatabaseURL())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	table := fmt.Sprintf("inbox_pq_it_%d", time.Now().UnixNano())
	if _, err := db.Exec(inbox.Schema(table)); err != nil {
		t.Fatalf("the documented DDL failed on lib/pq: %v", err)
	}
	defer db.Exec("DROP TABLE IF EXISTS " + table) //nolint:errcheck // best-effort cleanup
	in, err := inbox.New(table)
	if err != nil {
		t.Fatal(err)
	}

	for i, want := range []bool{true, false} {
		if got, err := in.Claim(ctx, db, "m-1", "billing"); err != nil || got != want {
			t.Errorf("claim %d = %v, %v; want %v", i+1, got, err, want)
		}
	}

	first, _ := db.BeginTx(ctx, nil)
	if got, err := in.Claim(ctx, first, "m-2", "billing"); err != nil || !got {
		t.Fatalf("first claim of m-2 = %v, %v", got, err)
	}
	second := make(chan bool, 1)
	go func() {
		tx, _ := db.BeginTx(ctx, nil)
		defer func() { _ = tx.Rollback() }()
		got, _ := in.Claim(ctx, tx, "m-2", "billing")
		second <- got
	}()
	time.Sleep(200 * time.Millisecond)
	_ = first.Rollback()
	if got := <-second; !got {
		t.Error("after the first transaction rolled back, the second claim was refused")
	}

	if _, err := db.Exec("UPDATE " + table + " SET claimed_at = now() - interval '2 days' WHERE message_id = 'm-1'"); err != nil {
		t.Fatal(err)
	}
	if purged, err := in.Purge(ctx, db, 24*time.Hour); err != nil || purged != 1 {
		t.Errorf("Purge = %d, %v; want the old claim", purged, err)
	}
}
