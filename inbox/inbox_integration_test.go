//go:build integration

// Needs the Postgres from compose.yaml. These run on pgx; testkit runs the
// same claims on lib/pq.

package inbox

import (
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
	db, err := sql.Open("pgx", fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		get("DB_USER", "test_user"), get("DB_PASSWORD", "test_password"),
		get("DB_HOST", "localhost"), get("DB_PORT", "5433"), get("DB_NAME", "test_db")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func freshInbox(t *testing.T, db *sql.DB) (*Inbox, string) {
	t.Helper()
	table := fmt.Sprintf("inbox_it_%d", time.Now().UnixNano())
	if _, err := db.Exec(Schema(table)); err != nil {
		t.Fatalf("the documented DDL failed: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + table) })
	in, err := New(table)
	if err != nil {
		t.Fatal(err)
	}
	return in, table
}

func TestAMessageIsClaimedOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db := testDB(t)
	in, _ := freshInbox(t, db)
	ctx := context.Background()

	for i, want := range []bool{true, false} {
		got, err := in.Claim(ctx, db, "m-1", "billing")
		if err != nil || got != want {
			t.Errorf("claim %d = %v, %v; want %v", i+1, got, err, want)
		}
	}
	// Another consumer of the same message has a claim of its own.
	if got, err := in.Claim(ctx, db, "m-1", "audit"); err != nil || !got {
		t.Errorf("another consumer's claim = %v, %v; want true", got, err)
	}
}

// Two deliveries of one message handled at the same time: the second
// claim waits for the first transaction and follows its outcome.
func TestConcurrentClaimsFollowTheFirstTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	for _, outcome := range []string{"commit", "rollback"} {
		t.Run(outcome, func(t *testing.T) {
			db := testDB(t)
			in, _ := freshInbox(t, db)
			ctx := context.Background()

			first, _ := db.BeginTx(ctx, nil)
			claimed, err := in.Claim(ctx, first, "m-1", "billing")
			if err != nil || !claimed {
				t.Fatalf("first claim = %v, %v", claimed, err)
			}

			second := make(chan bool, 1)
			go func() {
				tx, _ := db.BeginTx(ctx, nil)
				defer func() { _ = tx.Rollback() }()
				got, err := in.Claim(ctx, tx, "m-1", "billing")
				if err != nil {
					t.Error(err)
				}
				second <- got
			}()

			select {
			case <-second:
				t.Fatal("the second claim returned while the first transaction was open")
			case <-time.After(300 * time.Millisecond):
			}

			if outcome == "commit" {
				_ = first.Commit()
			} else {
				_ = first.Rollback()
			}
			want := outcome == "rollback"
			select {
			case got := <-second:
				if got != want {
					t.Errorf("after the first %s, the second claim = %v, want %v", outcome, got, want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the second claim never returned")
			}
		})
	}
}

func TestThePurgeRemovesOnlyOldClaims(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db := testDB(t)
	in, table := freshInbox(t, db)
	ctx := context.Background()
	for _, id := range []string{"old-1", "old-2", "new"} {
		if _, err := in.Claim(ctx, db, id, "billing"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("UPDATE " + table + " SET claimed_at = now() - interval '8 days' WHERE message_id LIKE 'old-%'"); err != nil {
		t.Fatal(err)
	}

	purged, err := in.Purge(ctx, db, 7*24*time.Hour)
	if err != nil || purged != 2 {
		t.Errorf("Purge = %d, %v; want the two old claims", purged, err)
	}
	if got, _ := in.Claim(ctx, db, "new", "billing"); got {
		t.Error("the recent claim was purged")
	}
}
