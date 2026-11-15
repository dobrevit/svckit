//go:build integration

// This test lives in testkit because testkit already depends on lib/pq; the
// core module does not, and pgcluster's SQLSTATE helpers are written so that
// it need not.

package testkit_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dobrevit/svckit/pgcluster"
	"github.com/lib/pq"
)

func testDatabaseURL() string {
	get := func(key, fallback string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return fallback
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		get("DB_USER", "test_user"), get("DB_PASSWORD", "test_password"),
		get("DB_HOST", "localhost"), get("DB_PORT", "5433"), get("DB_NAME", "test_db"))
}

// With DB_DRIVER=postgres pgcluster opens lib/pq, which used to be
// impossible through LoadConfigFromEnv, and IsUniqueViolation recognises
// lib/pq's error as it does pgx's.
func TestPgclusterUniqueViolationWithLibPQ(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	t.Setenv("DATABASE_URL", testDatabaseURL())
	t.Setenv("DB_DRIVER", "postgres")

	config, err := pgcluster.LoadConfigFromEnv("pgcluster-it")
	if err != nil {
		t.Fatalf("LoadConfigFromEnv: %v", err)
	}
	if config.DriverName != "postgres" {
		t.Fatalf("DriverName = %q, want postgres", config.DriverName)
	}
	cm, err := pgcluster.NewClusterManager(*config)
	if err != nil {
		t.Fatalf("NewClusterManager: %v", err)
	}
	defer cm.Close()
	db, err := cm.Writer()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	table := fmt.Sprintf("svckit_it_unique_pq_%d", time.Now().UnixNano())
	if _, err := db.ExecContext(ctx, "CREATE TABLE "+table+" (id text PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "DROP TABLE "+table) //nolint:errcheck // best-effort cleanup
	if _, err := db.ExecContext(ctx, "INSERT INTO "+table+" VALUES ('a')"); err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, "INSERT INTO "+table+" VALUES ('a')")
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) {
		t.Fatalf("duplicate insert returned %T %v, want lib/pq's *pq.Error", err, err)
	}
	if !pgcluster.IsUniqueViolation(fmt.Errorf("insert: %w", err)) {
		t.Errorf("IsUniqueViolation false for %v", err)
	}
}
