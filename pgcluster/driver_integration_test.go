//go:build integration

// The tests in this file need the Postgres from compose.yaml (`docker compose
// up -d --wait`), addressed by the DB_* variables it documents.

package pgcluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
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

// The default driver, through LoadConfigFromEnv as app.WithDatabase opens
// it: a duplicate key is recognised by IsUniqueViolation, and the error is
// pgx's.
func TestUniqueViolationWithPgx(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	t.Setenv("DATABASE_URL", testDatabaseURL())
	t.Setenv("DB_DRIVER", "pgx")

	config, err := LoadConfigFromEnv("pgcluster-it")
	if err != nil {
		t.Fatalf("LoadConfigFromEnv: %v", err)
	}
	cm, err := NewClusterManager(*config)
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
	table := fmt.Sprintf("svckit_it_unique_%d", time.Now().UnixNano())
	if _, err := db.ExecContext(ctx, "CREATE TABLE "+table+" (id text PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "DROP TABLE "+table) //nolint:errcheck // best-effort cleanup
	if _, err := db.ExecContext(ctx, "INSERT INTO "+table+" VALUES ('a')"); err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, "INSERT INTO "+table+" VALUES ('a')")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("duplicate insert returned %T %v, want pgx's *pgconn.PgError", err, err)
	}
	if !IsUniqueViolation(fmt.Errorf("insert: %w", err)) || SQLState(err) != SQLStateUniqueViolation {
		t.Errorf("IsUniqueViolation false for %v", err)
	}
}
