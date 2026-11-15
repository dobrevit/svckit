package pgcluster

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// pqLikeError has the shape of lib/pq's *pq.Error, whose SQLState method
// (lib/pq 1.10+) returns its Code. The real type is exercised in testkit's
// integration test, so that this module need not import lib/pq.
type pqLikeError struct{ Code string }

func (e *pqLikeError) Error() string    { return "pq: duplicate key value violates unique constraint" }
func (e *pqLikeError) SQLState() string { return e.Code }

func TestSQLStateWithEitherDriver(t *testing.T) {
	for name, err := range map[string]error{
		"pgx":         &pgconn.PgError{Code: "23505"},
		"lib/pq":      &pqLikeError{Code: "23505"},
		"pgx wrapped": fmt.Errorf("insert order: %w", fmt.Errorf("tx: %w", &pgconn.PgError{Code: "23505"})),
		"pq wrapped":  fmt.Errorf("insert order: %w", &pqLikeError{Code: "23505"}),
	} {
		if got := SQLState(err); got != "23505" {
			t.Errorf("%s: SQLState = %q, want 23505", name, got)
		}
		if !IsUniqueViolation(err) {
			t.Errorf("%s: IsUniqueViolation = false", name)
		}
	}
}

func TestSQLStateOfOtherErrors(t *testing.T) {
	if got := SQLState(errors.New("connection refused")); got != "" {
		t.Errorf("SQLState of a plain error = %q, want empty", got)
	}
	if SQLState(nil) != "" || IsUniqueViolation(nil) {
		t.Error("nil has a SQLSTATE")
	}
	if IsUniqueViolation(&pgconn.PgError{Code: "23503"}) {
		t.Error("a foreign key violation counted as a unique violation")
	}
}

func TestLoadConfigFromEnvReadsDBDriver(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost:5432/db")

	t.Setenv("DB_DRIVER", "")
	config, err := LoadConfigFromEnv("orders")
	if err != nil {
		t.Fatalf("LoadConfigFromEnv: %v", err)
	}
	if config.DriverName != "pgx" {
		t.Errorf("DriverName with DB_DRIVER unset = %q, want pgx", config.DriverName)
	}

	t.Setenv("DB_DRIVER", "pgx")
	if config, err = LoadConfigFromEnv("orders"); err != nil || config.DriverName != "pgx" {
		t.Errorf("DB_DRIVER=pgx: %q, %v", config.DriverName, err)
	}

	// lib/pq is not imported by this test binary, so "postgres" is not
	// registered, and the error says what to do about it.
	t.Setenv("DB_DRIVER", "postgres")
	_, err = LoadConfigFromEnv("orders")
	if err == nil || !strings.Contains(err.Error(), "github.com/lib/pq") {
		t.Errorf("an unregistered driver gave %v, want a pointer to the import", err)
	}
}
