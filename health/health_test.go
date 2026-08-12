package health_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dobrevit/svckit/health"
	"github.com/redis/go-redis/v9"
)

// unreachableConnector stands in for a database that cannot be reached,
// without needing a driver registration or a server.
type unreachableConnector struct{}

func (unreachableConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("connection refused")
}

func (unreachableConnector) Driver() driver.Driver { return nil }

func unreachableDB() *sql.DB { return sql.OpenDB(unreachableConnector{}) }

// reachableConnector stands in for a database that answers, so the healthy
// path is exercised without a server either.
type reachableConnector struct{}

func (reachableConnector) Connect(context.Context) (driver.Conn, error) { return pingableConn{}, nil }

func (reachableConnector) Driver() driver.Driver { return nil }

type pingableConn struct{}

func (pingableConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not needed") }
func (pingableConn) Close() error                        { return nil }
func (pingableConn) Begin() (driver.Tx, error)           { return nil, errors.New("not needed") }
func (pingableConn) Ping(context.Context) error          { return nil }

func reachableDB(t *testing.T) *sql.DB {
	t.Helper()
	db := sql.OpenDB(reachableConnector{})
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// unreachableRedis points at a port nothing is listening on.
func unreachableRedis() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
}

func TestAServiceWithNoDependenciesIsHealthy(t *testing.T) {
	report := health.NewHealthChecker("orders").CheckHealth(context.Background())

	if report.Status != "healthy" {
		t.Errorf("Status = %q, want healthy", report.Status)
	}
	if report.Service != "orders" {
		t.Errorf("Service = %q, want orders", report.Service)
	}
	if len(report.Dependencies) != 0 {
		t.Errorf("Dependencies = %v, want none for a service that wired none", report.Dependencies)
	}
	if report.Version == "" || report.Timestamp == "" {
		t.Errorf("Version = %q, Timestamp = %q — both should be populated", report.Version, report.Timestamp)
	}
}

// The runtime figures are the ones an operator reads first when a service is
// misbehaving but its dependencies are fine.
func TestRuntimeMetricsAreAlwaysReported(t *testing.T) {
	report := health.NewHealthChecker("orders").CheckHealth(context.Background())

	for _, key := range []string{"goroutines", "memory_alloc_mb", "memory_sys_mb", "gc_runs", "cpu_count"} {
		if _, ok := report.Metrics[key]; !ok {
			t.Errorf("metric %q missing from %v", key, report.Metrics)
		}
	}
	if n, ok := report.Metrics["goroutines"].(int); !ok || n <= 0 {
		t.Errorf("goroutines = %v, want a positive count", report.Metrics["goroutines"])
	}
}

func TestAnUnreachableDatabaseDegradesTheService(t *testing.T) {
	checker := health.NewHealthChecker("orders")
	checker.SetDatabase(unreachableDB())

	report := checker.CheckHealth(context.Background())

	if report.Status != "degraded" {
		t.Errorf("Status = %q, want degraded", report.Status)
	}
	if report.Dependencies["postgres"] {
		t.Error("postgres reported as reachable when it is not")
	}
}

func TestAReachableDatabaseKeepsTheServiceHealthy(t *testing.T) {
	checker := health.NewHealthChecker("orders")
	checker.SetDatabase(reachableDB(t))

	report := checker.CheckHealth(context.Background())

	if report.Status != "healthy" {
		t.Errorf("Status = %q, want healthy", report.Status)
	}
	if !report.Dependencies["postgres"] {
		t.Error("a reachable database was reported as down")
	}
}

func TestAnUnreachableRedisDegradesTheService(t *testing.T) {
	client := unreachableRedis()
	defer client.Close()

	checker := health.NewHealthChecker("orders")
	checker.SetRedis(client)

	report := checker.CheckHealth(context.Background())

	if report.Status != "degraded" {
		t.Errorf("Status = %q, want degraded", report.Status)
	}
	if report.Dependencies["redis"] {
		t.Error("redis reported as reachable when it is not")
	}
}

// A dependency that was never wired must not appear at all — reporting it as
// false would make a service look broken for not using Redis.
func TestUnwiredDependenciesAreAbsentRatherThanFalse(t *testing.T) {
	checker := health.NewHealthChecker("orders")
	checker.SetDatabase(unreachableDB())

	report := checker.CheckHealth(context.Background())

	for _, name := range []string{"redis", "redis_cluster", "rabbitmq_publisher", "rabbitmq_subscriber"} {
		if _, present := report.Dependencies[name]; present {
			t.Errorf("dependency %q appears in the report though it was never wired", name)
		}
	}
}

func TestSimpleHealthHandlerAnswersProbes(t *testing.T) {
	w := httptest.NewRecorder()
	health.SimpleHealthHandler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q", got)
	}

	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding body: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("body = %v, want status ok", body)
	}
}

func TestDetailedHandlerReports200WhenHealthy(t *testing.T) {
	w := httptest.NewRecorder()
	health.NewHealthChecker("orders").DetailedHealthHandler().
		ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}

	var report health.ServiceHealth
	if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
		t.Fatalf("decoding body: %v", err)
	}
	if report.Service != "orders" || report.Status != "healthy" {
		t.Errorf("report = %+v", report)
	}
}

// An orchestrator reads the status code, not the body, so a degraded service
// has to answer 503 or it will keep receiving traffic.
func TestDetailedHandlerReports503WhenDegraded(t *testing.T) {
	checker := health.NewHealthChecker("orders")
	checker.SetDatabase(unreachableDB())

	w := httptest.NewRecorder()
	checker.DetailedHealthHandler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestServiceHealthMarshalsWithSnakeCaseKeys(t *testing.T) {
	report := health.NewHealthChecker("orders").CheckHealth(context.Background())

	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshalling: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshalling: %v", err)
	}
	for _, key := range []string{"status", "version", "uptime", "timestamp", "service", "dependencies", "metrics"} {
		if _, ok := decoded[key]; !ok {
			t.Errorf("key %q missing from %s", key, encoded)
		}
	}
	// error is omitempty: a healthy report should not carry an empty one.
	if _, ok := decoded["error"]; ok {
		t.Errorf("a healthy report carries an empty error field: %s", encoded)
	}
}
