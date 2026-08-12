package pgcluster

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// newTestManager builds a manager with the given nodes, bypassing the
// constructor so no connection is attempted. The pools are non-nil sentinels:
// routing only checks for nil and returns the pointer.
func newTestManager(config ClusterConfig, nodes ...*DatabaseNode) *ClusterManager {
	config.setDefaults()
	return &ClusterManager{config: config, nodes: nodes}
}

func healthyNode(name string, role NodeRole, priority int) *DatabaseNode {
	return &DatabaseNode{
		Name:     name,
		Role:     role,
		Priority: priority,
		Health:   HealthHealthy,
		sqlDB:    &sql.DB{},
	}
}

func TestWriterReturnsTheHealthyWriter(t *testing.T) {
	writer := healthyNode("writer", RoleWriter, 0)
	cm := newTestManager(ClusterConfig{}, healthyNode("reader", RoleReader, 1), writer)

	pool, err := cm.Writer()
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if pool != writer.sqlDB {
		t.Error("Writer returned a pool other than the writer node's")
	}
}

func TestWriterFailsAndOpensTheCircuitWhenNoWriterIsHealthy(t *testing.T) {
	writer := healthyNode("writer", RoleWriter, 0)
	writer.Health = HealthFailed
	cm := newTestManager(ClusterConfig{}, writer)

	if _, err := cm.Writer(); err == nil {
		t.Fatal("Writer succeeded with no healthy writer node")
	}
	if !cm.circuitOpen {
		t.Error("the circuit breaker stayed closed after the writer was found unavailable")
	}
}

func TestWriterRefusesWhileTheCircuitIsOpen(t *testing.T) {
	cm := newTestManager(ClusterConfig{CircuitBreakerTimeout: time.Hour},
		healthyNode("writer", RoleWriter, 0))
	cm.circuitOpen = true
	cm.circuitOpenTime = time.Now()

	if _, err := cm.Writer(); err == nil {
		t.Fatal("Writer served a request while the circuit breaker was open")
	}
}

func TestTheCircuitClosesAgainAfterItsTimeout(t *testing.T) {
	writer := healthyNode("writer", RoleWriter, 0)
	cm := newTestManager(ClusterConfig{CircuitBreakerTimeout: time.Millisecond}, writer)
	cm.circuitOpen = true
	cm.circuitOpenTime = time.Now().Add(-time.Second)

	if _, err := cm.Writer(); err != nil {
		t.Fatalf("Writer stayed shut after the circuit timeout elapsed: %v", err)
	}
}

func TestReaderFallsBackToTheWriterWithNoHealthyReplica(t *testing.T) {
	writer := healthyNode("writer", RoleWriter, 0)
	sickReader := healthyNode("reader", RoleReader, 1)
	sickReader.Health = HealthFailed

	cm := newTestManager(ClusterConfig{}, writer, sickReader)

	pool, err := cm.Reader()
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	if pool != writer.sqlDB {
		t.Error("Reader did not fall back to the writer pool")
	}
}

// The writer serves reads alongside the replicas, so round robin cycles over
// all three healthy nodes.
func TestReaderRoundRobinsAcrossEveryHealthyNode(t *testing.T) {
	writer := healthyNode("writer", RoleWriter, 0)
	first := healthyNode("reader-1", RoleReader, 1)
	second := healthyNode("reader-2", RoleReader, 1)
	cm := newTestManager(ClusterConfig{ReadBalanceStrategy: "round_robin"}, writer, first, second)

	seen := map[*sql.DB]int{}
	for range 6 {
		pool, err := cm.Reader()
		if err != nil {
			t.Fatalf("Reader: %v", err)
		}
		seen[pool]++
	}

	for _, node := range []*DatabaseNode{writer, first, second} {
		if seen[node.sqlDB] != 2 {
			t.Errorf("distribution = %v, want each of the three nodes twice", seen)
			break
		}
	}
}

func TestDBRoutesByTheContextHint(t *testing.T) {
	writer := healthyNode("writer", RoleWriter, 0)
	reader := healthyNode("reader", RoleReader, 1)
	cm := newTestManager(ClusterConfig{}, writer, reader)

	writePool, err := cm.DB(cm.WithWriter(context.Background()))
	if err != nil {
		t.Fatalf("DB(write): %v", err)
	}
	if writePool != writer.sqlDB {
		t.Error("a write-hinted context was not routed to the writer")
	}

	// Read-hinted and unhinted contexts both take the balanced read path,
	// which includes the writer, so assert only that they stay within it.
	for _, ctx := range []context.Context{cm.WithReader(context.Background()), context.Background()} {
		pool, err := cm.DB(ctx)
		if err != nil {
			t.Fatalf("DB(read): %v", err)
		}
		if pool != reader.sqlDB && pool != writer.sqlDB {
			t.Error("a read went to neither the replica nor the writer")
		}
	}
}

// The hint used to travel under the bare string key "database_write", which
// any other package could set. Only the manager's own key may be honoured.
func TestForeignContextValueCannotForceTheWriter(t *testing.T) {
	writer := healthyNode("writer", RoleWriter, 0)
	reader := healthyNode("reader", RoleReader, 1)
	cm := newTestManager(ClusterConfig{}, writer, reader)

	//nolint:staticcheck // deliberately using a string key, as outside code would
	ctx := context.WithValue(context.Background(), "database_write", true)

	// The writer also serves reads, so a single call could land on it by
	// chance; a write hint would pin every call to it.
	for range 4 {
		pool, err := cm.DB(ctx)
		if err != nil {
			t.Fatalf("DB: %v", err)
		}
		if pool == reader.sqlDB {
			return // took the balanced read path, so the key was ignored
		}
	}
	t.Error("a string-keyed context value was honoured as a write hint")
}

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	cm := newTestManager(ClusterConfig{
		RetryBackoffBase: time.Second,
		RetryBackoffMax:  4 * time.Second,
	})

	first := cm.calculateBackoff(0)
	second := cm.calculateBackoff(1)
	if second <= first {
		t.Errorf("backoff did not grow: attempt 0 = %v, attempt 1 = %v", first, second)
	}

	// The cap applies before jitter, which adds at most a quarter more.
	capped := cm.calculateBackoff(10)
	if capped > 5*time.Second {
		t.Errorf("backoff = %v, want no more than the 4s cap plus jitter", capped)
	}
}

func TestDriverNameDefaults(t *testing.T) {
	config := ClusterConfig{WriterURL: "postgres://localhost/db"}
	config.setDefaults()

	if config.DriverName != DefaultDriverName {
		t.Errorf("DriverName = %q, want %q", config.DriverName, DefaultDriverName)
	}
}

func TestValidateRequiresAWriterURL(t *testing.T) {
	if err := (&ClusterConfig{}).Validate(); err == nil {
		t.Fatal("a config with no writer URL was accepted")
	}
}

// The constructor applies defaults before validating, so a caller only has to
// supply the two settings that have no sensible default.
func TestAMinimalConfigPassesValidationOnceDefaultsApply(t *testing.T) {
	config := ClusterConfig{WriterURL: "postgres://localhost/db", ServiceName: "test-service"}
	config.setDefaults()

	if err := config.Validate(); err != nil {
		t.Fatalf("a minimal config was rejected after defaults: %v", err)
	}
}
