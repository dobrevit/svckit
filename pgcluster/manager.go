package pgcluster

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"github.com/dobrevit/svckit/logging"
)

// NodeRole represents the role of a database node
type NodeRole int

const (
	RoleWriter NodeRole = iota
	RoleReader
	RoleUnknown
)

func (r NodeRole) String() string {
	switch r {
	case RoleWriter:
		return "writer"
	case RoleReader:
		return "reader"
	default:
		return "unknown"
	}
}

// NodeHealth represents the health state of a database node
type NodeHealth int

const (
	HealthHealthy NodeHealth = iota
	HealthDegraded
	HealthFailed
)

func (h NodeHealth) String() string {
	switch h {
	case HealthHealthy:
		return "healthy"
	case HealthDegraded:
		return "degraded"
	default:
		return "failed"
	}
}

// DatabaseNode represents a single PostgreSQL instance
type DatabaseNode struct {
	URL      string
	Name     string
	Role     NodeRole
	Health   NodeHealth
	Priority int // Lower number = higher priority for reads

	// Connection details
	sqlDB     *sql.DB
	lastCheck time.Time
	lastError error

	// Metrics
	readCount    int64
	writeCount   int64
	errorCount   int64
	responseTime time.Duration

	mutex sync.RWMutex
}

// WriterDetectionStrategy defines how to detect the writer node
type WriterDetectionStrategy int

const (
	StrategyDNS    WriterDetectionStrategy = iota // DNS-based (writer.db.example.com)
	StrategyQuery                                 // SQL query-based detection
	StrategyConfig                                // Configuration-based (explicit writer)
	StrategyProbe                                 // Health probe-based detection
)

// ClusterConfig holds configuration for the database cluster manager
type ClusterConfig struct {
	// Connection URLs
	WriterURL  string   // Primary writer connection
	ReaderURLs []string // Read replica connections

	// Writer detection
	WriterDetectionStrategy WriterDetectionStrategy
	WriterDetectionQuery    string // Custom query to detect writer
	WriterDNSName           string // DNS name that always points to writer

	// Connection pooling
	MaxOpenConns    int           // Maximum open connections per node
	MaxIdleConns    int           // Maximum idle connections per node
	ConnMaxLifetime time.Duration // Connection lifetime
	ConnMaxIdleTime time.Duration // Connection idle timeout

	// Health and retry settings
	HealthCheckInterval time.Duration // How often to check node health
	RetryAttempts       int           // Max retry attempts for connections
	RetryBackoffBase    time.Duration // Base backoff duration
	RetryBackoffMax     time.Duration // Maximum backoff duration

	// Circuit breaker
	CircuitBreakerThreshold int           // Failures before opening circuit
	CircuitBreakerTimeout   time.Duration // Circuit breaker timeout

	// Read balancing
	ReadBalanceStrategy string // "round_robin", "random", "priority", "least_connections"

	// Service identification
	ServiceName string

	// DriverName is the database/sql driver used to open each node.
	// Defaults to DefaultDriverName.
	DriverName string
}

// DefaultDriverName is the database/sql driver used when ClusterConfig leaves
// DriverName empty. The package imports the pgx stdlib driver to register it;
// setting DriverName to something else requires the caller to import that
// driver instead.
const DefaultDriverName = "pgx"

// ClusterManager manages connections to multiple PostgreSQL nodes
type ClusterManager struct {
	config ClusterConfig
	nodes  []*DatabaseNode

	// Read balancing
	readIndex int64 // For round-robin balancing

	// Circuit breaker state
	circuitOpen     bool
	circuitOpenTime time.Time

	// Background task management
	tombManager *TombManager

	mutex sync.RWMutex
}

// NewClusterManager creates a new database cluster manager
func NewClusterManager(config ClusterConfig) (*ClusterManager, error) {
	// Defaults first: Validate rejects a zero MaxOpenConns, health interval
	// and retry count, all of which setDefaults fills in. Validating first
	// meant a caller had to spell out every pool setting to get past it.
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	manager := &ClusterManager{
		config:      config,
		nodes:       make([]*DatabaseNode, 0),
		tombManager: NewTombManager(fmt.Sprintf("db-cluster-%s", config.ServiceName)),
	}

	// Initialize nodes
	if err := manager.initializeNodes(); err != nil {
		return nil, fmt.Errorf("failed to initialize nodes: %w", err)
	}

	// Start background monitoring
	manager.startHealthMonitoring()
	manager.startWriterDetection()

	return manager, nil
}

// initializeNodes sets up all database nodes
func (cm *ClusterManager) initializeNodes() error {
	var allErrors []error

	// Add writer node
	if cm.config.WriterURL != "" {
		writerNode := &DatabaseNode{
			URL:      cm.config.WriterURL,
			Name:     "writer-primary",
			Role:     RoleWriter,
			Priority: 0, // Highest priority
		}

		if err := cm.connectToNode(writerNode); err != nil {
			logging.Error("Failed to connect to writer node: %v", err)
			allErrors = append(allErrors, err)
		}

		cm.nodes = append(cm.nodes, writerNode)
	}

	// Add reader nodes
	for i, readerURL := range cm.config.ReaderURLs {
		readerNode := &DatabaseNode{
			URL:      readerURL,
			Name:     fmt.Sprintf("reader-%d", i+1),
			Role:     RoleReader,
			Priority: i + 1, // Lower priority than writer
		}

		if err := cm.connectToNode(readerNode); err != nil {
			logging.Error("Failed to connect to reader node %s: %v", readerNode.Name, err)
			allErrors = append(allErrors, err)
		}

		cm.nodes = append(cm.nodes, readerNode)
	}

	// Ensure we have at least one healthy connection
	if !cm.hasHealthyNodes() {
		return fmt.Errorf("no healthy database nodes available: %v", allErrors)
	}

	logging.Info("Initialized %d database nodes (%d healthy)", len(cm.nodes), cm.countHealthyNodes())
	return nil
}

// connectToNode establishes connection to a single database node
func (cm *ClusterManager) connectToNode(node *DatabaseNode) error {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	// Close existing connection
	if node.sqlDB != nil {
		_ = node.sqlDB.Close() // replacing this pool; nothing to do if it objects
	}

	// Open the pool with retry. sql.Open itself only validates arguments, so
	// the retry loop exists for the ping below, which is what actually
	// reaches the server.
	var sqlDB *sql.DB
	var err error

	for attempt := 0; attempt < cm.config.RetryAttempts; attempt++ {
		sqlDB, err = sql.Open(cm.config.DriverName, node.URL)
		if err == nil {
			break
		}

		if attempt < cm.config.RetryAttempts-1 {
			backoff := cm.calculateBackoff(attempt)
			logging.Warn("Connection attempt %d to %s failed, retrying in %v: %v",
				attempt+1, node.Name, backoff, err)
			time.Sleep(backoff)
		}
	}

	if err != nil {
		node.Health = HealthFailed
		node.lastError = err
		node.lastCheck = time.Now()
		atomic.AddInt64(&node.errorCount, 1)
		return fmt.Errorf("failed to connect to %s after %d attempts: %w",
			node.Name, cm.config.RetryAttempts, err)
	}

	// Configure connection pool
	sqlDB.SetMaxOpenConns(cm.config.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cm.config.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(cm.config.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(cm.config.ConnMaxIdleTime)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close() // discarding a pool that cannot reach the server
		node.Health = HealthFailed
		node.lastError = err
		return fmt.Errorf("failed to ping %s: %w", node.Name, err)
	}

	// Update node state
	node.sqlDB = sqlDB
	node.Health = HealthHealthy
	node.lastError = nil
	node.lastCheck = time.Now()

	logging.Info("Successfully connected to database node: %s (%s)", node.Name, node.Role)
	return nil
}

// calculateBackoff calculates exponential backoff duration
func (cm *ClusterManager) calculateBackoff(attempt int) time.Duration {
	backoff := time.Duration(1<<uint(attempt)) * cm.config.RetryBackoffBase
	if backoff > cm.config.RetryBackoffMax {
		backoff = cm.config.RetryBackoffMax
	}
	// Add jitter to prevent thundering herd
	jitter := time.Duration(rand.Int63n(int64(backoff / 4)))
	return backoff + jitter
}

// Writer returns the connection pool for the writer node.
func (cm *ClusterManager) Writer() (*sql.DB, error) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	// Check circuit breaker
	if cm.isCircuitOpen() {
		return nil, errors.New("circuit breaker is open")
	}

	// Find healthy writer
	for _, node := range cm.nodes {
		node.mutex.RLock()
		if node.Role == RoleWriter && node.Health == HealthHealthy && node.sqlDB != nil {
			atomic.AddInt64(&node.writeCount, 1)
			db := node.sqlDB
			node.mutex.RUnlock()
			return db, nil
		}
		node.mutex.RUnlock()
	}

	// No healthy writer found - this is critical
	cm.openCircuitBreaker()
	return nil, errors.New("no healthy writer node available")
}

// Reader returns a connection pool for reads, chosen by the configured
// balancing strategy. The writer participates in read balancing alongside the
// replicas -- a primary serves reads too -- so a single-node deployment and a
// deployment whose replicas are all down both keep serving.
func (cm *ClusterManager) Reader() (*sql.DB, error) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	// Check circuit breaker
	if cm.isCircuitOpen() {
		return nil, errors.New("circuit breaker is open")
	}

	// Get healthy readers
	readers := cm.getHealthyReaders()
	if len(readers) == 0 {
		// Fallback to writer if no readers available
		logging.Warn("No healthy readers available, falling back to writer")
		return cm.Writer()
	}

	// Select reader based on strategy
	var selectedNode *DatabaseNode
	switch cm.config.ReadBalanceStrategy {
	case "random":
		selectedNode = readers[rand.Intn(len(readers))]

	case "priority":
		// Use lowest priority number (highest priority)
		selectedNode = readers[0] // readers are sorted by priority

	case "least_connections":
		selectedNode = cm.selectLeastConnectedReader(readers)

	case "round_robin":
		fallthrough
	default:
		index := atomic.AddInt64(&cm.readIndex, 1) % int64(len(readers))
		selectedNode = readers[index]
	}

	atomic.AddInt64(&selectedNode.readCount, 1)
	return selectedNode.sqlDB, nil
}

// writeHintKey is the unexported key carrying the reader/writer hint. It was
// previously the bare string "database_write", which any other package could
// have collided with.
type writeHintKey struct{}

// DB returns the writer when ctx carries a write hint from WithWriter, and a
// read replica otherwise.
func (cm *ClusterManager) DB(ctx context.Context) (*sql.DB, error) {
	if isWrite, ok := ctx.Value(writeHintKey{}).(bool); ok && isWrite {
		return cm.Writer()
	}

	// Default to reader for better load distribution
	return cm.Reader()
}

// WithWriter returns a context that routes DB to the writer.
func (cm *ClusterManager) WithWriter(ctx context.Context) context.Context {
	return context.WithValue(ctx, writeHintKey{}, true)
}

// WithReader returns a context that routes DB to a read replica.
func (cm *ClusterManager) WithReader(ctx context.Context) context.Context {
	return context.WithValue(ctx, writeHintKey{}, false)
}

// getHealthyReaders returns all healthy reader nodes sorted by priority
func (cm *ClusterManager) getHealthyReaders() []*DatabaseNode {
	var readers []*DatabaseNode

	for _, node := range cm.nodes {
		node.mutex.RLock()
		if (node.Role == RoleReader || node.Role == RoleWriter) &&
			node.Health == HealthHealthy && node.sqlDB != nil {
			readers = append(readers, node)
		}
		node.mutex.RUnlock()
	}

	// Sort by priority (lower number = higher priority)
	for i := 0; i < len(readers)-1; i++ {
		for j := i + 1; j < len(readers); j++ {
			if readers[i].Priority > readers[j].Priority {
				readers[i], readers[j] = readers[j], readers[i]
			}
		}
	}

	return readers
}

// selectLeastConnectedReader selects reader with lowest connection count
func (cm *ClusterManager) selectLeastConnectedReader(readers []*DatabaseNode) *DatabaseNode {
	if len(readers) == 0 {
		return nil
	}

	selected := readers[0]
	minConnections := atomic.LoadInt64(&selected.readCount)

	for _, reader := range readers[1:] {
		connections := atomic.LoadInt64(&reader.readCount)
		if connections < minConnections {
			selected = reader
			minConnections = connections
		}
	}

	return selected
}

// hasHealthyNodes checks if any nodes are healthy
func (cm *ClusterManager) hasHealthyNodes() bool {
	for _, node := range cm.nodes {
		node.mutex.RLock()
		healthy := node.Health == HealthHealthy
		node.mutex.RUnlock()
		if healthy {
			return true
		}
	}
	return false
}

// countHealthyNodes returns the number of healthy nodes
func (cm *ClusterManager) countHealthyNodes() int {
	count := 0
	for _, node := range cm.nodes {
		node.mutex.RLock()
		if node.Health == HealthHealthy {
			count++
		}
		node.mutex.RUnlock()
	}
	return count
}

// isCircuitOpen checks if the circuit breaker is open
func (cm *ClusterManager) isCircuitOpen() bool {
	if !cm.circuitOpen {
		return false
	}

	// Check if circuit breaker timeout has passed
	if time.Since(cm.circuitOpenTime) > cm.config.CircuitBreakerTimeout {
		cm.circuitOpen = false
		logging.Info("Circuit breaker closed - attempting recovery")
		return false
	}

	return true
}

// openCircuitBreaker opens the circuit breaker
func (cm *ClusterManager) openCircuitBreaker() {
	if !cm.circuitOpen {
		cm.circuitOpen = true
		cm.circuitOpenTime = time.Now()
		logging.Warn("Circuit breaker opened due to database failures")
	}
}

// Close gracefully shuts down all connections. Every node is closed even if
// an earlier one failed; the failures are joined into the returned error
// rather than discarded, which is what this method used to do.
func (cm *ClusterManager) Close() error {
	// Stop background tasks
	cm.tombManager.Shutdown()

	var errs []error
	for _, node := range cm.nodes {
		node.mutex.Lock()
		if node.sqlDB != nil {
			if err := node.sqlDB.Close(); err != nil {
				errs = append(errs, fmt.Errorf("closing %s: %w", node.Name, err))
			}
		}
		node.mutex.Unlock()
	}

	logging.Info("Database cluster manager closed")
	return errors.Join(errs...)
}
