package rediscluster

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dobrevit/svckit/logging"
	"github.com/redis/go-redis/v9"
)

// NodeState represents the health state of a Redis node
type NodeState int

const (
	NodeHealthy NodeState = iota
	NodeDegraded
	NodeFailed
)

func (s NodeState) String() string {
	switch s {
	case NodeHealthy:
		return "healthy"
	case NodeDegraded:
		return "degraded"
	default:
		return "failed"
	}
}

// RedisNode represents a single Redis instance
type RedisNode struct {
	URL    string
	Name   string
	client *redis.Client
	state  NodeState

	// Health tracking
	lastError   error
	lastCheck   time.Time
	lastLatency time.Duration

	// Metrics
	opsCount   int64
	errorCount int64

	mutex sync.RWMutex
}

// ClusterConfig holds configuration for the Redis cluster adapter
type ClusterConfig struct {
	NodeURLs    []string // Redis node URLs
	ServiceName string   // Service identifier
	Password    string   // Redis password (if required)

	// Connection pooling
	MaxPoolSize     int           // Maximum connections per node
	MinIdleConns    int           // Minimum idle connections
	MaxIdleTime     time.Duration // Max time a connection can be idle
	ConnMaxLifetime time.Duration // Max lifetime of a connection

	// Health and retry settings
	HealthCheckInterval time.Duration // How often to check node health
	HealthCheckTimeout  time.Duration // Timeout for health checks
	MaxRetries          int           // Max retry attempts
	RetryBackoffBase    time.Duration // Base backoff duration
	RetryBackoffMax     time.Duration // Max backoff duration

	// Circuit breaker
	CircuitBreakerThreshold int           // Failures before marking node as failed
	CircuitBreakerTimeout   time.Duration // Time before retrying failed node

	// Load balancing
	LoadBalanceStrategy string // "round_robin", "random", "hash"
}

// ClusterAdapter manages connections to multiple Redis nodes
type ClusterAdapter struct {
	config    ClusterConfig
	nodes     []*RedisNode
	nodeIndex int64 // For round-robin load balancing

	// Circuit breaker tracking
	failureWindow time.Duration
	recoveryTime  time.Duration

	// Background task management
	tombManager *TombManager

	// Primary client for compatibility
	primaryClient *redis.Client

	mutex sync.RWMutex
}

// NewClusterAdapter creates a new multi-node Redis adapter
func NewClusterAdapter(config *ClusterConfig) (*ClusterAdapter, error) {
	if len(config.NodeURLs) == 0 {
		return nil, errors.New("at least one Redis node URL is required")
	}

	// Initialize global logger
	logging.InitializeGlobalLogger(config.ServiceName)

	// Validate configuration
	adapter := &ClusterAdapter{
		config:        *config,
		nodes:         make([]*RedisNode, len(config.NodeURLs)),
		failureWindow: 5 * time.Minute,
		recoveryTime:  30 * time.Second,
		tombManager:   NewTombManager(fmt.Sprintf("redis-cluster-%s", config.ServiceName)),
	}

	// Initialize nodes
	for i, nodeURL := range config.NodeURLs {
		adapter.nodes[i] = &RedisNode{
			URL:       nodeURL,
			Name:      fmt.Sprintf("node-%d", i),
			state:     NodeFailed, // Start as failed, health check will update
			lastCheck: time.Now(),
		}
	}

	// Connect to all nodes
	if err := adapter.connectToAllNodes(); err != nil {
		return nil, fmt.Errorf("failed to connect to any Redis nodes: %w", err)
	}

	// Set primary client for compatibility
	if healthyNode := adapter.getFirstHealthyNode(); healthyNode != nil {
		adapter.primaryClient = healthyNode.client
	}

	// Start health monitoring
	adapter.startHealthMonitoring()

	logging.Info("Redis cluster adapter initialized with %d nodes", len(adapter.nodes))

	return adapter, nil
}

// connectToAllNodes attempts to connect to all configured nodes
func (ca *ClusterAdapter) connectToAllNodes() error {
	var lastErr error
	healthyNodes := 0

	for _, node := range ca.nodes {
		if err := ca.connectToNode(node); err != nil {
			logging.Error("Failed to connect to Redis node %s: %v", node.Name, err)
			lastErr = err
		} else {
			healthyNodes++
		}
	}

	if healthyNodes == 0 {
		return fmt.Errorf("failed to connect to any nodes, last error: %w", lastErr)
	}

	logging.Info("Connected to %d/%d Redis nodes", healthyNodes, len(ca.nodes))
	return nil
}

// connectToNode establishes connection to a single node
func (ca *ClusterAdapter) connectToNode(node *RedisNode) error {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	// Close existing connection
	if node.client != nil {
		_ = node.client.Close() // replacing this client; nothing to do if it objects
	}

	// Parse URL and create options
	opt, err := redis.ParseURL(node.URL)
	if err != nil {
		node.state = NodeFailed
		node.lastError = err
		node.lastCheck = time.Now()
		return fmt.Errorf("failed to parse Redis URL: %w", err)
	}

	// Override password if provided in config
	if ca.config.Password != "" {
		opt.Password = ca.config.Password
	}

	// Apply connection pool settings
	opt.PoolSize = ca.config.MaxPoolSize
	opt.MinIdleConns = ca.config.MinIdleConns
	opt.ConnMaxLifetime = ca.config.ConnMaxLifetime
	// v8 spelled this MaxConnAge, and MaxIdleTime was being applied to
	// PoolTimeout -- how long to wait for a free connection, which is a
	// different thing entirely. v9's ConnMaxIdleTime is what the setting
	// always meant.
	opt.ConnMaxIdleTime = ca.config.MaxIdleTime
	opt.MaxRetries = ca.config.MaxRetries

	// Create client
	client := redis.NewClient(opt)

	// Test connection with timeout
	ctx, cancel := context.WithTimeout(context.Background(), ca.config.HealthCheckTimeout)
	defer cancel()

	start := time.Now()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close() // discarding a client that cannot reach the server
		node.state = NodeFailed
		node.lastError = err
		node.lastCheck = time.Now()
		atomic.AddInt64(&node.errorCount, 1)
		return fmt.Errorf("failed to ping Redis node: %w", err)
	}

	// Update node state
	node.client = client
	node.state = NodeHealthy
	node.lastError = nil
	node.lastCheck = time.Now()
	node.lastLatency = time.Since(start)

	logging.Info("Successfully connected to Redis node: %s (latency: %v)", node.Name, node.lastLatency)
	return nil
}

// Client returns a go-redis compatible client
func (ca *ClusterAdapter) Client() *redis.Client {
	ca.mutex.RLock()
	defer ca.mutex.RUnlock()

	// Return primary client for compatibility
	return ca.primaryClient
}

// GetHealthyNode returns a healthy node based on load balancing strategy
func (ca *ClusterAdapter) GetHealthyNode() *RedisNode {
	return ca.selectNode("")
}

// GetNodeByKey returns a node based on key (for hash-based routing)
func (ca *ClusterAdapter) GetNodeByKey(key string) *RedisNode {
	return ca.selectNode(key)
}

// selectNode selects a node based on the load balancing strategy
func (ca *ClusterAdapter) selectNode(key string) *RedisNode {
	healthy := ca.getHealthyNodes()
	if len(healthy) == 0 {
		return nil
	}

	switch ca.config.LoadBalanceStrategy {
	case "random":
		return healthy[rand.Intn(len(healthy))]

	case "hash":
		if key != "" {
			h := fnv.New32a()
			h.Write([]byte(key))
			return healthy[h.Sum32()%uint32(len(healthy))]
		}
		// Fall through to round robin if no key
		fallthrough

	case "round_robin":
		fallthrough
	default:
		index := atomic.AddInt64(&ca.nodeIndex, 1) % int64(len(healthy))
		return healthy[index]
	}
}

// getHealthyNodes returns all currently healthy nodes
func (ca *ClusterAdapter) getHealthyNodes() []*RedisNode {
	ca.mutex.RLock()
	defer ca.mutex.RUnlock()

	var healthy []*RedisNode
	for _, node := range ca.nodes {
		node.mutex.RLock()
		if node.state == NodeHealthy && node.client != nil {
			healthy = append(healthy, node)
		}
		node.mutex.RUnlock()
	}
	return healthy
}

// getFirstHealthyNode returns the first healthy node
func (ca *ClusterAdapter) getFirstHealthyNode() *RedisNode {
	healthy := ca.getHealthyNodes()
	if len(healthy) > 0 {
		return healthy[0]
	}
	return nil
}

// ExecuteWithRetry executes a Redis operation with retry and failover
func (ca *ClusterAdapter) ExecuteWithRetry(ctx context.Context, fn func(context.Context, *redis.Client) error, key ...string) error {
	var nodeKey string
	if len(key) > 0 {
		nodeKey = key[0]
	}

	// Track attempted nodes to avoid retrying the same failed node
	attemptedNodes := make(map[*RedisNode]bool)

	for attempt := 0; attempt <= ca.config.MaxRetries; attempt++ {
		// Select a node
		node := ca.selectNode(nodeKey)
		if node == nil {
			return errors.New("no healthy Redis nodes available")
		}

		// Skip if we already tried this node
		if attemptedNodes[node] {
			continue
		}
		attemptedNodes[node] = true

		// Execute operation
		err := ca.executeOnNode(ctx, node, fn)
		if err == nil {
			atomic.AddInt64(&node.opsCount, 1)
			return nil
		}

		// Handle different types of Redis errors appropriately
		if errors.Is(err, redis.Nil) {
			// Cache miss - this is normal, not an error worth logging
			return err
		}

		// Log actual errors (not cache misses)
		atomic.AddInt64(&node.errorCount, 1)
		logging.Warn("Redis operation failed on node %s (attempt %d/%d): %v",
			node.Name, attempt+1, ca.config.MaxRetries+1, err)

		// Check if node should be marked as degraded
		ca.checkNodeDegradation(node, err)

		// Backoff before retry
		if attempt < ca.config.MaxRetries {
			backoff := ca.calculateBackoff(attempt)
			select {
			case <-time.After(backoff):
				// Continue to next attempt
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return fmt.Errorf("redis operation failed after %d attempts", ca.config.MaxRetries+1)
}

// executeOnNode executes an operation on a specific node
func (ca *ClusterAdapter) executeOnNode(ctx context.Context, node *RedisNode, fn func(context.Context, *redis.Client) error) error {
	node.mutex.RLock()
	client := node.client
	node.mutex.RUnlock()

	if client == nil {
		return errors.New("node client is nil")
	}

	// Execute with timeout
	execCtx, cancel := context.WithTimeout(ctx, ca.config.HealthCheckTimeout)
	defer cancel()

	// v8 applied the timeout with client.WithContext, which the commands
	// ignored because each one takes a context of its own. Handing execCtx to
	// the callback is what makes HealthCheckTimeout actually bound the call.
	return fn(execCtx, client)
}

// checkNodeDegradation checks if a node should be marked as degraded
func (ca *ClusterAdapter) checkNodeDegradation(node *RedisNode, err error) {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	errorCount := atomic.LoadInt64(&node.errorCount)
	if errorCount >= int64(ca.config.CircuitBreakerThreshold) {
		if node.state == NodeHealthy {
			node.state = NodeDegraded
			logging.Warn("Redis node %s marked as degraded due to %d errors", node.Name, errorCount)
		}
	}
}

// calculateBackoff calculates exponential backoff duration
func (ca *ClusterAdapter) calculateBackoff(attempt int) time.Duration {
	backoff := time.Duration(1<<uint(attempt)) * ca.config.RetryBackoffBase
	if backoff > ca.config.RetryBackoffMax {
		backoff = ca.config.RetryBackoffMax
	}
	// Add jitter
	jitter := time.Duration(rand.Int63n(int64(backoff / 4)))
	return backoff + jitter
}

// GetClusterHealth returns health information for all nodes
func (ca *ClusterAdapter) GetClusterHealth() ClusterHealth {
	ca.mutex.RLock()
	defer ca.mutex.RUnlock()

	health := ClusterHealth{
		Nodes:      make([]NodeHealth, len(ca.nodes)),
		TotalNodes: len(ca.nodes),
		Timestamp:  time.Now(),
	}

	for i, node := range ca.nodes {
		node.mutex.RLock()
		health.Nodes[i] = NodeHealth{
			Name:       node.Name,
			URL:        maskURL(node.URL),
			State:      node.state,
			LastError:  node.lastError,
			LastCheck:  node.lastCheck,
			Latency:    node.lastLatency,
			OpsCount:   atomic.LoadInt64(&node.opsCount),
			ErrorCount: atomic.LoadInt64(&node.errorCount),
		}
		if node.state == NodeHealthy {
			health.HealthyNodes++
		}
		node.mutex.RUnlock()
	}

	return health
}

// GetMetrics returns cluster metrics
func (ca *ClusterAdapter) GetMetrics() ClusterMetrics {
	metrics := ClusterMetrics{
		Timestamp: time.Now(),
	}

	var totalLatency time.Duration
	latencyCount := 0

	for _, node := range ca.nodes {
		node.mutex.RLock()
		metrics.TotalOperations += atomic.LoadInt64(&node.opsCount)
		metrics.FailedOperations += atomic.LoadInt64(&node.errorCount)
		if node.lastLatency > 0 {
			totalLatency += node.lastLatency
			latencyCount++
			if metrics.MaxLatency < node.lastLatency {
				metrics.MaxLatency = node.lastLatency
			}
			if metrics.MinLatency == 0 || metrics.MinLatency > node.lastLatency {
				metrics.MinLatency = node.lastLatency
			}
		}
		node.mutex.RUnlock()
	}

	if latencyCount > 0 {
		metrics.AverageLatency = totalLatency / time.Duration(latencyCount)
	}

	return metrics
}

// MarkNodeFailed manually marks a node as failed
func (ca *ClusterAdapter) MarkNodeFailed(nodeURL string) {
	ca.mutex.RLock()
	defer ca.mutex.RUnlock()

	for _, node := range ca.nodes {
		if node.URL == nodeURL {
			node.mutex.Lock()
			node.state = NodeFailed
			logging.Info("Manually marked Redis node %s as failed", node.Name)
			node.mutex.Unlock()
			break
		}
	}
}

// GetNodeByIndex returns a specific node by index (for debugging)
func (ca *ClusterAdapter) GetNodeByIndex(index int) *RedisNode {
	ca.mutex.RLock()
	defer ca.mutex.RUnlock()

	if index >= 0 && index < len(ca.nodes) {
		return ca.nodes[index]
	}
	return nil
}

// Close gracefully shuts down all connections. Every node is closed even if
// an earlier one failed; the failures are joined into the returned error
// rather than discarded, which is what this method used to do.
func (ca *ClusterAdapter) Close() error {
	// Stop background tasks
	ca.tombManager.Shutdown()

	var errs []error
	for _, node := range ca.nodes {
		node.mutex.Lock()
		if node.client != nil {
			if err := node.client.Close(); err != nil {
				errs = append(errs, fmt.Errorf("closing %s: %w", node.Name, err))
			}
		}
		node.mutex.Unlock()
	}

	logging.Info("Redis cluster adapter closed")
	return errors.Join(errs...)
}

// ClusterHealth represents the health status of the cluster
type ClusterHealth struct {
	Nodes        []NodeHealth
	TotalNodes   int
	HealthyNodes int
	Timestamp    time.Time
}

// NodeHealth represents health information for a single node
type NodeHealth struct {
	Name       string
	URL        string
	State      NodeState
	LastError  error
	LastCheck  time.Time
	Latency    time.Duration
	OpsCount   int64
	ErrorCount int64
}

// ClusterMetrics represents operational metrics for the cluster
type ClusterMetrics struct {
	TotalOperations  int64
	FailedOperations int64
	AverageLatency   time.Duration
	MinLatency       time.Duration
	MaxLatency       time.Duration
	Timestamp        time.Time
}
