package amqpcluster

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
	amqp "github.com/rabbitmq/amqp091-go"
)

// NodeState represents the health state of a RabbitMQ node
type NodeState int

const (
	NodeHealthy NodeState = iota
	NodeDegraded
	NodeFailed
)

// Node represents a single RabbitMQ instance
type Node struct {
	URL         string
	Name        string
	conn        amqpConnection
	publishCh   amqpChannel
	subscribeCh amqpChannel
	state       NodeState
	lastError   error
	lastCheck   time.Time
	mutex       sync.RWMutex

	// connecting is set while a dial to this node is in flight, so that two
	// health checks never dial the same node at once. It is not guarded by
	// mutex, because the dial happens outside it.
	connecting atomic.Bool

	// Metrics
	publishCount   int64
	subscribeCount int64
	errorCount     int64
}

// ClusterConfig holds configuration for the RabbitMQ cluster adapter
type ClusterConfig struct {
	Nodes                   []string      // RabbitMQ node URLs
	ServiceName             string        // Service identifier
	HealthCheckInterval     time.Duration // How often to check node health
	RetryAttempts           int           // Max retry attempts for publishing
	CircuitBreakerThreshold int           // Failures before marking node as failed
	LoadBalanceStrategy     string        // "round_robin", "random", "hash"
}

// ClusterAdapter manages connections to multiple RabbitMQ nodes
type ClusterAdapter struct {
	config    ClusterConfig
	nodes     []*Node
	nodeIndex int64 // For round-robin load balancing

	// Circuit breaker tracking
	failureWindow time.Duration
	recoveryTime  time.Duration

	// Background task management
	tombManager *TombManager

	// dial opens a node's connection; nil means dialBroker. Tests replace it.
	dial dialFunc

	// closed is set by Close, so that a reconnect that finishes afterwards
	// does not install a connection nothing will ever close.
	closed atomic.Bool

	mutex sync.RWMutex
}

// NewClusterAdapter creates a new multi-node RabbitMQ adapter
func NewClusterAdapter(config ClusterConfig) (*ClusterAdapter, error) {
	if len(config.Nodes) == 0 {
		return nil, errors.New("at least one RabbitMQ node URL is required")
	}

	// Initialize global logger
	logging.InitializeGlobalLogger(config.ServiceName)

	// Set defaults
	if config.HealthCheckInterval == 0 {
		config.HealthCheckInterval = 30 * time.Second
	}
	if config.RetryAttempts == 0 {
		config.RetryAttempts = 3
	}
	if config.CircuitBreakerThreshold == 0 {
		config.CircuitBreakerThreshold = 5
	}
	if config.LoadBalanceStrategy == "" {
		config.LoadBalanceStrategy = "round_robin"
	}

	adapter := &ClusterAdapter{
		config:        config,
		nodes:         make([]*Node, len(config.Nodes)),
		failureWindow: 5 * time.Minute,
		recoveryTime:  30 * time.Second,
		tombManager:   NewTombManager(fmt.Sprintf("cluster-adapter-%s", config.ServiceName)),
		dial:          dialBroker,
	}

	// Initialize nodes
	for i, nodeURL := range config.Nodes {
		adapter.nodes[i] = &Node{
			URL:       nodeURL,
			Name:      fmt.Sprintf("node-%d", i),
			state:     NodeFailed, // Start as failed, health check will update
			lastCheck: time.Now(),
		}
	}

	// Connect to all nodes
	if err := adapter.connectToAllNodes(); err != nil {
		return nil, fmt.Errorf("failed to connect to any RabbitMQ nodes: %w", err)
	}

	// Start health monitoring
	adapter.startHealthMonitoring()

	return adapter, nil
}

// connectToAllNodes attempts to connect to all configured nodes
func (ca *ClusterAdapter) connectToAllNodes() error {
	var lastErr error
	healthyNodes := 0

	for _, node := range ca.nodes {
		if err := ca.connectToNode(node); err != nil {
			logging.Error("Failed to connect to node %s: %v", node.Name, err)
			lastErr = err
		} else {
			healthyNodes++
		}
	}

	if healthyNodes == 0 {
		return fmt.Errorf("failed to connect to any nodes, last error: %w", lastErr)
	}

	logging.Info("Connected to %d/%d RabbitMQ nodes", healthyNodes, len(ca.nodes))
	return nil
}

// errConnectInProgress is returned by connectToNode when another goroutine is
// already dialling the node.
var errConnectInProgress = errors.New("a connection attempt to this node is already in progress")

// connectToNode (re)establishes the connection to a single node.
//
// It must be called without node.mutex held. The dial runs outside the lock,
// because it can take as long as the TCP and AMQP handshakes, and
// getHealthyNodes, and with it every Publish, reads the node under that lock;
// only the swap of the new connection in is done under it. Taking the lock for
// the whole call, from a caller that already held it, is what used to deadlock
// the health check on the first closed connection.
func (ca *ClusterAdapter) connectToNode(node *Node) error {
	if !node.connecting.CompareAndSwap(false, true) {
		return errConnectInProgress
	}
	defer node.connecting.Store(false)

	conn, publishCh, subscribeCh, err := ca.openNode(node)

	node.mutex.Lock()
	if err == nil && ca.closed.Load() {
		err = errors.New("the adapter is closed")
		_ = conn.Close() // never installed, so nothing else will close it
	}
	if err != nil {
		node.state = NodeFailed
		node.lastError = err
		node.lastCheck = time.Now()
		node.mutex.Unlock()
		atomic.AddInt64(&node.errorCount, 1)
		return err
	}
	previous := node.conn
	node.conn = conn
	node.publishCh = publishCh
	node.subscribeCh = subscribeCh
	node.state = NodeHealthy
	node.lastError = nil
	node.lastCheck = time.Now()
	node.mutex.Unlock()

	// Closing a connection closes its channels too.
	if previous != nil && !previous.IsClosed() {
		_ = previous.Close() // replaced; nothing to do if it objects
	}

	logging.Info("Successfully connected to RabbitMQ node: %s", node.Name)
	return nil
}

// openNode dials a node and opens its publish and subscribe channels. It
// touches no node state, so it needs no lock.
func (ca *ClusterAdapter) openNode(node *Node) (amqpConnection, amqpChannel, amqpChannel, error) {
	dial := ca.dial
	if dial == nil {
		dial = dialBroker
	}

	conn, err := dial(node.URL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to connect to %s: %w", maskCredentials(node.URL), err)
	}

	publishCh, err := conn.Channel()
	if err != nil {
		_ = conn.Close() // the connection is unusable without a channel
		return nil, nil, nil, fmt.Errorf("failed to create publish channel for %s: %w", maskCredentials(node.URL), err)
	}

	subscribeCh, err := conn.Channel()
	if err != nil {
		_ = conn.Close() // closes publishCh with it
		return nil, nil, nil, fmt.Errorf("failed to create subscribe channel for %s: %w", maskCredentials(node.URL), err)
	}

	return conn, publishCh, subscribeCh, nil
}

// getHealthyNodes returns a list of currently healthy nodes
func (ca *ClusterAdapter) getHealthyNodes() []*Node {
	ca.mutex.RLock()
	defer ca.mutex.RUnlock()

	var healthy []*Node
	for _, node := range ca.nodes {
		node.mutex.RLock()
		if node.state == NodeHealthy {
			healthy = append(healthy, node)
		}
		node.mutex.RUnlock()
	}
	return healthy
}

// selectNodeForPublish selects a node for publishing based on load balancing strategy
func (ca *ClusterAdapter) selectNodeForPublish(routingKey string) *Node {
	healthy := ca.getHealthyNodes()
	if len(healthy) == 0 {
		return nil
	}

	switch ca.config.LoadBalanceStrategy {
	case "random":
		return healthy[rand.Intn(len(healthy))]

	case "hash":
		if routingKey != "" {
			h := fnv.New32a()
			h.Write([]byte(routingKey))
			return healthy[h.Sum32()%uint32(len(healthy))]
		}
		fallthrough // Fall back to round robin if no routing key

	case "round_robin":
		fallthrough
	default:
		index := atomic.AddInt64(&ca.nodeIndex, 1) % int64(len(healthy))
		return healthy[index]
	}
}

// DeclareExchange creates an exchange on all healthy nodes
func (ca *ClusterAdapter) DeclareExchange(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	healthy := ca.getHealthyNodes()
	if len(healthy) == 0 {
		return errors.New("no healthy nodes available for exchange declaration")
	}

	var lastErr error
	successCount := 0

	for _, node := range healthy {
		node.mutex.RLock()
		ch := node.publishCh
		node.mutex.RUnlock()

		if ch != nil {
			if err := ch.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args); err != nil {
				logging.Error("Failed to declare exchange %s on node %s: %v", name, node.Name, err)
				lastErr = err
				continue
			}
			successCount++
		}
	}

	if successCount == 0 {
		return fmt.Errorf("failed to declare exchange on any node: %w", lastErr)
	}

	logging.Info("Exchange %s declared on %d/%d nodes", name, successCount, len(healthy))
	return nil
}

// DeclareQueue declares a queue on all healthy nodes
func (ca *ClusterAdapter) DeclareQueue(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) error {
	healthy := ca.getHealthyNodes()
	if len(healthy) == 0 {
		return errors.New("no healthy nodes available for queue declaration")
	}

	var lastErr error
	successCount := 0

	for _, node := range healthy {
		node.mutex.RLock()
		ch := node.subscribeCh
		node.mutex.RUnlock()

		if ch != nil {
			if _, err := ch.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args); err != nil {
				logging.Error("Failed to declare queue %s on node %s: %v", name, node.Name, err)
				lastErr = err
				continue
			}
			successCount++
		}
	}

	if successCount == 0 {
		return fmt.Errorf("failed to declare queue on any node: %w", lastErr)
	}

	logging.Info("Queue %s declared on %d/%d nodes", name, successCount, len(healthy))
	return nil
}

// BindQueue binds a queue to an exchange on all healthy nodes
func (ca *ClusterAdapter) BindQueue(queueName, routingKey, exchangeName string, noWait bool, args amqp.Table) error {
	healthy := ca.getHealthyNodes()
	if len(healthy) == 0 {
		return errors.New("no healthy nodes available for queue binding")
	}

	var lastErr error
	successCount := 0

	for _, node := range healthy {
		node.mutex.RLock()
		ch := node.subscribeCh
		node.mutex.RUnlock()

		if ch != nil {
			if err := ch.QueueBind(queueName, routingKey, exchangeName, noWait, args); err != nil {
				logging.Error("Failed to bind queue %s on node %s: %v", queueName, node.Name, err)
				lastErr = err
				continue
			}
			successCount++
		}
	}

	if successCount == 0 {
		return fmt.Errorf("failed to bind queue on any node: %w", lastErr)
	}

	logging.Info("Queue %s bound to exchange %s on %d/%d nodes", queueName, exchangeName, successCount, len(healthy))
	return nil
}

// Publish publishes a message to one of the healthy nodes with failover
func (ca *ClusterAdapter) Publish(exchange, routingKey string, mandatory, immediate bool, msg amqp.Publishing) error {
	// Try primary node selection first
	node := ca.selectNodeForPublish(routingKey)
	if node != nil {
		if err := ca.publishToNode(node, exchange, routingKey, mandatory, immediate, msg); err == nil {
			atomic.AddInt64(&node.publishCount, 1)
			return nil
		}
	}

	// If primary failed, try all other healthy nodes
	healthy := ca.getHealthyNodes()
	for _, node := range healthy {
		if err := ca.publishToNode(node, exchange, routingKey, mandatory, immediate, msg); err == nil {
			atomic.AddInt64(&node.publishCount, 1)
			return nil
		}
	}

	return errors.New("failed to publish message to any healthy node")
}

// publishToNode publishes to a specific node
func (ca *ClusterAdapter) publishToNode(node *Node, exchange, routingKey string, mandatory, immediate bool, msg amqp.Publishing) error {
	node.mutex.RLock()
	ch := node.publishCh
	node.mutex.RUnlock()

	if ch == nil {
		return errors.New("node channel is nil")
	}

	if err := ch.Publish(exchange, routingKey, mandatory, immediate, msg); err != nil {
		atomic.AddInt64(&node.errorCount, 1)
		// Mark node as degraded if it fails
		node.mutex.Lock()
		if node.state == NodeHealthy {
			node.state = NodeDegraded
			logging.Warn("Node %s marked as degraded due to publish error: %v", node.Name, err)
		}
		node.mutex.Unlock()
		return err
	}

	return nil
}

// Subscribe consumes messages from all healthy nodes
func (ca *ClusterAdapter) Subscribe(queueName, consumerTag string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	healthy := ca.getHealthyNodes()
	if len(healthy) == 0 {
		return nil, errors.New("no healthy nodes available for subscription")
	}

	// Create a multiplexed channel that combines messages from all nodes
	multiplexed := make(chan amqp.Delivery, 100) // Buffer for better performance

	var successCount int
	for i, node := range healthy {
		node.mutex.RLock()
		ch := node.subscribeCh
		node.mutex.RUnlock()

		if ch != nil {
			// Create unique consumer tag per node
			nodeConsumerTag := fmt.Sprintf("%s-node-%d", consumerTag, i)

			msgs, err := ch.Consume(queueName, nodeConsumerTag, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				logging.Error("Failed to consume from node %s: %v", node.Name, err)
				continue
			}

			// Forward messages from this node to the multiplexed channel
			go func(node *Node, msgs <-chan amqp.Delivery) {
				for msg := range msgs {
					atomic.AddInt64(&node.subscribeCount, 1)
					multiplexed <- msg
				}
			}(node, msgs)

			successCount++
		}
	}

	if successCount == 0 {
		close(multiplexed)
		return nil, errors.New("failed to subscribe to any healthy node")
	}

	logging.Info("Subscribed to queue %s on %d/%d nodes", queueName, successCount, len(healthy))
	return multiplexed, nil
}

// GetNodeStats returns statistics for all nodes
func (ca *ClusterAdapter) GetNodeStats() map[string]NodeStats {
	ca.mutex.RLock()
	defer ca.mutex.RUnlock()

	stats := make(map[string]NodeStats)
	for _, node := range ca.nodes {
		node.mutex.RLock()
		stats[node.Name] = NodeStats{
			URL:            node.URL,
			State:          node.state,
			PublishCount:   atomic.LoadInt64(&node.publishCount),
			SubscribeCount: atomic.LoadInt64(&node.subscribeCount),
			ErrorCount:     atomic.LoadInt64(&node.errorCount),
			LastError:      node.lastError,
			LastCheck:      node.lastCheck,
		}
		node.mutex.RUnlock()
	}
	return stats
}

// NodeStats holds statistics for a single node
type NodeStats struct {
	URL            string
	State          NodeState
	PublishCount   int64
	SubscribeCount int64
	ErrorCount     int64
	LastError      error
	LastCheck      time.Time
}

// Close gracefully shuts down all connections
// Close gracefully shuts down all connections. Every node is closed even if
// an earlier one failed; the failures are joined into the returned error
// rather than discarded, which is what this method used to do.
func (ca *ClusterAdapter) Close() error {
	ca.closed.Store(true)

	// Stop background tasks using tomb manager. This waits for any health
	// check, and so any reconnect, in flight.
	ca.tombManager.Shutdown()

	var errs []error
	for _, node := range ca.nodes {
		node.mutex.Lock()
		if node.publishCh != nil {
			if err := node.publishCh.Close(); err != nil {
				errs = append(errs, fmt.Errorf("closing publish channel on %s: %w", node.Name, err))
			}
		}
		if node.subscribeCh != nil {
			if err := node.subscribeCh.Close(); err != nil {
				errs = append(errs, fmt.Errorf("closing subscribe channel on %s: %w", node.Name, err))
			}
		}
		if node.conn != nil && !node.conn.IsClosed() {
			if err := node.conn.Close(); err != nil {
				errs = append(errs, fmt.Errorf("closing connection to %s: %w", node.Name, err))
			}
		}
		node.mutex.Unlock()
	}

	logging.Info("RabbitMQ cluster adapter closed")
	return errors.Join(errs...)
}

// startHealthMonitoring begins periodic health checks of all nodes using tomb manager
func (ca *ClusterAdapter) startHealthMonitoring() {
	ca.tombManager.StartBackgroundTask(func(ctx context.Context) {
		ticker := time.NewTicker(ca.config.HealthCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ca.performHealthChecks()
			case <-ctx.Done():
				logging.Info("Health monitoring stopped for RabbitMQ cluster adapter")
				return
			}
		}
	}, "health-monitoring")
}

// performHealthChecks checks the health of all nodes and attempts recovery.
// The nodes are checked concurrently, so one slow dial does not delay the
// others, and the call returns when all are done, so Close (which waits for
// the health task) also waits for any reconnect it started.
func (ca *ClusterAdapter) performHealthChecks() {
	var wg sync.WaitGroup
	for _, node := range ca.nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ca.checkNodeHealth(node)
		}()
	}
	wg.Wait()
}

// checkNodeHealth checks and potentially recovers a single node. It holds
// node.mutex only while it inspects and updates the node; a reconnect happens
// after the lock is released (see connectToNode).
func (ca *ClusterAdapter) checkNodeHealth(node *Node) {
	if ca.inspectNode(node) {
		logging.Info("Node %s connection is closed, attempting reconnection", node.Name)
		if err := ca.connectToNode(node); err != nil && !errors.Is(err, errConnectInProgress) {
			logging.Error("Failed to reconnect to node %s: %v", node.Name, err)
		}
	}
}

// inspectNode does the part of a health check that needs the node lock, and
// reports whether the node's connection is gone and has to be re-established.
func (ca *ClusterAdapter) inspectNode(node *Node) (reconnect bool) {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	// Skip if we checked recently
	if time.Since(node.lastCheck) < 10*time.Second {
		return false
	}

	node.lastCheck = time.Now()

	// Test connection
	if node.conn == nil || node.conn.IsClosed() {
		return true
	}

	// Test channel health
	if node.publishCh == nil {
		logging.Warn("Node %s publish channel is nil, recreating", node.Name)
		if ch, err := node.conn.Channel(); err != nil {
			logging.Error("Failed to recreate publish channel for node %s: %v", node.Name, err)
			node.state = NodeFailed
		} else {
			node.publishCh = ch
			if node.state == NodeFailed {
				node.state = NodeHealthy
				logging.Info("Node %s recovered", node.Name)
			}
		}
	}

	// Update state based on recent error rate
	if node.state == NodeDegraded {
		// Attempt to upgrade degraded nodes back to healthy
		errorRate := float64(atomic.LoadInt64(&node.errorCount)) / float64(time.Since(node.lastCheck).Minutes())
		if errorRate < 1.0 { // Less than 1 error per minute
			node.state = NodeHealthy
			logging.Info("Node %s upgraded from degraded to healthy", node.Name)
		}
	}
	return false
}
