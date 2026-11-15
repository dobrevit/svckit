package amqpcluster

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dobrevit/svckit/internal/amqpconfirm"
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
	confirmer   *amqpconfirm.Publisher // on publishCh, with PublisherConfirms
	state       NodeState
	lastError   error
	lastCheck   time.Time
	mutex       sync.RWMutex

	// consumeMu serialises starting consumers on this node, and guards
	// consumers: the channel each subscription is consuming on, so that a
	// subscription is never started twice on one channel.
	consumeMu sync.Mutex
	consumers map[*subscription]amqpChannel

	// connecting is set while a dial to this node is in flight, so that two
	// health checks never dial the same node at once. It is not guarded by
	// mutex, because the dial happens outside it.
	connecting atomic.Bool

	// windowStart is when the current error window began; the health check
	// closes the window and starts a new one. Guarded by mutex.
	windowStart time.Time

	// Metrics
	publishCount   int64
	subscribeCount int64
	errorCount     int64 // every error since the node was created
	windowErrors   int64 // errors since windowStart; decides recovery
}

// ClusterConfig holds configuration for the RabbitMQ cluster adapter
type ClusterConfig struct {
	Nodes                   []string      // RabbitMQ node URLs
	ServiceName             string        // Service identifier
	HealthCheckInterval     time.Duration // How often to check node health
	RetryAttempts           int           // Max retry attempts for publishing
	CircuitBreakerThreshold int           // Failures before marking node as failed
	LoadBalanceStrategy     string        // "round_robin", "random", "hash"

	// DegradedErrorRate is the error rate, in errors per minute, below which a
	// degraded node is returned to service. A node is degraded by a failed
	// publish; each health check measures the errors since the previous one,
	// and a node whose connection and channel are open and whose rate over
	// that window is below this value is healthy again. Zero means 1, so with
	// the default 30-second interval a degraded node recovers after one
	// health interval without errors.
	DegradedErrorRate float64

	// PublisherConfirms puts every publish channel in confirm mode, so that
	// Publish returns only once the broker has confirmed the message: nil
	// means the broker stored it. Messages are published mandatory, so one
	// no queue is bound to receive comes back as eventbus.ErrUnroutable;
	// a refusal is eventbus.ErrNacked, and no answer within ConfirmTimeout
	// eventbus.ErrConfirmTimeout. Only a publish that never reached a broker
	// fails over to another node: a nacked or returned message reached one
	// that said no, and a timed-out one may have been stored.
	PublisherConfirms bool

	// ConfirmTimeout bounds the wait for a confirmation; zero means five
	// seconds.
	ConfirmTimeout time.Duration
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

	// now is the clock; nil means time.Now. Tests replace it.
	now func() time.Time

	// registry holds the subscriptions and declarations restored on a node
	// after it connects.
	registry registry

	// forwarders counts the goroutines moving deliveries from a node to a
	// subscription; forwardersMu orders adding one against Close's wait.
	forwarders   sync.WaitGroup
	forwardersMu sync.Mutex

	stop      chan struct{} // closed by Close; see stopping
	stopOnce  sync.Once
	closeOnce sync.Once

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
	if config.DegradedErrorRate <= 0 {
		config.DegradedErrorRate = 1
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
			URL:         nodeURL,
			Name:        fmt.Sprintf("node-%d", i),
			state:       NodeFailed, // Start as failed, health check will update
			lastCheck:   adapter.clock(),
			windowStart: adapter.clock(),
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

	conn, publishCh, confirmer, subscribeCh, err := ca.openNode(node)

	node.mutex.Lock()
	if err == nil && ca.closed.Load() {
		err = errors.New("the adapter is closed")
		_ = conn.Close() // never installed, so nothing else will close it
	}
	if err != nil {
		node.state = NodeFailed
		node.lastError = err
		node.lastCheck = ca.clock()
		node.mutex.Unlock()
		atomic.AddInt64(&node.errorCount, 1)
		return err
	}
	previous := node.conn
	node.conn = conn
	node.publishCh = publishCh
	node.confirmer = confirmer
	node.subscribeCh = subscribeCh
	node.state = NodeHealthy
	node.lastError = nil
	node.lastCheck = ca.clock()
	// A new connection starts with a clean window: errors on the old one say
	// nothing about it.
	node.windowStart = node.lastCheck
	atomic.StoreInt64(&node.windowErrors, 0)
	node.mutex.Unlock()

	// Closing a connection closes its channels too.
	if previous != nil && !previous.IsClosed() {
		_ = previous.Close() // replaced; nothing to do if it objects
	}

	logging.Info("Successfully connected to RabbitMQ node: %s", node.Name)

	ca.restoreNode(node)
	return nil
}

// openNode dials a node and opens its publish channel (in confirm mode, with
// its confirmer, when PublisherConfirms is set) and its subscribe channel.
// It touches no node state, so it needs no lock.
func (ca *ClusterAdapter) openNode(node *Node) (amqpConnection, amqpChannel, *amqpconfirm.Publisher, amqpChannel, error) {
	dial := ca.dial
	if dial == nil {
		dial = dialBroker
	}

	conn, err := dial(node.URL)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to connect to %s: %w", maskCredentials(node.URL), err)
	}

	publishCh, confirmer, err := ca.openPublishChannel(conn)
	if err != nil {
		_ = conn.Close() // the connection is unusable without a channel
		return nil, nil, nil, nil, fmt.Errorf("failed to create publish channel for %s: %w", maskCredentials(node.URL), err)
	}

	subscribeCh, err := conn.Channel()
	if err != nil {
		_ = conn.Close() // closes publishCh with it
		return nil, nil, nil, nil, fmt.Errorf("failed to create subscribe channel for %s: %w", maskCredentials(node.URL), err)
	}

	return conn, publishCh, confirmer, subscribeCh, nil
}

// openPublishChannel opens a publish channel on conn and, with
// PublisherConfirms, puts it in confirm mode. A confirmer belongs to one
// channel, so every new publish channel gets its own.
func (ca *ClusterAdapter) openPublishChannel(conn amqpConnection) (amqpChannel, *amqpconfirm.Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}
	if !ca.config.PublisherConfirms {
		return ch, nil, nil
	}
	confirmer, err := amqpconfirm.New(ch, ca.config.ConfirmTimeout)
	if err != nil {
		_ = ch.Close()
		return nil, nil, err
	}
	return ch, confirmer, nil
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
	ca.registry.remember(declaration{
		key:         "exchange " + name,
		onPublishCh: true,
		apply: func(ch amqpChannel) error {
			return ch.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
		},
	})

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
	ca.registry.remember(declaration{
		key: "queue " + name,
		apply: func(ch amqpChannel) error {
			_, err := ch.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
			return err
		},
	})

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
	ca.registry.remember(declaration{
		key: fmt.Sprintf("binding %s to %s with %q", queueName, exchangeName, routingKey),
		apply: func(ch amqpChannel) error {
			return ch.QueueBind(queueName, routingKey, exchangeName, noWait, args)
		},
	})

	logging.Info("Queue %s bound to exchange %s on %d/%d nodes", queueName, exchangeName, successCount, len(healthy))
	return nil
}

// Publish publishes a message to one of the healthy nodes with failover. It
// is PublishWithContext without a context.
func (ca *ClusterAdapter) Publish(exchange, routingKey string, mandatory, immediate bool, msg amqp.Publishing) error {
	return ca.PublishWithContext(context.Background(), exchange, routingKey, mandatory, immediate, msg)
}

// PublishWithContext publishes a message to one of the healthy nodes, chosen
// by the load-balancing strategy, and fails over to the others. With
// PublisherConfirms it waits for the broker's confirmation (see
// ClusterConfig.PublisherConfirms), publishes mandatory whatever mandatory
// says, and fails over only a publish that never reached a broker.
func (ca *ClusterAdapter) PublishWithContext(ctx context.Context, exchange, routingKey string, mandatory, immediate bool, msg amqp.Publishing) error {
	tried := make(map[*Node]bool)
	var lastErr error

	attempt := func(node *Node) (done bool) {
		tried[node] = true
		err := ca.publishToNode(ctx, node, exchange, routingKey, mandatory, immediate, msg)
		lastErr = err
		if err == nil {
			atomic.AddInt64(&node.publishCount, 1)
			return true
		}
		return !failsOver(err)
	}

	// Try primary node selection first
	if node := ca.selectNodeForPublish(routingKey); node != nil {
		if attempt(node) {
			return lastErr
		}
	}

	// If primary failed, try all other healthy nodes
	for _, node := range ca.getHealthyNodes() {
		if tried[node] {
			continue
		}
		if attempt(node) {
			return lastErr
		}
	}

	if lastErr == nil {
		return errors.New("failed to publish message to any healthy node")
	}
	return fmt.Errorf("failed to publish message to any healthy node: %w", lastErr)
}

// failsOver reports whether a failed publish may be tried on another node.
// Without confirms every failure is a failure to send. With them, a nacked
// or returned message reached a broker that said no, and a timed-out one may
// have been stored, so sending it elsewhere would duplicate or override that
// answer; only a publish that never reached a broker fails over. A cancelled
// context ends the attempt.
func failsOver(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	switch {
	case errors.Is(err, amqpconfirm.ErrNacked),
		errors.Is(err, amqpconfirm.ErrUnroutable),
		errors.Is(err, amqpconfirm.ErrConfirmTimeout):
		return false
	}
	return true
}

// publishToNode publishes to a specific node. A failure that says the node
// is in trouble (the message could not be sent, or no confirmation came)
// counts against it and degrades it; a nack or a return is the broker
// working as intended and does not.
func (ca *ClusterAdapter) publishToNode(ctx context.Context, node *Node, exchange, routingKey string, mandatory, immediate bool, msg amqp.Publishing) error {
	node.mutex.RLock()
	ch := node.publishCh
	confirmer := node.confirmer
	node.mutex.RUnlock()

	if ch == nil {
		return errors.New("node channel is nil")
	}

	var err error
	if confirmer != nil {
		err = confirmer.Publish(ctx, exchange, routingKey, msg)
	} else {
		err = ch.Publish(exchange, routingKey, mandatory, immediate, msg)
	}
	if err == nil {
		return nil
	}

	if errors.Is(err, amqpconfirm.ErrNacked) || errors.Is(err, amqpconfirm.ErrUnroutable) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	atomic.AddInt64(&node.errorCount, 1)
	atomic.AddInt64(&node.windowErrors, 1)
	// Mark node as degraded if it fails
	node.mutex.Lock()
	if node.state == NodeHealthy {
		node.state = NodeDegraded
		logging.Warn("Node %s marked as degraded due to publish error: %v", node.Name, err)
	}
	node.mutex.Unlock()
	return err
}

// Subscribe consumes messages from a queue on all healthy nodes, and keeps
// consuming: a node that reconnects, or that was down when Subscribe was
// called and connects later, starts consuming again into the same channel.
// The channel is closed by Close. No prefetch limit is set; ClusterSubscriber
// applies SubscriptionOptions.PrefetchCount.
func (ca *ClusterAdapter) Subscribe(queueName, consumerTag string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return ca.subscribe(&subscription{
		queue:       queueName,
		consumerTag: consumerTag,
		autoAck:     autoAck,
		exclusive:   exclusive,
		noLocal:     noLocal,
		noWait:      noWait,
		args:        args,
	})
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
	ca.closeOnce.Do(func() {
		ca.stopping() // makes ca.stop if nothing has yet
		close(ca.stop)
	})

	// Stop background tasks using tomb manager. This waits for any health
	// check, and so any reconnect, in flight.
	if ca.tombManager != nil {
		ca.tombManager.Shutdown()
	}

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

	// Closing the connections closed every consumer's deliveries, and
	// stopping released any forwarder blocked on a full subscription. Once
	// they have all returned nothing sends on a subscription channel, so it
	// is safe to close them, which ends a range over one.
	ca.forwardersMu.Lock()
	ca.forwardersMu.Unlock() //nolint:staticcheck // orders Close's Wait after any forwarder added before closed was set
	ca.forwarders.Wait()
	_, subscriptions := ca.registry.snapshot()
	for _, s := range subscriptions {
		close(s.out)
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
	switch ca.inspectNode(node) {
	case reconnectNode:
		logging.Info("Node %s connection is closed, attempting reconnection", node.Name)
		if err := ca.connectToNode(node); err != nil && !errors.Is(err, errConnectInProgress) {
			logging.Error("Failed to reconnect to node %s: %v", node.Name, err)
		}
	case restoreConsumers:
		ca.restoreNode(node)
	}
}

// healthAction is what a health check has to do after it releases the lock.
type healthAction int

const (
	nothingToDo healthAction = iota
	reconnectNode
	restoreConsumers // the subscribe channel was recreated, and its consumers with it
)

// inspectNode does the part of a health check that needs the node lock, and
// reports what remains to be done without it.
func (ca *ClusterAdapter) inspectNode(node *Node) (action healthAction) {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	now := ca.clock()

	// Skip a node checked within the last half interval: connectToNode records
	// a check as well, and a node that has just connected needs no second look.
	if now.Sub(node.lastCheck) < ca.config.HealthCheckInterval/2 {
		return nothingToDo
	}
	node.lastCheck = now

	// Close the error window. The rate that decides recovery is over the time
	// since the previous check; dividing by the time since lastCheck, after
	// setting it to now, is what used to keep a degraded node degraded forever.
	window := now.Sub(node.windowStart)
	windowErrors := atomic.SwapInt64(&node.windowErrors, 0)
	node.windowStart = now

	if node.conn == nil || node.conn.IsClosed() {
		return reconnectNode
	}

	// A channel the broker closed (a publish to a missing exchange does that)
	// leaves the connection open; recreate it rather than wait for the
	// connection to drop.
	if node.publishCh == nil || node.publishCh.IsClosed() {
		logging.Warn("Node %s publish channel is closed, recreating", node.Name)
		ch, confirmer, err := ca.openPublishChannel(node.conn)
		if err != nil {
			logging.Error("Failed to recreate publish channel for node %s: %v", node.Name, err)
			node.state = NodeFailed
			node.lastError = err
			return nothingToDo
		}
		node.publishCh = ch
		node.confirmer = confirmer
	}

	// The same for the subscribe channel, whose consumers closed with it.
	if node.subscribeCh == nil || node.subscribeCh.IsClosed() {
		logging.Warn("Node %s subscribe channel is closed, recreating", node.Name)
		ch, err := node.conn.Channel()
		if err != nil {
			logging.Error("Failed to recreate subscribe channel for node %s: %v", node.Name, err)
			node.state = NodeFailed
			node.lastError = err
			return nothingToDo
		}
		node.subscribeCh = ch
		action = restoreConsumers
	}

	switch node.state {
	case NodeFailed:
		node.state = NodeHealthy
		logging.Info("Node %s recovered", node.Name)
	case NodeDegraded:
		if errorsPerMinute(windowErrors, window) < ca.degradedErrorRate() {
			node.state = NodeHealthy
			logging.Info("Node %s upgraded from degraded to healthy", node.Name)
		}
	}
	return action
}

// errorsPerMinute is n errors over window as a rate. No errors is a rate of
// zero however short the window; errors over no time at all are an infinite
// rate, not a division by zero.
func errorsPerMinute(n int64, window time.Duration) float64 {
	if n == 0 {
		return 0
	}
	if window <= 0 {
		return math.Inf(1)
	}
	return float64(n) / window.Minutes()
}

func (ca *ClusterAdapter) degradedErrorRate() float64 {
	if ca.config.DegradedErrorRate > 0 {
		return ca.config.DegradedErrorRate
	}
	return 1
}

func (ca *ClusterAdapter) clock() time.Time {
	if ca.now != nil {
		return ca.now()
	}
	return time.Now()
}
