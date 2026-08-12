package rediscluster

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/dobrevit/svckit/logging"
)

// startHealthMonitoring begins periodic health checks of all nodes
func (ca *ClusterAdapter) startHealthMonitoring() {
	ca.tombManager.StartBackgroundTask(func(ctx context.Context) {
		ticker := time.NewTicker(ca.config.HealthCheckInterval)
		defer ticker.Stop()

		logging.Info("Starting Redis cluster health monitoring")

		for {
			select {
			case <-ticker.C:
				ca.performHealthChecks()
			case <-ctx.Done():
				logging.Info("Redis cluster health monitoring stopped")
				return
			}
		}
	}, "health-monitoring")
}

// performHealthChecks checks the health of all nodes
func (ca *ClusterAdapter) performHealthChecks() {
	logging.Debug("Performing Redis cluster health checks")

	for _, node := range ca.nodes {
		go ca.checkNodeHealth(node)
	}
}

// checkNodeHealth checks and potentially recovers a single node
func (ca *ClusterAdapter) checkNodeHealth(node *RedisNode) {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	// Skip if we checked recently
	if time.Since(node.lastCheck) < 10*time.Second {
		return
	}

	node.lastCheck = time.Now()

	// Test connection
	if node.client == nil {
		logging.Debug("Node %s has no client, attempting connection", node.Name)
		if err := ca.connectToNode(node); err != nil {
			logging.Warn("Failed to reconnect to node %s: %v", node.Name, err)
			return
		}
		return
	}

	// Ping test
	ctx, cancel := context.WithTimeout(context.Background(), ca.config.HealthCheckTimeout)
	defer cancel()

	start := time.Now()
	err := node.client.Ping(ctx).Err()
	latency := time.Since(start)

	if err != nil {
		atomic.AddInt64(&node.errorCount, 1)
		node.lastError = err

		// Mark as failed if it was healthy
		if node.state == NodeHealthy {
			node.state = NodeFailed
			logging.Error("Redis node %s health check failed: %v", node.Name, err)
		}

		// Attempt reconnection
		logging.Info("Attempting to reconnect to failed node %s", node.Name)
		_ = node.client.Close() // forcing a reconnect; the client is going away
		node.client = nil

		// Try to reconnect
		if err := ca.connectToNode(node); err != nil {
			logging.Error("Failed to reconnect to node %s: %v", node.Name, err)
		}
		return
	}

	// Update latency
	node.lastLatency = latency

	// Check if node should be upgraded from degraded to healthy
	switch node.state {
	case NodeDegraded:
		// Reset error count if enough time has passed
		if time.Since(node.lastCheck) > ca.config.CircuitBreakerTimeout {
			atomic.StoreInt64(&node.errorCount, 0)
			node.state = NodeHealthy
			logging.Info("Redis node %s recovered from degraded to healthy", node.Name)
		}
	case NodeFailed:
		// Node recovered from failed state
		node.state = NodeHealthy
		atomic.StoreInt64(&node.errorCount, 0)
		logging.Info("Redis node %s recovered from failed to healthy", node.Name)

		// Update primary client if needed
		ca.updatePrimaryClient()
	}
}

// updatePrimaryClient updates the primary client to a healthy node
func (ca *ClusterAdapter) updatePrimaryClient() {
	ca.mutex.Lock()
	defer ca.mutex.Unlock()

	// If current primary is healthy, keep it
	if ca.primaryClient != nil {
		for _, node := range ca.nodes {
			node.mutex.RLock()
			if node.client == ca.primaryClient && node.state == NodeHealthy {
				node.mutex.RUnlock()
				return
			}
			node.mutex.RUnlock()
		}
	}

	// Find a new healthy node
	for _, node := range ca.nodes {
		node.mutex.RLock()
		if node.state == NodeHealthy && node.client != nil {
			ca.primaryClient = node.client
			logging.Info("Updated primary Redis client to node %s", node.Name)
			node.mutex.RUnlock()
			return
		}
		node.mutex.RUnlock()
	}

	logging.Warn("No healthy Redis nodes available for primary client")
	ca.primaryClient = nil
}

// StartHealthMonitoring manually starts health monitoring (if not already started)
func (ca *ClusterAdapter) StartHealthMonitoring() {
	// Health monitoring is started automatically in NewClusterAdapter
	// This method is provided for compatibility
	logging.Debug("Health monitoring is already running")
}

// ForceHealthCheck triggers an immediate health check on all nodes
func (ca *ClusterAdapter) ForceHealthCheck() {
	logging.Info("Forcing immediate health check on all Redis nodes")
	ca.performHealthChecks()
}

// IsHealthy returns true if at least one node is healthy
func (ca *ClusterAdapter) IsHealthy() bool {
	return len(ca.getHealthyNodes()) > 0
}

// WaitForHealthy waits for at least one node to become healthy
func (ca *ClusterAdapter) WaitForHealthy(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		if ca.IsHealthy() {
			return nil
		}

		select {
		case <-ticker.C:
			if time.Now().After(deadline) {
				return errors.New("timeout waiting for healthy Redis nodes")
			}
			// Force a health check
			ca.ForceHealthCheck()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
