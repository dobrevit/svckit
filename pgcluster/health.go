package pgcluster

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dobrevit/svckit/logging"
)

// startHealthMonitoring begins periodic health checks of all nodes
func (cm *ClusterManager) startHealthMonitoring() {
	cm.tombManager.StartBackgroundTask(func(ctx context.Context) {
		ticker := time.NewTicker(cm.config.HealthCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cm.performHealthChecks()
			case <-ctx.Done():
				logging.Warn("Health monitoring stopped for database cluster")
				return
			}
		}
	}, "health-monitoring")
}

// performHealthChecks checks the health of all nodes
func (cm *ClusterManager) performHealthChecks() {
	for _, node := range cm.nodes {
		go cm.checkNodeHealth(node)
	}
}

// checkNodeHealth checks and potentially recovers a single node
func (cm *ClusterManager) checkNodeHealth(node *DatabaseNode) {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	// Skip if we checked recently
	if time.Since(node.lastCheck) < 10*time.Second {
		return
	}

	node.lastCheck = time.Now()

	// Test connection health
	if node.sqlDB == nil {
		logging.Warn("Node %s has no connection, attempting to reconnect", node.Name)
		if err := cm.connectToNode(node); err != nil {
			logging.Error("Failed to reconnect to node %s: %v", node.Name, err)
			return
		}
		return
	}

	// Ping test with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	err := node.sqlDB.PingContext(ctx)
	node.responseTime = time.Since(start)

	if err != nil {
		atomic.AddInt64(&node.errorCount, 1)
		node.lastError = err

		// Determine if this is a temporary or permanent failure
		if cm.isRetryableError(err) {
			if node.Health == HealthHealthy {
				node.Health = HealthDegraded
				logging.Warn("Node %s marked as degraded due to ping error: %v", node.Name, err)
			}
		} else {
			node.Health = HealthFailed
			logging.Error("Node %s marked as failed due to non-retryable error: %v", node.Name, err)

			// Close the connection to force reconnection
			_ = node.sqlDB.Close() // forcing a reconnect; the pool is going away
			node.sqlDB = nil
		}
		return
	}

	// Connection is healthy - upgrade state if needed
	if node.Health != HealthHealthy {
		node.Health = HealthHealthy
		node.lastError = nil
		logging.Info("Node %s recovered and marked as healthy", node.Name)
	}

	// Additional health checks for high response times
	if node.responseTime > 5*time.Second {
		logging.Warn("Node %s has high response time: %v", node.Name, node.responseTime)
		if node.Health == HealthHealthy {
			node.Health = HealthDegraded
		}
	}
}

// isRetryableError determines if a database error is worth retrying
func (cm *ClusterManager) isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	// Retryable: temporary network, connection, timeout errors
	retryableKeywords := []string{
		"connection refused", "timeout", "network", "dial", "temporary",
		"connection reset", "broken pipe", "no route to host",
		"connection timed out", "i/o timeout", "context deadline exceeded",
	}

	for _, keyword := range retryableKeywords {
		if contains(errStr, keyword) {
			return true
		}
	}

	// Non-retryable: authentication, configuration, permanent errors
	nonRetryableKeywords := []string{
		"authentication", "password", "permission denied", "access denied",
		"database does not exist", "role does not exist", "syntax error",
		"invalid", "malformed", "ssl required",
	}

	for _, keyword := range nonRetryableKeywords {
		if contains(errStr, keyword) {
			return false
		}
	}

	// Default to retryable for unknown errors
	return true
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	s = strings.ToLower(s)
	substr = strings.ToLower(substr)
	return strings.Contains(s, substr)
}

// GetNodeStats returns statistics for all nodes
func (cm *ClusterManager) GetNodeStats() map[string]NodeStats {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	stats := make(map[string]NodeStats)
	for _, node := range cm.nodes {
		node.mutex.RLock()

		var poolStats sql.DBStats
		if node.sqlDB != nil {
			poolStats = node.sqlDB.Stats()
		}

		stats[node.Name] = NodeStats{
			URL:          node.URL,
			Role:         node.Role,
			Health:       node.Health,
			Priority:     node.Priority,
			ReadCount:    atomic.LoadInt64(&node.readCount),
			WriteCount:   atomic.LoadInt64(&node.writeCount),
			ErrorCount:   atomic.LoadInt64(&node.errorCount),
			ResponseTime: node.responseTime,
			LastError:    node.lastError,
			LastCheck:    node.lastCheck,
			PoolStats:    poolStats,
		}
		node.mutex.RUnlock()
	}

	return stats
}

// NodeStats holds statistics for a single database node
type NodeStats struct {
	URL          string
	Role         NodeRole
	Health       NodeHealth
	Priority     int
	ReadCount    int64
	WriteCount   int64
	ErrorCount   int64
	ResponseTime time.Duration
	LastError    error
	LastCheck    time.Time
	PoolStats    sql.DBStats
}

// HealthCheck performs a comprehensive health check of the cluster
func (cm *ClusterManager) HealthCheck() error {
	cm.performHealthChecks()

	// Wait a moment for health checks to complete
	time.Sleep(100 * time.Millisecond)

	if !cm.hasHealthyNodes() {
		return errors.New("no healthy database nodes available")
	}

	// Check if we have a healthy writer
	writerHealthy := false
	readerCount := 0

	for _, node := range cm.nodes {
		node.mutex.RLock()
		if node.Health == HealthHealthy {
			switch node.Role {
			case RoleWriter:
				writerHealthy = true
			case RoleReader:
				readerCount++
			}
		}
		node.mutex.RUnlock()
	}

	if !writerHealthy {
		return errors.New("no healthy writer node available")
	}

	logging.Info("Database cluster health check passed: 1 writer, %d readers healthy", readerCount)
	return nil
}
