package pgcluster

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/dobrevit/svckit/logging"
)

// startWriterDetection begins periodic writer detection
func (cm *ClusterManager) startWriterDetection() {
	// Only start if we have a detection strategy that requires monitoring
	if cm.config.WriterDetectionStrategy == StrategyConfig {
		return // Config-based detection doesn't need monitoring
	}

	cm.tombManager.StartBackgroundTask(func(ctx context.Context) {
		// Initial detection
		cm.detectWriter()

		// Periodic detection (less frequent than health checks)
		ticker := time.NewTicker(cm.config.HealthCheckInterval * 3)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cm.detectWriter()
			case <-ctx.Done():
				logging.Warn("Writer detection stopped for database cluster")
				return
			}
		}
	}, "writer-detection")
}

// detectWriter detects which node is currently the writer
func (cm *ClusterManager) detectWriter() {
	switch cm.config.WriterDetectionStrategy {
	case StrategyDNS:
		cm.detectWriterByDNS()
	case StrategyQuery:
		cm.detectWriterByQuery()
	case StrategyProbe:
		cm.detectWriterByProbe()
	case StrategyConfig:
		// Already configured, nothing to do
		return
	default:
		logging.Warn("Unknown writer detection strategy: %v", cm.config.WriterDetectionStrategy)
	}
}

// detectWriterByDNS uses DNS resolution to identify the writer
func (cm *ClusterManager) detectWriterByDNS() {
	if cm.config.WriterDNSName == "" {
		logging.Warn("Writer DNS name not configured for DNS detection")
		return
	}

	// Resolve the writer DNS name
	ips, err := net.LookupIP(cm.config.WriterDNSName)
	if err != nil {
		logging.Error("Failed to resolve writer DNS %s: %v", cm.config.WriterDNSName, err)
		return
	}

	if len(ips) == 0 {
		logging.Warn("No IPs resolved for writer DNS %s", cm.config.WriterDNSName)
		return
	}

	writerIP := ips[0].String()
	logging.Info("Resolved writer DNS %s to IP: %s", cm.config.WriterDNSName, writerIP)

	// Match nodes to the resolved IP
	cm.updateWriterByIP(writerIP)
}

// detectWriterByQuery uses SQL queries to detect the writer
func (cm *ClusterManager) detectWriterByQuery() {
	query := cm.config.WriterDetectionQuery
	if query == "" {
		// Default PostgreSQL query to detect if node is primary
		query = "SELECT NOT pg_is_in_recovery();"
	}

	for _, node := range cm.nodes {
		if node.Health != HealthHealthy || node.sqlDB == nil {
			continue
		}

		go func(node *DatabaseNode) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var isPrimary bool
			if err := node.sqlDB.QueryRowContext(ctx, query).Scan(&isPrimary); err != nil {
				logging.Error("Writer detection query failed for node %s: %v", node.Name, err)
				return
			}

			node.mutex.Lock()
			defer node.mutex.Unlock()

			if isPrimary {
				if node.Role != RoleWriter {
					logging.Info("Node %s detected as writer (was %s)", node.Name, node.Role)
					node.Role = RoleWriter
					node.Priority = 0 // Highest priority
				}
			} else {
				if node.Role != RoleReader {
					logging.Info("Node %s detected as reader (was %s)", node.Name, node.Role)
					node.Role = RoleReader
					if node.Priority == 0 {
						node.Priority = 1 // Lower priority
					}
				}
			}
		}(node)
	}
}

// detectWriterByProbe uses health probes to detect the writer
func (cm *ClusterManager) detectWriterByProbe() {
	// This strategy tries to start transactions to detect which node accepts writes
	for _, node := range cm.nodes {
		if node.Health != HealthHealthy || node.sqlDB == nil {
			continue
		}

		go func(node *DatabaseNode) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			// Try a simple transaction to see if writes are accepted. A read
			// replica refuses a read-write transaction, so starting one is
			// the probe.
			tx, err := node.sqlDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
			if err != nil {
				return // Can't start transaction
			}

			// Immediately rollback - we don't want to actually write anything
			_ = tx.Rollback()

			// If we got here, the node accepts write transactions
			node.mutex.Lock()
			defer node.mutex.Unlock()

			if node.Role != RoleWriter {
				logging.Info("Node %s detected as writer via probe (was %s)", node.Name, node.Role)
				node.Role = RoleWriter
				node.Priority = 0
			}
		}(node)
	}
}

// updateWriterByIP updates node roles based on resolved writer IP
func (cm *ClusterManager) updateWriterByIP(writerIP string) {
	for _, node := range cm.nodes {
		// Extract IP from connection string
		nodeIP := cm.extractIPFromURL(node.URL)
		if nodeIP == "" {
			continue
		}

		node.mutex.Lock()

		if nodeIP == writerIP {
			if node.Role != RoleWriter {
				logging.Info("Node %s (%s) marked as writer based on DNS resolution", node.Name, nodeIP)
				node.Role = RoleWriter
				node.Priority = 0
			}
		} else {
			if node.Role != RoleReader {
				logging.Info("Node %s (%s) marked as reader based on DNS resolution", node.Name, nodeIP)
				node.Role = RoleReader
				if node.Priority == 0 {
					node.Priority = 1
				}
			}
		}

		node.mutex.Unlock()
	}
}

// extractIPFromURL extracts IP address from PostgreSQL connection URL
func (cm *ClusterManager) extractIPFromURL(url string) string {
	// Parse postgres://user:pass@host:port/db format
	if !strings.HasPrefix(url, "postgres://") {
		return ""
	}

	// Remove protocol
	url = strings.TrimPrefix(url, "postgres://")

	// Find @ symbol
	atIndex := strings.Index(url, "@")
	if atIndex == -1 {
		return ""
	}

	// Get host:port part
	hostPort := url[atIndex+1:]

	// Remove database name if present
	if slashIndex := strings.Index(hostPort, "/"); slashIndex != -1 {
		hostPort = hostPort[:slashIndex]
	}

	// Remove port if present
	if colonIndex := strings.Index(hostPort, ":"); colonIndex != -1 {
		return hostPort[:colonIndex]
	}

	return hostPort
}

// ForceWriterDetection triggers immediate writer detection
func (cm *ClusterManager) ForceWriterDetection() {
	logging.Info("Forcing writer detection for database cluster")
	cm.detectWriter()
}

// SetWriterNode manually sets a node as the writer (for emergencies)
func (cm *ClusterManager) SetWriterNode(nodeName string) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	var targetNode *DatabaseNode

	// Find the target node
	for _, node := range cm.nodes {
		if node.Name == nodeName {
			targetNode = node
			break
		}
	}

	if targetNode == nil {
		return fmt.Errorf("node %s not found", nodeName)
	}

	// Update all nodes
	for _, node := range cm.nodes {
		node.mutex.Lock()
		if node == targetNode {
			node.Role = RoleWriter
			node.Priority = 0
			logging.Info("Node %s manually set as writer", node.Name)
		} else if node.Role == RoleWriter {
			node.Role = RoleReader
			if node.Priority == 0 {
				node.Priority = 1
			}
			logging.Info("Node %s demoted from writer to reader", node.Name)
		}
		node.mutex.Unlock()
	}

	return nil
}
