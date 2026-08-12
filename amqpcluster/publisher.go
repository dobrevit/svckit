package amqpcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	"github.com/dobrevit/svckit/logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ClusterPublisher wraps the cluster adapter for event publishing
type ClusterPublisher struct {
	cluster     *ClusterAdapter
	serviceName string
	signer      *eventbus.EventSigner
	exchange    string
}

// NewClusterPublisher creates a new cluster-aware event publisher
func NewClusterPublisher(nodes []string, serviceName string, signingConfig *eventbus.SignatureConfig) (*ClusterPublisher, error) {
	config := ClusterConfig{
		Nodes:               nodes,
		ServiceName:         serviceName,
		HealthCheckInterval: 30 * time.Second,
		RetryAttempts:       3,
		LoadBalanceStrategy: "round_robin",
	}

	cluster, err := NewClusterAdapter(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster adapter: %w", err)
	}

	// Initialize signer if provided
	var signer *eventbus.EventSigner
	if signingConfig != nil {
		signer = eventbus.NewEventSigner(signingConfig)
	}

	publisher := &ClusterPublisher{
		cluster:     cluster,
		serviceName: serviceName,
		signer:      signer,
		exchange:    eventbus.DefaultExchange,
	}

	// Setup exchange on all nodes
	if err := publisher.setupInfrastructure(); err != nil {
		_ = cluster.Close() // returning the setup failure, not this one
		return nil, fmt.Errorf("failed to setup infrastructure: %w", err)
	}

	return publisher, nil
}

// setupInfrastructure creates necessary exchanges and infrastructure
func (cp *ClusterPublisher) setupInfrastructure() error {
	// Declare the main events exchange on all nodes
	return cp.cluster.DeclareExchange(
		cp.exchange, // name
		"topic",     // type
		true,        // durable
		false,       // auto-delete
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
}

// Publish publishes an event using cluster load balancing
func (cp *ClusterPublisher) Publish(ctx context.Context, event *eventbus.BaseEvent) error {
	// Add service metadata
	if event.Source == "" {
		event.Source = cp.serviceName
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Determine if we should sign the event
	if cp.signer != nil {
		return cp.publishSigned(ctx, event)
	}
	return cp.publishUnsigned(ctx, event)
}

// publishSigned publishes a signed event
func (cp *ClusterPublisher) publishSigned(ctx context.Context, event *eventbus.BaseEvent) error {
	// Create signed event
	signedEvent, err := cp.signer.Sign(event)
	if err != nil {
		return fmt.Errorf("failed to sign event: %w", err)
	}

	// Serialize signed event
	body, err := json.Marshal(signedEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal signed event: %w", err)
	}

	// Create AMQP message
	msg := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent, // Make messages persistent
		Timestamp:    time.Now(),
		MessageId:    event.ID,
		Type:         event.Type,
		AppId:        cp.serviceName,
		Body:         body,
		Headers: amqp.Table{
			"signed":     true,
			"signature":  signedEvent.Signature,
			"source":     event.Source,
			"user_id":    event.UserID,
			"event_id":   event.ID,
			"event_type": event.Type,
		},
	}

	// Publish with cluster failover
	if err := cp.cluster.Publish(cp.exchange, event.Type, false, false, msg); err != nil {
		return fmt.Errorf("failed to publish signed event to cluster: %w", err)
	}

	logging.Info("Published signed event %s (ID: %s) via cluster", event.Type, event.ID)
	return nil
}

// publishUnsigned publishes an unsigned event
func (cp *ClusterPublisher) publishUnsigned(ctx context.Context, event *eventbus.BaseEvent) error {
	// Serialize event
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Create AMQP message
	msg := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		MessageId:    event.ID,
		Type:         event.Type,
		AppId:        cp.serviceName,
		Body:         body,
		Headers: amqp.Table{
			"signed":     false,
			"source":     event.Source,
			"user_id":    event.UserID,
			"event_id":   event.ID,
			"event_type": event.Type,
		},
	}

	// Publish with cluster failover
	if err := cp.cluster.Publish(cp.exchange, event.Type, false, false, msg); err != nil {
		return fmt.Errorf("failed to publish unsigned event to cluster: %w", err)
	}

	logging.Info("Published unsigned event %s (ID: %s) via cluster", event.Type, event.ID)
	return nil
}

// TestConnection tests connectivity to the cluster
func (cp *ClusterPublisher) TestConnection() error {
	healthy := cp.cluster.getHealthyNodes()
	if len(healthy) == 0 {
		return fmt.Errorf("no healthy nodes available")
	}

	return nil
}

// GetStats returns cluster and publisher statistics
func (cp *ClusterPublisher) GetStats() PublisherStats {
	nodeStats := cp.cluster.GetNodeStats()

	stats := PublisherStats{
		ServiceName:  cp.serviceName,
		Exchange:     cp.exchange,
		NodeStats:    nodeStats,
		HealthyNodes: 0,
		TotalNodes:   len(nodeStats),
	}

	// Count healthy nodes
	for _, node := range nodeStats {
		if node.State == NodeHealthy {
			stats.HealthyNodes++
		}
	}

	return stats
}

// PublisherStats holds statistics for the cluster publisher
type PublisherStats struct {
	ServiceName  string
	Exchange     string
	NodeStats    map[string]NodeStats
	HealthyNodes int
	TotalNodes   int
}

// Close gracefully shuts down the publisher
func (cp *ClusterPublisher) Close() error {
	if cp.cluster != nil {
		return cp.cluster.Close()
	}
	return nil
}
