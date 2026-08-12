package audit

import (
	"context"
	"fmt"
	"time"

	"github.com/dobrevit/svckit/amqpcluster"
	"github.com/dobrevit/svckit/eventbus"
	"github.com/dobrevit/svckit/logging"
	"github.com/google/uuid"
)

// Event types for audit service
const (
	EventAuditLog = "audit.log"
)

// PublisherInterface allows for both single and cluster publishers
type PublisherInterface interface {
	Publish(ctx context.Context, event *eventbus.BaseEvent) error
	Close() error
}

// AuditClient provides an interface for services to publish audit events.
//
// A nil *AuditClient is valid and means auditing is switched off: every method
// returns nil without doing anything. That is what a service gets when audit
// was wired as optional and the broker was unreachable, so callers do not have
// to nil-check before every call.
type AuditClient struct {
	publisher   PublisherInterface
	serviceName string
}

// NewAuditClient creates a new audit client with signed event publishing
func NewAuditClient(rabbitmqURL, serviceName string, signingConfig *eventbus.SignatureConfig) (*AuditClient, error) {
	publisher, err := eventbus.NewPublisherWithSigning(rabbitmqURL, serviceName, signingConfig, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create signed event publisher: %w", err)
	}

	return &AuditClient{
		publisher:   publisher,
		serviceName: serviceName,
	}, nil
}

// NewClusterAuditClient creates a new audit client with cluster support
func NewClusterAuditClient(nodes []string, serviceName string, signingConfig *eventbus.SignatureConfig) (*AuditClient, error) {
	publisher, err := amqpcluster.NewClusterPublisher(nodes, serviceName, signingConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster event publisher: %w", err)
	}

	return &AuditClient{
		publisher:   publisher,
		serviceName: serviceName,
	}, nil
}

// NewAuditClientWithPublisher creates an audit client over an existing
// publisher. It is how a service routes audit events through a transport it
// already owns — and the only way to supply the exported PublisherInterface,
// which otherwise had no constructor accepting it.
func NewAuditClientWithPublisher(publisher PublisherInterface, serviceName string) *AuditClient {
	return &AuditClient{
		publisher:   publisher,
		serviceName: serviceName,
	}
}

// NewAuditClientUnsigned creates an audit client without signing (for testing only)
func NewAuditClientUnsigned(rabbitmqURL, serviceName string) (*AuditClient, error) {
	publisher, err := eventbus.NewPublisherWithService(rabbitmqURL, serviceName)
	if err != nil {
		return nil, fmt.Errorf("failed to create event publisher: %w", err)
	}

	return &AuditClient{
		publisher:   publisher,
		serviceName: serviceName,
	}, nil
}

// AuditEventData represents the data structure for audit events
type AuditEventData struct {
	// Event identifiers
	EventID string `json:"event_id"`
	TraceID string `json:"trace_id,omitempty"`

	// Event metadata
	EventType string `json:"event_type"` // "user_action", "system_event", "security_event"
	Version   string `json:"version"`

	// Actor information
	ActorType string `json:"actor_type"` // "user", "service", "system"
	ActorID   string `json:"actor_id,omitempty"`
	ActorName string `json:"actor_name,omitempty"`

	// Action details
	Action     string `json:"action"`   // "create", "read", "update", "delete"
	Resource   string `json:"resource"` // "user", "form", "skill", "location"
	ResourceID string `json:"resource_id,omitempty"`

	// Context
	SessionID string `json:"session_id,omitempty"`
	IPAddress string `json:"ip_address,omitempty"`
	UserAgent string `json:"user_agent,omitempty"`
	Location  string `json:"location,omitempty"`
	Platform  string `json:"platform,omitempty"` // "web", "mobile", "api"

	// Result
	Status     string `json:"status"` // "success", "failure", "warning"
	StatusCode int    `json:"status_code,omitempty"`
	ErrorMsg   string `json:"error_message,omitempty"`
	Duration   int64  `json:"duration_ms,omitempty"`

	// Compliance
	Sensitivity string `json:"sensitivity,omitempty"` // "public", "internal", "confidential", "restricted"
	LegalBasis  string `json:"legal_basis,omitempty"`
	Purpose     string `json:"purpose,omitempty"`

	// Additional data
	Metadata map[string]any `json:"metadata,omitempty"`
	Changes  map[string]any `json:"changes,omitempty"`
}

// LogOptions contains optional parameters for audit logging
type LogOptions struct {
	TraceID     string
	SessionID   string
	IPAddress   string
	UserAgent   string
	Location    string
	Platform    string
	Duration    time.Duration
	StatusCode  int
	ErrorMsg    string
	Sensitivity string
	LegalBasis  string
	Purpose     string
	Metadata    map[string]any
	Changes     map[string]any
}

// Log publishes an audit event asynchronously via RabbitMQ
func (c *AuditClient) Log(ctx context.Context, eventType, actorType, actorID, action, resource string, status string, opts *LogOptions) error {
	if c == nil {
		return nil // auditing is switched off
	}
	if opts == nil {
		opts = &LogOptions{}
	}

	// Generate event ID
	eventID := uuid.New().String()

	// Build audit event data
	auditData := AuditEventData{
		EventID:     eventID,
		TraceID:     opts.TraceID,
		EventType:   eventType,
		Version:     "1.0",
		ActorType:   actorType,
		ActorID:     actorID,
		Action:      action,
		Resource:    resource,
		ResourceID:  extractResourceID(opts.Metadata),
		SessionID:   opts.SessionID,
		IPAddress:   opts.IPAddress,
		UserAgent:   opts.UserAgent,
		Location:    opts.Location,
		Platform:    opts.Platform,
		Status:      status,
		StatusCode:  opts.StatusCode,
		ErrorMsg:    opts.ErrorMsg,
		Duration:    int64(opts.Duration / time.Millisecond),
		Sensitivity: opts.Sensitivity,
		LegalBasis:  opts.LegalBasis,
		Purpose:     opts.Purpose,
		Metadata:    opts.Metadata,
		Changes:     opts.Changes,
	}

	// Convert to map for event publishing
	eventData := map[string]any{
		"audit_data": auditData,
		"timestamp":  time.Now(),
		"source":     c.serviceName,
	}

	// Create event
	event := eventbus.NewEvent(EventAuditLog, c.serviceName, eventData)

	// Set user ID if actor is a user
	if actorType == "user" && actorID != "" {
		event.SetUserID(actorID)
	}

	// Publish event asynchronously
	err := c.publisher.Publish(ctx, event)
	if err != nil {
		logging.Error("Failed to publish audit event: %v", err)
		return fmt.Errorf("failed to publish audit event: %w", err)
	}

	logging.Debug("Published audit event: %s (Action: %s %s, Status: %s)", eventID, action, resource, status)
	return nil
}

// Helper methods for common audit scenarios

// LogUserAction logs a user-initiated action
func (c *AuditClient) LogUserAction(ctx context.Context, userID, userName, action, resource string, status string, opts *LogOptions) error {
	if c == nil {
		return nil // auditing is switched off
	}
	if opts == nil {
		opts = &LogOptions{}
	}

	// Add actor name to metadata
	if userName != "" && opts.Metadata == nil {
		opts.Metadata = make(map[string]any)
	}
	if userName != "" {
		opts.Metadata["actor_name"] = userName
	}

	return c.Log(ctx, "user_action", "user", userID, action, resource, status, opts)
}

// LogServiceAction logs a service-initiated action
func (c *AuditClient) LogServiceAction(ctx context.Context, action, resource string, status string, opts *LogOptions) error {
	if c == nil {
		return nil // auditing is switched off
	}
	return c.Log(ctx, "system_event", "service", c.serviceName, action, resource, status, opts)
}

// LogSecurityEvent logs a security-related event
func (c *AuditClient) LogSecurityEvent(ctx context.Context, actorType, actorID, action, resource string, status string, opts *LogOptions) error {
	if c == nil {
		return nil // auditing is switched off
	}
	if opts == nil {
		opts = &LogOptions{}
	}

	// Security events are always marked as restricted
	if opts.Sensitivity == "" {
		opts.Sensitivity = "restricted"
	}

	return c.Log(ctx, "security_event", actorType, actorID, action, resource, status, opts)
}

// LogDataAccess logs data access events (for compliance)
func (c *AuditClient) LogDataAccess(ctx context.Context, userID, resource, resourceID string, opts *LogOptions) error {
	if c == nil {
		return nil // auditing is switched off
	}
	if opts == nil {
		opts = &LogOptions{}
	}

	if opts.Metadata == nil {
		opts.Metadata = make(map[string]any)
	}
	opts.Metadata["resource_id"] = resourceID

	return c.LogUserAction(ctx, userID, "", "read", resource, "success", opts)
}

// LogDataModification logs data modification events
func (c *AuditClient) LogDataModification(ctx context.Context, userID, action, resource, resourceID string, changes map[string]any, opts *LogOptions) error {
	if c == nil {
		return nil // auditing is switched off
	}
	if opts == nil {
		opts = &LogOptions{}
	}

	opts.Changes = changes

	if opts.Metadata == nil {
		opts.Metadata = make(map[string]any)
	}
	opts.Metadata["resource_id"] = resourceID

	return c.LogUserAction(ctx, userID, "", action, resource, "success", opts)
}

// Close closes the audit client connection
func (c *AuditClient) Close() error {
	if c == nil {
		return nil // auditing is switched off
	}
	if c.publisher != nil {
		return c.publisher.Close()
	}
	return nil
}

// Helper function to extract resource ID from metadata
func extractResourceID(metadata map[string]any) string {
	if metadata == nil {
		return ""
	}

	if id, ok := metadata["resource_id"].(string); ok {
		return id
	}

	if id, ok := metadata["id"].(string); ok {
		return id
	}

	return ""
}
