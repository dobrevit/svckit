package eventbus

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// BroadcastSubscriber extends the regular subscriber to support broadcast messaging
// where all service instances receive the same message
type BroadcastSubscriber struct {
	*Subscriber
	instanceID string
	logger     interface {
		Infof(format string, args ...interface{})
		Warnf(format string, args ...interface{})
		Errorf(format string, args ...interface{})
		Debugf(format string, args ...interface{})
	}
}

// BroadcastConfig configures broadcast messaging
type BroadcastConfig struct {
	// ExchangeName for broadcast messages (fanout exchange)
	ExchangeName string

	// ServiceName for the consuming service
	ServiceName string

	// InstanceID unique identifier for this service instance
	InstanceID string

	// TTL for broadcast messages (optional)
	MessageTTL time.Duration

	// MaxRetries for broadcast message processing
	MaxRetries int
}

// NewBroadcastSubscriber creates a subscriber for broadcast messages
func NewBroadcastSubscriber(rabbitmqURL string, config BroadcastConfig) (*BroadcastSubscriber, error) {
	// Create base subscriber
	subscriber, err := NewSubscriberWithService(rabbitmqURL, config.ServiceName)
	if err != nil {
		return nil, fmt.Errorf("failed to create base subscriber: %w", err)
	}

	// Create a simple logger adapter
	logger := &simpleLogger{}

	return &BroadcastSubscriber{
		Subscriber: subscriber,
		instanceID: config.InstanceID,
		logger:     logger,
	}, nil
}

// SubscribeBroadcast subscribes to broadcast messages using fanout exchange
// Each service instance gets its own temporary queue
func (bs *BroadcastSubscriber) SubscribeBroadcast(exchangeName, routingKey string, handler EventHandler) error {
	// Declare fanout exchange for broadcast
	err := bs.channel.ExchangeDeclare(
		exchangeName, // name
		"fanout",     // type - fanout broadcasts to all bound queues
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare broadcast exchange: %w", err)
	}

	// Create instance-specific queue name
	// Format: exchange.service.instance-id (auto-delete when service stops)
	queueName := fmt.Sprintf("%s.%s.%s", exchangeName, bs.serviceName, bs.instanceID)

	// Declare exclusive queue for this service instance
	queue, err := bs.channel.QueueDeclare(
		queueName, // name - unique per service instance
		false,     // durable - temporary queue
		true,      // delete when unused
		true,      // exclusive to this connection
		false,     // no-wait
		amqp.Table{
			"x-message-ttl": int64(300000), // 5 minutes TTL for broadcast messages
		},
	)
	if err != nil {
		return fmt.Errorf("failed to declare broadcast queue: %w", err)
	}

	// Bind queue to fanout exchange (routing key ignored for fanout)
	err = bs.channel.QueueBind(
		queue.Name,   // queue name
		"",           // routing key (ignored for fanout)
		exchangeName, // exchange
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to bind broadcast queue: %w", err)
	}

	// Start consuming with broadcast-specific settings
	msgs, err := bs.channel.Consume(
		queue.Name,    // queue
		bs.instanceID, // consumer tag (use instance ID)
		false,         // auto-ack disabled
		true,          // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		return fmt.Errorf("failed to register broadcast consumer: %w", err)
	}

	// Process messages in separate goroutine
	go bs.processBroadcastMessages(msgs, handler, exchangeName, routingKey)

	bs.logger.Infof("Subscribed to broadcast exchange '%s' with instance queue '%s'", exchangeName, queue.Name)
	return nil
}

// BroadcastEventHandler handles broadcast events with context
type BroadcastEventHandler func(event *BroadcastEvent) error

// BroadcastEvent represents a broadcast event with additional metadata
type BroadcastEvent struct {
	*BaseEvent
	Exchange   string            `json:"exchange"`
	InstanceID string            `json:"instance_id"`
	Headers    map[string]string `json:"headers"`
}

// processBroadcastMessages handles incoming broadcast messages
func (bs *BroadcastSubscriber) processBroadcastMessages(msgs <-chan amqp.Delivery, handler EventHandler, exchangeName, routingKey string) {
	for msg := range msgs {
		// Parse the BaseEvent from message body
		baseEvent, err := FromJSON(msg.Body)
		if err != nil {
			bs.logger.Errorf("Failed to parse broadcast event JSON: %v", err)
			msg.Nack(false, false)
			continue
		}

		// Create broadcast event with additional metadata
		broadcastEvent := &BroadcastEvent{
			BaseEvent:  baseEvent,
			Exchange:   exchangeName,
			InstanceID: bs.instanceID,
			Headers:    make(map[string]string),
		}

		// Convert AMQP headers to event headers
		for key, value := range msg.Headers {
			if str, ok := value.(string); ok {
				broadcastEvent.Headers[key] = str
			}
		}

		// Add broadcast-specific headers
		broadcastEvent.Headers["broadcast"] = "true"
		broadcastEvent.Headers["exchange"] = exchangeName
		broadcastEvent.Headers["instance_id"] = bs.instanceID

		bs.logger.Debugf("Processing broadcast message: %s from exchange %s", baseEvent.Type, exchangeName)

		// Process event with handler (using BaseEvent interface)
		ctx := context.WithValue(context.Background(), "broadcast_metadata", broadcastEvent)
		if err := handler.Handle(ctx, baseEvent); err != nil {
			bs.logger.Errorf("Failed to process broadcast event %s: %v", baseEvent.ID, err)

			// Check retry count
			retryCount := 0
			if retryHeader, exists := msg.Headers["x-retry-count"]; exists {
				if count, ok := retryHeader.(int32); ok {
					retryCount = int(count)
				}
			}

			// Retry logic for broadcast messages
			if retryCount < 3 { // Max 3 retries for broadcast
				bs.logger.Warnf("Retrying broadcast message %s (attempt %d)", baseEvent.ID, retryCount+1)
				msg.Nack(false, true) // Requeue for retry
			} else {
				bs.logger.Errorf("Max retries exceeded for broadcast message %s, discarding", baseEvent.ID)
				msg.Nack(false, false) // Don't requeue, send to DLX if configured
			}
		} else {
			// Acknowledge successful processing
			msg.Ack(false)
			bs.logger.Debugf("Successfully processed broadcast event %s", baseEvent.ID)
		}
	}
}

// BroadcastPublisher extends the regular publisher for broadcast messaging
type BroadcastPublisher struct {
	*Publisher
}

// NewBroadcastPublisher creates a publisher for broadcast messages
// ServiceName returns the service name this publisher stamps on events.
func (bp *BroadcastPublisher) ServiceName() string {
	return bp.serviceName
}

func NewBroadcastPublisher(rabbitmqURL, serviceName string) (*BroadcastPublisher, error) {
	publisher, err := NewPublisherWithService(rabbitmqURL, serviceName)
	if err != nil {
		return nil, fmt.Errorf("failed to create base publisher: %w", err)
	}

	return &BroadcastPublisher{
		Publisher: publisher,
	}, nil
}

// PublishBroadcast publishes a message to all service instances via fanout exchange
func (bp *BroadcastPublisher) PublishBroadcast(exchangeName string, event *BaseEvent) error {
	// Declare fanout exchange
	err := bp.channel.ExchangeDeclare(
		exchangeName, // name
		"fanout",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare broadcast exchange: %w", err)
	}

	// Serialize the BaseEvent to JSON
	body, err := event.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize broadcast event: %w", err)
	}

	// Prepare message headers
	headers := amqp.Table{
		"broadcast":    true,
		"published_at": time.Now().Format(time.RFC3339),
		"source":       event.Source,
		"event_type":   event.Type,
		"event_id":     event.ID,
	}

	// Publish to fanout exchange (routing key ignored)
	err = bp.channel.Publish(
		exchangeName, // exchange
		"",           // routing key (ignored for fanout)
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			MessageId:    event.ID,
			Type:         event.Type,
			AppId:        bp.serviceName,
			Timestamp:    event.Timestamp,
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Headers:      headers,
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish broadcast event: %w", err)
	}

	log.Printf("Published broadcast event %s to exchange %s", event.Type, exchangeName)
	return nil
}

// BroadcastExchanges defines standard broadcast exchanges
var BroadcastExchanges = struct {
	CertificateEvents string
	SecurityAlerts    string
	SystemEvents      string
}{
	CertificateEvents: "certificate.broadcast",
	SecurityAlerts:    "security.broadcast",
	SystemEvents:      "system.broadcast",
}

// CertificateEventTypes defines certificate-related broadcast event types
var CertificateEventTypes = struct {
	CertificateRevoked     string
	CertificateExpiring    string
	CACompromised          string
	EmergencyRenewal       string
	CertificateProvisioned string
}{
	CertificateRevoked:     "certificate.revoked",
	CertificateExpiring:    "certificate.expiring",
	CACompromised:          "ca.compromised",
	EmergencyRenewal:       "certificate.emergency.renewal",
	CertificateProvisioned: "certificate.provisioned",
}

// simpleLogger provides basic logging functionality
type simpleLogger struct{}

func (l *simpleLogger) Infof(format string, args ...interface{}) {
	log.Printf("[INFO] "+format, args...)
}

func (l *simpleLogger) Warnf(format string, args ...interface{}) {
	log.Printf("[WARN] "+format, args...)
}

func (l *simpleLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}

func (l *simpleLogger) Debugf(format string, args ...interface{}) {
	log.Printf("[DEBUG] "+format, args...)
}
