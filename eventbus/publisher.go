package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dobrevit/svckit/logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

// DefaultExchange is the topic exchange publishers and subscribers declare
// and bind against when no other exchange is configured. Deployments with an
// established wire contract override it at startup (before any publisher or
// subscriber is created).
var DefaultExchange = "events"

// Publisher handles publishing events to RabbitMQ
type Publisher struct {
	conn           *amqp.Connection
	channel        *amqp.Channel
	serviceName    string
	signer         *EventSigner
	requireSigning bool
}

// NewPublisher creates a new event publisher
func NewPublisher(rabbitmqURL string) (*Publisher, error) {
	return NewPublisherWithService(rabbitmqURL, "unknown")
}

// NewPublisherWithService creates a new event publisher with service name
func NewPublisherWithService(rabbitmqURL, serviceName string) (*Publisher, error) {
	return NewPublisherWithSigning(rabbitmqURL, serviceName, nil, false)
}

// NewPublisherWithSigning creates a new event publisher with optional message signing
func NewPublisherWithSigning(rabbitmqURL, serviceName string, signingConfig *SignatureConfig, requireSigning bool) (*Publisher, error) {
	conn, err := amqp.Dial(rabbitmqURL)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	// Declare exchange
	err = ch.ExchangeDeclare(
		DefaultExchange, // name
		"topic",         // type
		true,            // durable
		false,           // auto-deleted
		false,           // internal
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		return nil, err
	}

	var signer *EventSigner
	if signingConfig != nil || requireSigning {
		signer = NewEventSigner(signingConfig)
	}

	publisher := &Publisher{
		conn:           conn,
		channel:        ch,
		serviceName:    serviceName,
		signer:         signer,
		requireSigning: requireSigning,
	}

	// Update connection status
	UpdateConnectionStatus(serviceName, "publisher", true)

	return publisher, nil
}

// Publish publishes an event to the event bus with optional signing
func (p *Publisher) Publish(ctx context.Context, event *BaseEvent) error {
	var body []byte
	var err error
	var headers amqp.Table

	// Check if signing is enabled
	if p.signer != nil {
		return p.PublishSigned(ctx, event)
	}

	// Publish unsigned event
	body, err = event.ToJSON()
	if err != nil {
		RecordEventPublished(event.Type, p.serviceName, "error")
		return err
	}

	headers = amqp.Table{
		"signed":     false,
		"source":     event.Source,
		"event_type": event.Type,
		"event_id":   event.ID,
	}

	err = p.channel.Publish(
		DefaultExchange, // exchange
		event.Type,      // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
			Headers:     headers,
			Timestamp:   time.Now(),
		},
	)

	if err != nil {
		RecordEventPublished(event.Type, p.serviceName, "error")
		return err
	}

	logging.Warn("⚠️  Published unsigned event: %s (ID: %s)", event.Type, event.ID)
	RecordEventPublished(event.Type, p.serviceName, "success")
	return nil
}

// PublishSigned publishes a cryptographically signed event
func (p *Publisher) PublishSigned(ctx context.Context, event *BaseEvent) error {
	if p.signer == nil {
		return errors.New("signer not configured for signed publishing")
	}

	// Sign the event
	signedEvent, err := p.signer.Sign(event)
	if err != nil {
		RecordEventPublished(event.Type, p.serviceName, "error")
		return err
	}

	// Serialize signed event
	body, err := json.Marshal(signedEvent)
	if err != nil {
		RecordEventPublished(event.Type, p.serviceName, "error")
		return err
	}

	// Create headers with signature information
	headers := amqp.Table{
		"signed":     true,
		"signature":  signedEvent.Signature,
		"signed_at":  signedEvent.SignedAt.Unix(),
		"expires_at": signedEvent.ExpiresAt.Unix(),
		"source":     event.Source,
		"event_type": event.Type,
		"event_id":   event.ID,
	}

	err = p.channel.Publish(
		DefaultExchange, // exchange
		event.Type,      // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
			Headers:     headers,
			Timestamp:   time.Now(),
		},
	)

	if err != nil {
		RecordEventPublished(event.Type, p.serviceName, "error")
		return err
	}

	logging.Info("✅ Published signed event: %s (ID: %s, Signature: %.8s...)",
		event.Type, event.ID, signedEvent.Signature)
	RecordEventPublished(event.Type, p.serviceName, "success")
	return nil
}

// TestConnection tests the RabbitMQ connection health
func (p *Publisher) TestConnection() error {
	if p.conn == nil || p.conn.IsClosed() {
		return errors.New("connection is closed")
	}

	if p.channel == nil {
		return errors.New("channel is nil")
	}

	// Try to create a temporary channel to test the connection
	testCh, err := p.conn.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = testCh.Close() }()

	return nil
}

// Close closes the publisher connection
func (p *Publisher) Close() error {
	// Update connection status
	UpdateConnectionStatus(p.serviceName, "publisher", false)

	var errs []error
	if p.channel != nil {
		if err := p.channel.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing channel: %w", err))
		}
	}
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing connection: %w", err))
		}
	}
	return errors.Join(errs...)
}

// EventHandler defines the interface for handling events
type EventHandler interface {
	Handle(ctx context.Context, event *BaseEvent) error
}

// Subscriber handles subscribing to events from RabbitMQ
type Subscriber struct {
	conn           *amqp.Connection
	channel        *amqp.Channel
	serviceName    string
	verifier       *EventVerifier
	requireSigning bool
}

// NewSubscriber creates a new event subscriber
func NewSubscriber(rabbitmqURL string) (*Subscriber, error) {
	return NewSubscriberWithService(rabbitmqURL, "unknown")
}

// NewSubscriberWithService creates a new event subscriber with service name
func NewSubscriberWithService(rabbitmqURL, serviceName string) (*Subscriber, error) {
	return NewSubscriberWithSigning(rabbitmqURL, serviceName, nil, false)
}

// NewSubscriberWithSigning creates a new event subscriber with optional signature verification
func NewSubscriberWithSigning(rabbitmqURL, serviceName string, signingConfig *SignatureConfig, requireSigning bool) (*Subscriber, error) {
	conn, err := amqp.Dial(rabbitmqURL)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	var verifier *EventVerifier
	if signingConfig != nil || requireSigning {
		verifier = NewEventVerifier(signingConfig)
	}

	subscriber := &Subscriber{
		conn:           conn,
		channel:        ch,
		serviceName:    serviceName,
		verifier:       verifier,
		requireSigning: requireSigning,
	}

	// Update connection status
	UpdateConnectionStatus(serviceName, "subscriber", true)

	return subscriber, nil
}

// Subscribe subscribes to events with the given routing key
func (s *Subscriber) Subscribe(queueName string, routingKey string, handler EventHandler) error {
	// Declare dead letter queue first
	dlqName := queueName + ".dlq"
	_, err := s.channel.QueueDeclare(
		dlqName, // name
		true,    // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		return err
	}

	// Declare main queue with dead letter configuration
	q, err := s.channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		amqp.Table{
			"x-dead-letter-exchange":    DefaultExchange,
			"x-dead-letter-routing-key": dlqName,
			"x-message-ttl":             300000, // 5 minutes
		}, // arguments
	)
	if err != nil {
		return err
	}

	// Bind main queue to exchange
	err = s.channel.QueueBind(
		q.Name,          // queue name
		routingKey,      // routing key
		DefaultExchange, // exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// Bind dead letter queue to exchange
	err = s.channel.QueueBind(
		dlqName,         // queue name
		dlqName,         // routing key
		DefaultExchange, // exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// Set QoS to limit unacknowledged messages
	err = s.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return err
	}

	// Consume messages with manual acknowledgment
	msgs, err := s.channel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack (changed to false)
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return err
	}

	// Process messages with proper error handling
	go func() {
		for d := range msgs {
			func() {
				defer func() {
					if r := recover(); r != nil {
						logging.Error("Panic handling event: %v", r)
						nack(d, false) // Send to dead letter queue
					}
				}()

				// Check if message is signed
				var event *BaseEvent
				isSigned, exists := d.Headers["signed"].(bool)

				if exists && isSigned && s.verifier != nil {
					// Parse as signed event
					var signedEvent SignedEvent
					if err := json.Unmarshal(d.Body, &signedEvent); err != nil {
						logging.Error("Error parsing signed event JSON: %v", err)
						RecordEventDLQ("unknown", s.serviceName, queueName, "parse_error")
						nack(d, false)
						return
					}

					// Verify signature
					if err := s.verifier.Verify(&signedEvent); err != nil {
						logging.Error("🚨 SECURITY ALERT: Invalid signature for event %s from %s: %v",
							signedEvent.Event.Type, signedEvent.Event.Source, err)
						RecordEventDLQ(signedEvent.Event.Type, s.serviceName, queueName, "invalid_signature")
						nack(d, false) // Don't requeue security failures
						return
					}

					event = signedEvent.Event
					logging.Info("✅ Verified signed event: %s (ID: %s)", event.Type, event.ID)
				} else {
					// Handle unsigned event
					if s.requireSigning && (!exists || !isSigned) {
						logging.Error("🚨 SECURITY ALERT: Unsigned event rejected (signing required)")
						RecordEventDLQ("unknown", s.serviceName, queueName, "unsigned_event_rejected")
						nack(d, false)
						return
					}

					var err error
					event, err = FromJSON(d.Body)
					if err != nil {
						logging.Error("Error parsing event JSON: %v", err)
						RecordEventDLQ("unknown", s.serviceName, queueName, "parse_error")
						nack(d, false)
						return
					}

					if !exists || !isSigned {
						logging.Warn("⚠️  Processing unsigned event: %s (ID: %s)", event.Type, event.ID)
					}
				}

				// Record event received
				RecordEventReceived(event.Type, s.serviceName, queueName)

				// Log the event for debugging
				logging.Info("Processing event: %s for user: %s", event.Type, event.UserID)

				// Start timing the processing
				processingStart := time.Now()

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				if err := handler.Handle(ctx, event); err != nil {
					logging.Error("Error handling event %s: %v", event.Type, err)

					// Record processing duration even for failures
					EventProcessingDuration.WithLabelValues(event.Type, s.serviceName, queueName).Observe(time.Since(processingStart).Seconds())

					// Check if this is a retryable error
					if isRetryableError(err) {
						RecordEventRequeued(event.Type, s.serviceName, queueName)
						RecordEventProcessed(event.Type, s.serviceName, queueName, "requeued")
						nack(d, true) // Requeue for retry
					} else {
						RecordEventDLQ(event.Type, s.serviceName, queueName, "processing_error")
						RecordEventProcessed(event.Type, s.serviceName, queueName, "error")
						nack(d, false) // Send to dead letter queue
					}
					return
				}

				// Record processing duration
				EventProcessingDuration.WithLabelValues(event.Type, s.serviceName, queueName).Observe(time.Since(processingStart).Seconds())

				// Successfully processed
				RecordEventAcknowledged(event.Type, s.serviceName, queueName)
				RecordEventProcessed(event.Type, s.serviceName, queueName, "success")
				ack(d)
				logging.Info("Successfully processed event: %s", event.Type)
			}()
		}
	}()

	logging.Info("Subscribed to queue: %s with routing key: %s", queueName, routingKey)
	return nil
}

// isRetryableError determines if an error is retryable
func isRetryableError(err error) bool {
	errStr := strings.ToLower(err.Error())

	// Database connection errors are retryable
	if strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "network") ||
		strings.Contains(errStr, "dial") {
		return true
	}

	// Data validation errors are not retryable
	if strings.Contains(errStr, "invalid") ||
		strings.Contains(errStr, "missing") ||
		strings.Contains(errStr, "malformed") {
		return false
	}

	// Default to not retryable to prevent infinite loops
	return false
}

// Close closes the subscriber connection
func (s *Subscriber) Close() error {
	// Update connection status
	UpdateConnectionStatus(s.serviceName, "subscriber", false)

	var errs []error
	if s.channel != nil {
		if err := s.channel.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing channel: %w", err))
		}
	}
	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing connection: %w", err))
		}
	}
	return errors.Join(errs...)
}
