package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dobrevit/svckit/internal/amqpconfirm"
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
	deliveryMode   uint8

	confirmTimeout time.Duration
	confirms       bool
	confirmer      *amqpconfirm.Publisher // set when confirms is
}

// The outcomes of a publish on a publisher with confirms (WithConfirms). They
// are also what amqpcluster's publishers return with
// ClusterConfig.PublisherConfirms.
var (
	// ErrUnroutable is matched (errors.Is) by a *ReturnedError: no queue was
	// bound to receive the message, so the broker returned it.
	ErrUnroutable = amqpconfirm.ErrUnroutable

	// ErrNacked means the broker refused the message, for instance a queue at
	// its length limit with x-overflow: reject-publish.
	ErrNacked = amqpconfirm.ErrNacked

	// ErrConfirmTimeout means no confirmation arrived within the timeout. The
	// broker may still have stored the message; a retry can duplicate it,
	// which consumers that deduplicate by message id absorb.
	ErrConfirmTimeout = amqpconfirm.ErrConfirmTimeout
)

// ReturnedError is a message the broker returned as unroutable, with its
// reply code and text.
type ReturnedError = amqpconfirm.ReturnedError

// PublisherOption adjusts a Publisher when it is created.
type PublisherOption func(*Publisher)

// WithTransient publishes messages as transient (delivery mode 1) instead of
// persistent: they cost no disk write, and a broker restart loses them even
// from a durable queue. Persistent is the default because the exchange and
// queues svckit declares are durable, and a durable queue of transient
// messages is almost never what was meant; use this for messages that are
// worthless after a restart, such as cache invalidations.
func WithTransient() PublisherOption {
	return func(p *Publisher) { p.deliveryMode = amqp.Transient }
}

// WithConfirms makes Publish return only once the broker has confirmed the
// message, so a nil error means the broker stored it rather than that it
// left this process. Messages are published mandatory: one that no queue is
// bound to receive comes back as ErrUnroutable instead of being dropped. A
// refused message is ErrNacked, no answer within timeout is
// ErrConfirmTimeout (zero means five seconds), and a cancelled ctx returns
// ctx.Err(). Each publish waits for one round trip to the broker.
func WithConfirms(timeout time.Duration) PublisherOption {
	return func(p *Publisher) {
		p.confirms = true
		p.confirmTimeout = timeout
	}
}

// NewPublisher creates a new event publisher
func NewPublisher(rabbitmqURL string, opts ...PublisherOption) (*Publisher, error) {
	return NewPublisherWithService(rabbitmqURL, "unknown", opts...)
}

// NewPublisherWithService creates a new event publisher with service name
func NewPublisherWithService(rabbitmqURL, serviceName string, opts ...PublisherOption) (*Publisher, error) {
	return NewPublisherWithSigning(rabbitmqURL, serviceName, nil, false, opts...)
}

// NewPublisherWithSigning creates a new event publisher with optional message
// signing. Messages are persistent unless WithTransient is given.
func NewPublisherWithSigning(rabbitmqURL, serviceName string, signingConfig *SignatureConfig, requireSigning bool, opts ...PublisherOption) (*Publisher, error) {
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
		deliveryMode:   amqp.Persistent,
	}
	for _, opt := range opts {
		opt(publisher)
	}
	if publisher.confirms {
		confirmer, err := amqpconfirm.New(amqpconfirm.AMQPChannel{Channel: ch}, publisher.confirmTimeout)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		publisher.confirmer = confirmer
	}

	// Update connection status
	UpdateConnectionStatus(serviceName, "publisher", true)

	return publisher, nil
}

// Publish publishes an event to the event bus with optional signing
func (p *Publisher) Publish(ctx context.Context, event *BaseEvent) error {
	// Check if signing is enabled
	if p.signer != nil {
		return p.PublishSigned(ctx, event)
	}

	// Publish unsigned event
	body, err := event.ToJSON()
	if err != nil {
		RecordEventPublished(event.Type, p.serviceName, "error")
		return err
	}

	headers := amqp.Table{
		"signed":     false,
		"source":     event.Source,
		"event_type": event.Type,
		"event_id":   event.ID,
	}

	if err := p.send(ctx, event, body, headers); err != nil {
		return err
	}
	logging.Warn("⚠️  Published unsigned event: %s (ID: %s)", event.Type, event.ID)
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

	if err := p.send(ctx, event, body, headers); err != nil {
		return err
	}
	logging.Info("✅ Published signed event: %s (ID: %s, Signature: %.8s...)",
		event.Type, event.ID, signedEvent.Signature)
	return nil
}

// send publishes one message to the default exchange, routed by the event
// type, waits for the broker's confirmation when confirms are on, and
// records the outcome.
func (p *Publisher) send(ctx context.Context, event *BaseEvent, body []byte, headers amqp.Table) error {
	msg := amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		Headers:      headers,
		Timestamp:    time.Now(),
		MessageId:    event.ID,
		DeliveryMode: p.deliveryMode,
	}

	var err error
	if p.confirmer != nil {
		err = p.confirmer.Publish(ctx, DefaultExchange, event.Type, msg)
	} else {
		err = p.channel.Publish(DefaultExchange, event.Type, false, false, msg)
	}

	RecordEventPublished(event.Type, p.serviceName, amqpconfirm.Status(err))
	return err
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

// SubscribeOptions adjusts one subscription. The zero value is what
// Subscribe uses.
type SubscribeOptions struct {
	// AllowUnsigned lets unsigned messages reach the handler (with a Warn)
	// even though the subscriber has a verifier. A subscriber with a verifier
	// otherwise dead-letters them: the "signed" header is set by the sender,
	// so trusting its absence would let anyone who can publish skip
	// verification. Use it only while the publishers of a queue migrate to
	// signing. It is refused when the subscriber requires signing.
	AllowUnsigned bool
}

// Subscribe subscribes to events with the given routing key
func (s *Subscriber) Subscribe(queueName string, routingKey string, handler EventHandler) error {
	return s.SubscribeWithOptions(queueName, routingKey, handler, SubscribeOptions{})
}

// SubscribeWithOptions is Subscribe with per-subscription options.
func (s *Subscriber) SubscribeWithOptions(queueName string, routingKey string, handler EventHandler, opts SubscribeOptions) error {
	if opts.AllowUnsigned && s.requireSigning {
		return errors.New("AllowUnsigned conflicts with a subscriber that requires signing")
	}

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

				event, err := decodeDelivery(d, s.verifier, s.requireSigning, opts.AllowUnsigned)
				if err != nil {
					var r *rejection
					if !errors.As(err, &r) {
						r = &rejection{reason: reasonParseError, eventType: "unknown", err: err}
					}
					if r.reason == reasonParseError {
						logging.Error("Error parsing event: %v", err)
					} else {
						logging.Error("🚨 SECURITY ALERT: %v", err)
					}
					RecordEventDLQ(r.eventType, s.serviceName, queueName, r.reason)
					nack(d, false) // Dead-letter: redelivery cannot change the outcome
					return
				}
				if signed, _ := d.Headers["signed"].(bool); signed {
					logging.Info("✅ Verified signed event: %s (ID: %s)", event.Type, event.ID)
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
