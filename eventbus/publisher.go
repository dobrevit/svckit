package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dobrevit/svckit/internal/amqpconfirm"
	"github.com/dobrevit/svckit/internal/inflight"
	"github.com/dobrevit/svckit/internal/retry"
	"github.com/dobrevit/svckit/logging"
	"github.com/google/uuid"
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
	logging.Debug("Published unsigned event: %s (ID: %s)", event.Type, event.ID)
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
	logging.Debug("Published signed event: %s (ID: %s, Signature: %.8s...)",
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

	// retrier republishes failed messages for a RetryPolicy, on a channel of
	// its own in confirm mode; created by the first subscription with one.
	retrierMu sync.Mutex
	retrier   *amqpconfirm.Publisher

	// handlers tracks the handlers running, for Close to drain.
	handlers     *inflight.Tracker
	closeTimeout time.Duration

	consumersMu sync.Mutex
	consumers   []string // consumer tags, for Close to cancel
}

// DefaultCloseTimeout is how long Close waits for running handlers before
// it cancels their context.
const DefaultCloseTimeout = 10 * time.Second

// DefaultHandlerTimeout bounds one call of an eventbus handler unless
// SubscribeOptions.HandlerTimeout says otherwise.
const DefaultHandlerTimeout = 30 * time.Second

// SubscriberOption adjusts a Subscriber when it is created.
type SubscriberOption func(*Subscriber)

// WithCloseTimeout sets how long Close waits for handlers still running
// before it cancels their context (default DefaultCloseTimeout). A handler
// cancelled this way has its message requeued.
func WithCloseTimeout(d time.Duration) SubscriberOption {
	return func(s *Subscriber) { s.closeTimeout = d }
}

// RetryPolicy retries a message whose handler failed with a retryable error
// after a delay, a bounded number of times, and then dead-letters it. See
// SubscribeOptions.Retry.
type RetryPolicy = retry.Policy

// NewSubscriber creates a new event subscriber
func NewSubscriber(rabbitmqURL string, opts ...SubscriberOption) (*Subscriber, error) {
	return NewSubscriberWithService(rabbitmqURL, "unknown", opts...)
}

// NewSubscriberWithService creates a new event subscriber with service name
func NewSubscriberWithService(rabbitmqURL, serviceName string, opts ...SubscriberOption) (*Subscriber, error) {
	return NewSubscriberWithSigning(rabbitmqURL, serviceName, nil, false, opts...)
}

// NewSubscriberWithSigning creates a new event subscriber with optional signature verification
func NewSubscriberWithSigning(rabbitmqURL, serviceName string, signingConfig *SignatureConfig, requireSigning bool, opts ...SubscriberOption) (*Subscriber, error) {
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
		handlers:       inflight.New(),
		closeTimeout:   DefaultCloseTimeout,
	}
	for _, opt := range opts {
		opt(subscriber)
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

	// QueueTTL is the queue's x-message-ttl: a message that waits longer for
	// a consumer is dead-lettered. Zero keeps the default of five minutes;
	// NoQueueTTL (any negative value) declares the queue without one, so
	// messages wait for a consumer however long it is down.
	//
	// The default stays five minutes deliberately. RabbitMQ refuses to
	// redeclare an existing queue with different arguments
	// (PRECONDITION_FAILED, and the channel is closed), so changing it would
	// stop every existing consumer from starting against its own queue. A
	// new queue should choose its TTL here, or leave it to a broker policy,
	// which can change without a redeclare. An existing queue keeps the
	// arguments it was declared with until it is deleted.
	QueueTTL time.Duration

	// Retry, when set, retries a message whose handler failed with a
	// retryable error (see Retryable) after the policy's delay instead of
	// requeueing it at once, up to MaxAttempts calls, and then dead-letters
	// it; a permanent failure is dead-lettered at once. The handler reads
	// the attempt with Attempt(ctx). The subscriber declares a retry queue
	// per distinct delay (<queue>.retry.<delay>) and routes retries and dead
	// letters by queue name, so they never pass through the topic exchange
	// and its other bindings. A dead letter carries the headers
	// x-svckit-attempt and x-svckit-reason. The original is acknowledged
	// only after its republish is confirmed, so a crash in between can
	// duplicate a message but not lose it.
	//
	// The main queue's dead-letter arguments differ from those of a queue
	// without a policy, and RabbitMQ refuses to redeclare a queue with
	// different arguments: enable Retry on a new queue, not an existing one.
	Retry *RetryPolicy

	// Context is the parent of every handler call's context; nil means
	// context.Background(). Close cancels the handlers either way, once its
	// timeout (WithCloseTimeout) has passed.
	Context context.Context

	// HandlerTimeout bounds one handler call. Zero means
	// DefaultHandlerTimeout (30 seconds, as before); a negative value means
	// no limit.
	HandlerTimeout time.Duration
}

// NoQueueTTL as a QueueTTL (eventbus.SubscribeOptions,
// amqpcluster.SubscriptionOptions.TTL) declares a queue without a message
// TTL. Any negative duration means the same.
const NoQueueTTL time.Duration = -1

// defaultQueueTTL is the x-message-ttl a queue gets when no TTL is chosen.
// Its value must not change: the broker refuses a redeclare whose arguments
// differ from the queue's.
const defaultQueueTTL = 300000 // 5 minutes, in milliseconds

// queueArguments are the arguments of a subscriber's main queue.
func queueArguments(dlqName string, ttl time.Duration) amqp.Table {
	args := amqp.Table{
		"x-dead-letter-exchange":    DefaultExchange,
		"x-dead-letter-routing-key": dlqName,
	}
	switch {
	case ttl == 0:
		args["x-message-ttl"] = defaultQueueTTL
	case ttl > 0:
		args["x-message-ttl"] = int64(ttl / time.Millisecond)
	}
	return args
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
	var retrier *amqpconfirm.Publisher
	if opts.Retry != nil {
		if err := opts.Retry.Validate(); err != nil {
			return err
		}
		var err error
		if retrier, err = s.retryPublisher(); err != nil {
			return fmt.Errorf("open the retry channel: %w", err)
		}
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
	mainArgs := queueArguments(dlqName, opts.QueueTTL)
	if opts.Retry != nil {
		mainArgs = retry.MainQueueArgs(queueName, mainArgs)
	}
	q, err := s.channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		mainArgs,  // arguments
	)
	if err != nil {
		return err
	}

	if opts.Retry != nil {
		for name, args := range retry.RetryQueues(queueName, *opts.Retry) {
			if _, err := s.channel.QueueDeclare(name, true, false, false, false, args); err != nil {
				return fmt.Errorf("declare retry queue %s: %w", name, err)
			}
		}
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

	// Bind dead letter queue to exchange. With a retry policy dead letters
	// reach it through the default exchange instead, so it stays off the
	// topic exchange.
	if opts.Retry == nil {
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
	consumerTag := fmt.Sprintf("%s-%s-%s", s.serviceName, q.Name, uuid.NewString()[:8])
	msgs, err := s.channel.Consume(
		q.Name,      // queue
		consumerTag, // consumer, named so that Close can cancel it
		false,       // auto-ack (changed to false)
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return err
	}
	s.consumersMu.Lock()
	s.consumers = append(s.consumers, consumerTag)
	s.consumersMu.Unlock()

	handlerTimeout := opts.HandlerTimeout
	if handlerTimeout == 0 {
		handlerTimeout = DefaultHandlerTimeout
	}

	// Process messages with proper error handling
	go func() {
		for d := range msgs {
			// Once Close has begun, a message already delivered goes back to
			// the queue instead of to a handler.
			if !s.handlers.Begin() {
				nack(d, true)
				continue
			}
			func() {
				defer s.handlers.End()
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
					logging.Debug("Verified signed event: %s (ID: %s)", event.Type, event.ID)
				}

				// Record event received
				RecordEventReceived(event.Type, s.serviceName, queueName)

				// Per-message lines are Debug, and carry no user id: that is
				// personal data in most deployments.
				logging.Debug("Processing event: %s (ID: %s)", event.Type, event.ID)

				// Start timing the processing
				processingStart := time.Now()

				ctx, cancel := s.handlers.HandlerContext(opts.Context, handlerTimeout)
				defer cancel()
				ctx = WithAttempt(ctx, retry.Attempt(d))

				if err := handler.Handle(ctx, event); err != nil {
					logging.Error("Error handling event %s (ID: %s): %v", event.Type, event.ID, err)

					// Record processing duration even for failures
					EventProcessingDuration.WithLabelValues(event.Type, s.serviceName, queueName).Observe(time.Since(processingStart).Seconds())

					// Cancelled by Close: the work was not finished, so the
					// message goes back to the queue whatever the error says.
					if s.handlers.Cancelled().Err() != nil {
						RecordEventRequeued(event.Type, s.serviceName, queueName)
						RecordEventProcessed(event.Type, s.serviceName, queueName, "requeued")
						nack(d, true)
						return
					}

					if opts.Retry != nil {
						s.retryOrDeadLetter(retrier, *opts.Retry, d, event, queueName, isRetryableError(err))
						return
					}

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
				logging.Debug("Successfully processed event: %s (ID: %s)", event.Type, event.ID)
			}()
		}
	}()

	logging.Info("Subscribed to queue: %s with routing key: %s", queueName, routingKey)
	return nil
}

// retryOrDeadLetter sends a message whose handler failed to its retry queue
// or the DLQ, as the policy says, and acknowledges it only once that publish
// is confirmed. If the publish fails the message is requeued, so it is
// handled again rather than lost.
func (s *Subscriber) retryOrDeadLetter(retrier *amqpconfirm.Publisher, policy RetryPolicy, d amqp.Delivery, event *BaseEvent, queueName string, retryable bool) {
	target, msg := policy.Next(d, queueName, retryable)
	if err := retrier.Publish(context.Background(), "", target, msg); err != nil {
		logging.Error("Failed to move event %s (ID: %s) to %s, requeueing it: %v", event.Type, event.ID, target, err)
		RecordEventRequeued(event.Type, s.serviceName, queueName)
		RecordEventProcessed(event.Type, s.serviceName, queueName, "requeued")
		nack(d, true)
		return
	}
	if target == retry.DLQName(queueName) {
		RecordEventDLQ(event.Type, s.serviceName, queueName, "processing_error")
		RecordEventProcessed(event.Type, s.serviceName, queueName, "error")
	} else {
		RecordEventRequeued(event.Type, s.serviceName, queueName)
		RecordEventProcessed(event.Type, s.serviceName, queueName, "retry_scheduled")
	}
	ack(d)
}

// retryPublisher returns the subscriber's retry publisher, opening its
// channel the first time.
func (s *Subscriber) retryPublisher() (*amqpconfirm.Publisher, error) {
	s.retrierMu.Lock()
	defer s.retrierMu.Unlock()
	if s.retrier != nil {
		return s.retrier, nil
	}
	ch, err := s.conn.Channel()
	if err != nil {
		return nil, err
	}
	retrier, err := amqpconfirm.New(amqpconfirm.AMQPChannel{Channel: ch}, 0)
	if err != nil {
		_ = ch.Close()
		return nil, err
	}
	s.retrier = retrier
	return retrier, nil
}

// isRetryableError decides whether a handler's error requeues the message:
// what the handler said with Retryable or Permanent, or, for an error with
// neither mark, guessedRetryable.
func isRetryableError(err error) bool {
	if retry, decided := IsRetryable(err); decided {
		return retry
	}
	return guessedRetryable(err)
}

// guessedRetryable is the fallback for an unmarked error: it looks for words
// in the error's text, so a message that happens to mention "connection" is
// retried. Deprecated: mark errors with Retryable or Permanent instead; this
// fallback will be removed in a later release.
func guessedRetryable(err error) bool {
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
// Close stops the subscriber. It stops handing messages to handlers (one
// already delivered goes back to its queue), cancels its consumers, and
// waits for running handlers up to the close timeout (WithCloseTimeout);
// then it cancels their context, gives them up to a second more, and closes
// the channel. A handler that returns after the cancellation has its
// message requeued, and the broker requeues the message of one that never
// returns when the channel closes. Close returns within the close timeout
// plus that second.
func (s *Subscriber) Close() error {
	// Update connection status
	UpdateConnectionStatus(s.serviceName, "subscriber", false)

	if s.handlers != nil {
		s.handlers.Stop()
		s.consumersMu.Lock()
		consumers := append([]string(nil), s.consumers...)
		s.consumersMu.Unlock()
		if s.channel != nil {
			for _, tag := range consumers {
				if err := s.channel.Cancel(tag, false); err != nil {
					logging.Warn("Failed to cancel consumer %s: %v", tag, err)
				}
			}
		}
		if !s.handlers.Drain(s.closeTimeout) {
			logging.Warn("A handler did not return within the close timeout and after cancellation; its message is requeued when the channel closes")
		}
	}

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
