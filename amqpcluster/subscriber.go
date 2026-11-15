package amqpcluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	"github.com/dobrevit/svckit/internal/inflight"
	"github.com/dobrevit/svckit/internal/retry"
	"github.com/dobrevit/svckit/logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ClusterSubscriber wraps the cluster adapter for event consumption
type ClusterSubscriber struct {
	cluster     *ClusterAdapter
	serviceName string
	verifier    *eventbus.EventVerifier
	exchange    string

	// signingRequired is Config.SigningRequired: unsigned messages are
	// rejected, and no subscription may opt out with AllowUnsigned.
	signingRequired bool

	// Background task management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// handlers tracks the handlers running, for Close to drain.
	handlers     *inflight.Tracker
	closeTimeout time.Duration
}

// DefaultCloseTimeout is how long Close waits for running handlers before
// it cancels their context.
const DefaultCloseTimeout = 10 * time.Second

// SubscriberOption adjusts a ClusterSubscriber when it is created.
type SubscriberOption func(*ClusterSubscriber)

// WithCloseTimeout sets how long Close waits for handlers still running
// before it cancels their context (default DefaultCloseTimeout). A handler
// cancelled this way has its message requeued.
func WithCloseTimeout(d time.Duration) SubscriberOption {
	return func(cs *ClusterSubscriber) { cs.closeTimeout = d }
}

// NewClusterSubscriber creates a new cluster-aware event subscriber. With a
// signing config it verifies signed messages and dead-letters unsigned ones;
// see SubscriptionOptions.AllowUnsigned for the exception.
func NewClusterSubscriber(nodes []string, serviceName string, signingConfig *eventbus.SignatureConfig, opts ...SubscriberOption) (*ClusterSubscriber, error) {
	return newClusterSubscriber(ClusterConfig{
		Nodes:               nodes,
		ServiceName:         serviceName,
		HealthCheckInterval: 30 * time.Second,
		RetryAttempts:       3,
		LoadBalanceStrategy: "round_robin",
	}, signingConfig, false, opts)
}

// NewClusterSubscriberWithConfig creates a subscriber from a Config, as
// LoadConfigFromEnv returns it: its nodes, health interval, retries and load
// balancing, its signing key, and SigningRequired, which NewClusterSubscriber
// has no way to take. With SigningRequired, unsigned messages are
// dead-lettered and a subscription asking for AllowUnsigned is refused, so a
// deployment's policy cannot be relaxed in code.
func NewClusterSubscriberWithConfig(config Config, opts ...SubscriberOption) (*ClusterSubscriber, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	var signingConfig *eventbus.SignatureConfig
	if config.EventSigningKey != "" {
		signingConfig = eventbus.DefaultSignatureConfig()
		signingConfig.SigningKey = []byte(config.EventSigningKey)
	} else if config.SigningRequired {
		return nil, errors.New("event signing is required but no signing key is configured")
	}

	return newClusterSubscriber(ClusterConfig{
		Nodes:               config.Nodes,
		ServiceName:         config.ServiceName,
		HealthCheckInterval: config.HealthCheckInterval,
		RetryAttempts:       config.RetryAttempts,
		LoadBalanceStrategy: config.LoadBalanceStrategy,
	}, signingConfig, config.SigningRequired, opts)
}

func newClusterSubscriber(config ClusterConfig, signingConfig *eventbus.SignatureConfig, signingRequired bool, opts []SubscriberOption) (*ClusterSubscriber, error) {
	cluster, err := NewClusterAdapter(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster adapter: %w", err)
	}
	subscriber := subscriberOn(cluster, config.ServiceName, signingConfig, signingRequired)
	for _, opt := range opts {
		opt(subscriber)
	}
	return subscriber, nil
}

// subscriberOn builds a subscriber over an adapter that is already
// connected.
func subscriberOn(cluster *ClusterAdapter, serviceName string, signingConfig *eventbus.SignatureConfig, signingRequired bool) *ClusterSubscriber {

	// Initialize verifier if provided
	var verifier *eventbus.EventVerifier
	if signingConfig != nil {
		verifier = eventbus.NewEventVerifier(signingConfig)
	}

	// Create context for background tasks
	ctx, cancel := context.WithCancel(context.Background())

	subscriber := &ClusterSubscriber{
		cluster:     cluster,
		serviceName: serviceName,
		verifier:    verifier,
		exchange:    eventbus.DefaultExchange,
		ctx:         ctx,
		cancel:      cancel,

		signingRequired: signingRequired,
		handlers:        inflight.New(),
		closeTimeout:    DefaultCloseTimeout,
	}

	return subscriber
}

// Subscribe subscribes to events with a specific routing pattern
func (cs *ClusterSubscriber) Subscribe(queueName, routingPattern string, handler eventbus.EventHandler) error {
	return cs.SubscribeWithOptions(queueName, routingPattern, handler, SubscriptionOptions{
		AutoAck:       false,
		Exclusive:     false,
		Durable:       true,
		AutoDelete:    false,
		PrefetchCount: 1,
	})
}

// SubscriptionOptions provides configuration for subscriptions
type SubscriptionOptions struct {
	AutoAck       bool
	Exclusive     bool
	Durable       bool
	AutoDelete    bool

	// PrefetchCount is how many unacknowledged messages the broker sends
	// each consumer (one per node) before it waits for an ack. Zero sets no
	// limit, so the broker sends every ready message at once; Subscribe uses 1.
	PrefetchCount int

	// TTL is the queue's x-message-ttl: a message that waits longer for a
	// consumer is dead-lettered. Zero keeps the default of five minutes;
	// eventbus.NoQueueTTL (any negative value) declares the queue without
	// one. The default stays five minutes because RabbitMQ refuses to
	// redeclare an existing queue with different arguments; see
	// eventbus.SubscribeOptions.QueueTTL.
	TTL time.Duration

	// Retry, when set, retries a message whose handler failed with a
	// retryable error after a delay, up to MaxAttempts calls, and then
	// dead-letters it, through retry queues and a DLQ routed by queue name;
	// see eventbus.SubscribeOptions.Retry, which behaves the same. The
	// republish goes through the adapter, so it is confirmed when the
	// adapter has PublisherConfirms, and the original is acknowledged only
	// after it. Enable it on a new queue: an existing one cannot be
	// redeclared with the dead-letter arguments it needs.
	Retry *eventbus.RetryPolicy

	// Context is the parent of every handler call's context; nil means
	// context.Background(). Close cancels the handlers either way, once its
	// timeout (WithCloseTimeout) has passed.
	Context context.Context

	// HandlerTimeout bounds one handler call; zero means no limit, as
	// before.
	HandlerTimeout time.Duration

	// AllowUnsigned lets unsigned messages reach the handler (with a Warn)
	// even though the subscriber verifies signatures. Without it they are
	// dead-lettered: the "signed" header is set by the sender, so trusting
	// its absence would let anyone who can publish skip verification. Use it
	// only while a queue's publishers migrate to signing. It is refused when
	// the subscriber was built with Config.SigningRequired.
	AllowUnsigned bool
}

// SubscribeWithOptions subscribes with custom options
func (cs *ClusterSubscriber) SubscribeWithOptions(queueName, routingPattern string, handler eventbus.EventHandler, opts SubscriptionOptions) error {
	if opts.AllowUnsigned && cs.signingRequired {
		return errors.New("AllowUnsigned conflicts with a subscriber that requires signing")
	}
	if opts.Retry != nil {
		if err := opts.Retry.Validate(); err != nil {
			return err
		}
	}

	// Setup queue infrastructure on all nodes
	if err := cs.setupQueue(queueName, routingPattern, opts); err != nil {
		return fmt.Errorf("failed to setup queue infrastructure: %w", err)
	}

	// Start consuming from all healthy nodes, and from any node that
	// connects or reconnects later.
	msgs, err := cs.cluster.subscribe(&subscription{
		queue:       queueName,
		consumerTag: fmt.Sprintf("%s-%s", cs.serviceName, queueName),
		autoAck:     opts.AutoAck,
		exclusive:   opts.Exclusive,
		prefetch:    opts.PrefetchCount,
	})
	if err != nil {
		return fmt.Errorf("failed to start cluster subscription: %w", err)
	}

	// Process messages asynchronously using background task management
	cs.wg.Add(1)
	go func() {
		defer cs.wg.Done()
		cs.processMessages(queueName, msgs, handler, opts)
	}()

	logging.Info("Subscribed to queue %s with pattern %s on cluster", queueName, routingPattern)
	return nil
}

// setupQueue creates queue and bindings on all nodes
func (cs *ClusterSubscriber) setupQueue(queueName, routingPattern string, opts SubscriptionOptions) error {
	// Create DLQ name
	dlqName := queueName + ".dlq"

	// Setup Dead Letter Queue first
	if err := cs.cluster.DeclareQueue(
		dlqName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // args
	); err != nil {
		return fmt.Errorf("failed to declare DLQ: %w", err)
	}

	// Bind DLQ to exchange. With a retry policy dead letters reach it
	// through the default exchange instead, so it stays off the topic
	// exchange.
	if opts.Retry == nil {
		if err := cs.cluster.BindQueue(dlqName, dlqName, cs.exchange, false, nil); err != nil {
			return fmt.Errorf("failed to bind DLQ: %w", err)
		}
	}

	// Setup main queue with DLQ configuration
	queueArgs := amqp.Table{
		"x-dead-letter-exchange":    cs.exchange,
		"x-dead-letter-routing-key": dlqName,
	}

	// The message TTL: the option, the five-minute default for zero, none
	// for a negative value.
	switch {
	case opts.TTL > 0:
		queueArgs["x-message-ttl"] = int64(opts.TTL / time.Millisecond)
	case opts.TTL == 0:
		queueArgs["x-message-ttl"] = int64(5 * time.Minute / time.Millisecond) // Default 5 minutes
	}
	if opts.Retry != nil {
		queueArgs = retry.MainQueueArgs(queueName, queueArgs)
		for name, args := range retry.RetryQueues(queueName, *opts.Retry) {
			if err := cs.cluster.DeclareQueue(name, opts.Durable, opts.AutoDelete, false, false, args); err != nil {
				return fmt.Errorf("failed to declare retry queue %s: %w", name, err)
			}
		}
	}

	if err := cs.cluster.DeclareQueue(
		queueName,
		opts.Durable,
		opts.AutoDelete,
		opts.Exclusive,
		false, // no-wait
		queueArgs,
	); err != nil {
		return fmt.Errorf("failed to declare main queue: %w", err)
	}

	// Bind main queue to exchange
	if err := cs.cluster.BindQueue(queueName, routingPattern, cs.exchange, false, nil); err != nil {
		return fmt.Errorf("failed to bind main queue: %w", err)
	}

	return nil
}

// processMessages handles incoming messages from the cluster with context cancellation
func (cs *ClusterSubscriber) processMessages(queueName string, msgs <-chan amqp.Delivery, handler eventbus.EventHandler, opts SubscriptionOptions) {
	for {
		select {
		case delivery, ok := <-msgs:
			if !ok {
				logging.Info("Messages channel closed, stopping message processing")
				return
			}
			// Once Close has begun, a message already delivered goes back to
			// the queue instead of to the handler.
			if !cs.handlers.Begin() {
				if !opts.AutoAck {
					nack(delivery, true)
				}
				continue
			}
			cs.handleMessage(queueName, delivery, handler, opts)
			cs.handlers.End()
		case <-cs.ctx.Done():
			logging.Info("Message processing stopped due to context cancellation")
			return
		}
	}
}

// handleMessage processes a single message with error handling
func (cs *ClusterSubscriber) handleMessage(queueName string, delivery amqp.Delivery, handler eventbus.EventHandler, opts SubscriptionOptions) {
	autoAck := opts.AutoAck
	defer func() {
		if r := recover(); r != nil {
			logging.Error("Panic while handling message: %v", r)
			if !autoAck {
				nack(delivery, false) // Send to DLQ on panic
			}
		}
	}()

	event, err := decodeDelivery(delivery, cs.verifier, cs.signingRequired, opts.AllowUnsigned)
	if err != nil {
		var r *rejection
		if !errors.As(err, &r) {
			r = &rejection{reason: reasonParseError, eventType: "unknown", err: err}
		}
		if r.reason == reasonParseError {
			logging.Error("Failed to parse message: %v", err)
		} else {
			logging.Error("🚨 SECURITY ALERT: %v", err)
		}
		eventbus.RecordEventDLQ(r.eventType, cs.serviceName, queueName, r.reason)
		if !autoAck {
			nack(delivery, false) // Dead-letter: redelivery cannot change the outcome
		}
		return
	}
	if signed, _ := delivery.Headers["signed"].(bool); signed {
		logging.Debug("Verified signed event: %s (ID: %s)", event.Type, event.ID)
	}

	// Create context for handler
	ctx, cancel := cs.handlers.HandlerContext(opts.Context, opts.HandlerTimeout)
	defer cancel()
	ctx = eventbus.WithAttempt(ctx, retry.Attempt(delivery))
	if userID, ok := delivery.Headers["user_id"]; ok {
		if userIDStr, ok := userID.(string); ok && userIDStr != "" {
			ctx = eventbus.WithUserID(ctx, userIDStr)
		}
	}

	// Call the event handler
	if err := handler.Handle(ctx, event); err != nil {
		logging.Error("Handler failed to process event %s (ID: %s): %v", event.Type, event.ID, err)
		// Cancelled by Close: the work was not finished, so the message goes
		// back to the queue whatever the error says.
		if !autoAck && cs.handlers.Cancelled().Err() != nil {
			nack(delivery, true)
			return
		}
		if !autoAck && opts.Retry != nil {
			cs.retryOrDeadLetter(*opts.Retry, delivery, event, queueName, cs.isRetryableError(err))
			return
		}
		if !autoAck {
			if cs.isRetryableError(err) {
				nack(delivery, true) // Requeue for retry
			} else {
				nack(delivery, false) // Send to DLQ
			}
		}
		return
	}

	// Acknowledge successful processing
	if !autoAck {
		ack(delivery)
	}
}

// retryOrDeadLetter sends a message whose handler failed to its retry queue
// or the DLQ, as the policy says, and acknowledges it only once that publish
// has succeeded (and, with PublisherConfirms, been confirmed). If the
// publish fails the message is requeued, so it is handled again rather than
// lost.
func (cs *ClusterSubscriber) retryOrDeadLetter(policy eventbus.RetryPolicy, delivery amqp.Delivery, event *eventbus.BaseEvent, queueName string, retryable bool) {
	target, msg := policy.Next(delivery, queueName, retryable)
	if err := cs.cluster.PublishWithContext(context.Background(), "", target, true, false, msg); err != nil {
		logging.Error("Failed to move event %s (ID: %s) to %s, requeueing it: %v", event.Type, event.ID, target, err)
		nack(delivery, true)
		return
	}
	if target == retry.DLQName(queueName) {
		eventbus.RecordEventDLQ(event.Type, cs.serviceName, queueName, "processing_error")
	} else {
		eventbus.RecordEventRequeued(event.Type, cs.serviceName, queueName)
	}
	ack(delivery)
}

// isRetryableError decides whether a handler's error requeues the message:
// what the handler said with eventbus.Retryable or eventbus.Permanent, or,
// for an error with neither mark, guessedRetryable.
func (cs *ClusterSubscriber) isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if retryable, decided := eventbus.IsRetryable(err); decided {
		return retryable
	}
	return guessedRetryable(err)
}

// guessedRetryable is the fallback for an unmarked error: it looks for words
// in the error's text, so a message that happens to mention "connection" is
// retried. Its list differs from eventbus's, and the match is
// case-sensitive. Deprecated: mark errors with eventbus.Retryable or
// eventbus.Permanent instead; this fallback will be removed in a later
// release.
func guessedRetryable(err error) bool {
	errStr := err.Error()

	// Retryable: temporary network, connection, timeout errors
	retryableKeywords := []string{
		"connection", "timeout", "network", "dial", "temporary",
		"refused", "unavailable", "deadline exceeded",
	}

	for _, keyword := range retryableKeywords {
		if contains(errStr, keyword) {
			return true
		}
	}

	// Non-retryable: validation, parsing, security errors
	nonRetryableKeywords := []string{
		"invalid", "missing", "malformed", "signature", "verification",
		"parse", "unmarshal", "marshal", "json",
	}

	for _, keyword := range nonRetryableKeywords {
		if contains(errStr, keyword) {
			return false
		}
	}

	// Default to non-retryable to prevent infinite loops
	return false
}

// contains checks if a string contains a substring. It is case-sensitive.
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			len(s) > len(substr) &&
				(s[:len(substr)] == substr ||
					s[len(s)-len(substr):] == substr ||
					containsInner(s, substr)))
}

func containsInner(s, substr string) bool {
	for i := 1; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// GetStats returns subscription statistics
func (cs *ClusterSubscriber) GetStats() SubscriberStats {
	nodeStats := cs.cluster.GetNodeStats()

	stats := SubscriberStats{
		ServiceName:  cs.serviceName,
		Exchange:     cs.exchange,
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

// SubscriberStats holds statistics for the cluster subscriber
type SubscriberStats struct {
	ServiceName  string
	Exchange     string
	NodeStats    map[string]NodeStats
	HealthyNodes int
	TotalNodes   int
}

// Close gracefully shuts down the subscriber
// Close stops the subscriber. It stops handing messages to handlers (one
// already delivered goes back to its queue), cancels its consumers, and
// waits for running handlers up to the close timeout (WithCloseTimeout);
// then it cancels their context, gives them up to a second more, and closes
// the adapter. A handler that returns after the cancellation has its
// message requeued, and the broker requeues the message of one that never
// returns when the channel closes. Close returns within the close timeout
// plus that second.
func (cs *ClusterSubscriber) Close() error {
	cs.handlers.Stop()
	if cs.cluster != nil {
		cs.cluster.cancelConsumers()
	}
	if !cs.handlers.Drain(cs.closeTimeout) {
		logging.Warn("A handler did not return within the close timeout and after cancellation; its message is requeued when the channel closes")
	}

	// Stop the processing loops. A loop still inside a handler that ignored
	// its context is left behind; closing the adapter closes its delivery
	// channel, so it ends when that handler does.
	cs.cancel()
	done := make(chan struct{})
	go func() {
		cs.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(inflight.Grace):
	}

	// Close the cluster adapter
	if cs.cluster != nil {
		return cs.cluster.Close()
	}
	return nil
}

// ack and nack record the outcome of a delivery and log a failure to do so.
// A failed acknowledgement is not cosmetic: the broker never learns the
// message was handled, so it redelivers it, and the duplicate surfaces far
// from here. Logging is all that can be done — the channel is already in
// trouble if these fail.
func ack(delivery amqp.Delivery) {
	if err := delivery.Ack(false); err != nil {
		logging.Error("Failed to ack message (it will be redelivered): %v", err)
	}
}

func nack(delivery amqp.Delivery, requeue bool) {
	if err := delivery.Nack(false, requeue); err != nil {
		logging.Error("Failed to nack message (it will be redelivered): %v", err)
	}
}
