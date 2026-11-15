// Package amqpconfirm publishes on a RabbitMQ channel in confirm mode and
// returns, for each message, what the broker did with it: stored it (nil),
// refused it (ErrNacked), could not route it to any queue (ErrUnroutable), or
// did not answer in time (ErrConfirmTimeout). Without confirms a nil error
// from a publish means only that the frame reached the socket.
//
// It is the one implementation behind eventbus.WithConfirms and
// amqpcluster.ClusterConfig.PublisherConfirms.
package amqpconfirm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// DefaultTimeout is how long a publish waits for the broker's confirmation
// when no timeout is given.
const DefaultTimeout = 5 * time.Second

var (
	// ErrUnroutable is matched (errors.Is) by a *ReturnedError: the message
	// was published mandatory and no queue was bound to receive it.
	ErrUnroutable = errors.New("amqp: the broker returned the message as unroutable")

	// ErrNacked means the broker refused to take responsibility for the
	// message, for instance a queue at its length limit with
	// x-overflow: reject-publish.
	ErrNacked = errors.New("amqp: the broker nacked the message")

	// ErrConfirmTimeout means no confirmation arrived in time. The broker may
	// still have stored the message, so a retry can duplicate it.
	ErrConfirmTimeout = errors.New("amqp: timed out waiting for the broker to confirm the message")

	// ErrNotSent wraps an error from the publish itself: the message never
	// reached a broker, so it is safe to try another.
	ErrNotSent = errors.New("amqp: the message was not sent")
)

// ReturnedError is a message the broker returned as unroutable.
type ReturnedError struct {
	ReplyCode  uint16
	ReplyText  string
	Exchange   string
	RoutingKey string
}

func (e *ReturnedError) Error() string {
	return fmt.Sprintf("%v: %d %s (exchange %q, routing key %q)",
		ErrUnroutable, e.ReplyCode, e.ReplyText, e.Exchange, e.RoutingKey)
}

// Is makes errors.Is(err, ErrUnroutable) true for a *ReturnedError.
func (e *ReturnedError) Is(target error) bool { return target == ErrUnroutable }

// Status is the outcome label of a publish for metrics: "success",
// "returned", "nacked", "timeout" or "error".
func Status(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, ErrUnroutable):
		return "returned"
	case errors.Is(err, ErrNacked):
		return "nacked"
	case errors.Is(err, ErrConfirmTimeout):
		return "timeout"
	default:
		return "error"
	}
}

// Confirmation is the broker's pending answer to one publish;
// *amqp.DeferredConfirmation satisfies it.
type Confirmation interface {
	Done() <-chan struct{}
	Acked() bool
}

// Channel is what a Publisher needs of a channel. AMQPChannel adapts
// *amqp.Channel to it.
type Channel interface {
	Confirm(noWait bool) error
	NotifyReturn(c chan amqp.Return) chan amqp.Return
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	IsClosed() bool
	PublishDeferred(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (Confirmation, error)
}

// AMQPChannel is a *amqp.Channel with PublishDeferred, so it satisfies
// Channel and still offers everything else a channel does.
type AMQPChannel struct{ *amqp.Channel }

// PublishDeferred publishes with a deferred confirmation.
func (c AMQPChannel) PublishDeferred(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (Confirmation, error) {
	dc, err := c.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
	if err != nil {
		return nil, err
	}
	if dc == nil {
		return nil, errors.New("the channel is not in confirm mode")
	}
	return dc, nil
}

// Publisher publishes on one channel in confirm mode. It is safe for
// concurrent use; each Publish gets its own outcome.
type Publisher struct {
	ch      Channel
	timeout time.Duration

	// barrier lets a publish wait until the router has handled every return
	// it has received so far; see Publish.
	barrier chan chan struct{}
	routed  chan struct{} // closed when the router stops, with the channel

	mu       sync.Mutex
	waiting  map[string][]*waiter // by message id
	closeErr *amqp.Error
}

type waiter struct {
	returned *amqp.Return
}

// New puts ch in confirm mode and returns a Publisher on it. A timeout of
// zero or less means DefaultTimeout. The Publisher lives as long as the
// channel; after the channel closes every Publish fails, and a new channel
// needs a new Publisher.
func New(ch Channel, timeout time.Duration) (*Publisher, error) {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("put the channel in confirm mode: %w", err)
	}

	p := &Publisher{
		ch:      ch,
		timeout: timeout,
		barrier: make(chan chan struct{}),
		routed:  make(chan struct{}),
		waiting: make(map[string][]*waiter),
	}

	// Unbuffered on purpose. The client hands a basic.return to its
	// listeners on the connection's reader goroutine, and handles the ack
	// that follows it only after the hand-over; with no buffer, the
	// hand-over completes only when the router has the return. Combined with
	// the barrier, that is what makes a return visible to the publish whose
	// ack it preceded.
	returns := ch.NotifyReturn(make(chan amqp.Return))
	closes := ch.NotifyClose(make(chan *amqp.Error, 1))
	go p.route(returns, closes)
	return p, nil
}

func (p *Publisher) route(returns <-chan amqp.Return, closes <-chan *amqp.Error) {
	defer close(p.routed)
	for returns != nil || closes != nil {
		select {
		case r, ok := <-returns:
			if !ok {
				returns = nil
				continue
			}
			p.mu.Lock()
			for _, w := range p.waiting[r.MessageId] {
				if w.returned == nil {
					w.returned = &r
					break
				}
			}
			p.mu.Unlock()
		case e, ok := <-closes:
			if !ok {
				closes = nil
				continue
			}
			p.mu.Lock()
			p.closeErr = e
			p.mu.Unlock()
		case reply := <-p.barrier:
			close(reply)
		}
	}
}

// Publish publishes msg as mandatory and waits for the broker's
// confirmation, ctx, or the timeout. It returns nil once the broker has
// stored the message; a *ReturnedError (ErrUnroutable) if no queue took it;
// ErrNacked if the broker refused it; ErrConfirmTimeout; ctx.Err(); the
// channel's error if it closed first; or an error wrapping ErrNotSent if the
// publish itself failed. A message without a MessageId is given one, since
// returns are matched to publishes by it.
func (p *Publisher) Publish(ctx context.Context, exchange, key string, msg amqp.Publishing) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if msg.MessageId == "" {
		msg.MessageId = uuid.NewString()
	}

	w := &waiter{}
	p.register(msg.MessageId, w)
	defer p.unregister(msg.MessageId, w)

	confirmation, err := p.ch.PublishDeferred(ctx, exchange, key, true, false, msg)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("%w: %w", ErrNotSent, err)
	}

	timer := time.NewTimer(p.timeout)
	defer timer.Stop()
	select {
	case <-confirmation.Done():
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ErrConfirmTimeout
	}

	acked := confirmation.Acked()
	// Closing a channel resolves every pending confirmation as not acked.
	// The client marks the channel closed before it does that, so a closed
	// channel here means the close, not a nack, settled this one.
	if !acked && p.ch.IsClosed() {
		return p.closedError()
	}

	// A return arrives before its ack. Waiting for the router to get past
	// everything it has received means a return for this message, if there
	// was one, has been recorded on w.
	p.sync()

	p.mu.Lock()
	returned := w.returned
	p.mu.Unlock()
	if returned != nil {
		return &ReturnedError{
			ReplyCode:  returned.ReplyCode,
			ReplyText:  returned.ReplyText,
			Exchange:   returned.Exchange,
			RoutingKey: returned.RoutingKey,
		}
	}
	if !acked {
		return ErrNacked
	}
	return nil
}

func (p *Publisher) sync() {
	reply := make(chan struct{})
	select {
	case p.barrier <- reply:
		<-reply
	case <-p.routed:
	}
}

func (p *Publisher) register(id string, w *waiter) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.waiting[id] = append(p.waiting[id], w)
}

func (p *Publisher) unregister(id string, w *waiter) {
	p.mu.Lock()
	defer p.mu.Unlock()
	waiters := p.waiting[id]
	for i, other := range waiters {
		if other == w {
			waiters = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
	if len(waiters) == 0 {
		delete(p.waiting, id)
	} else {
		p.waiting[id] = waiters
	}
}

func (p *Publisher) closedError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closeErr != nil {
		return fmt.Errorf("the channel closed before the broker confirmed the message: %w", p.closeErr)
	}
	return fmt.Errorf("the channel closed before the broker confirmed the message: %w", amqp.ErrClosed)
}
