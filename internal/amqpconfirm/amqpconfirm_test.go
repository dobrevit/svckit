package amqpconfirm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// outcome is what the fake broker does with a message.
type outcome int

const (
	ack outcome = iota
	nack
	returnThenAck // what RabbitMQ does with an unroutable mandatory message
	silence       // never confirms
)

type fakeConfirmation struct {
	done chan struct{}
	ack  bool
}

func (c *fakeConfirmation) Done() <-chan struct{} { return c.done }

func (c *fakeConfirmation) Acked() bool {
	select {
	case <-c.done:
		return c.ack
	default:
		return false
	}
}

// fakeChannel reproduces the ordering amqp091 gives a real channel: a return
// is handed to the listener synchronously and only then is the confirmation
// that follows it resolved; closing marks the channel closed, then resolves
// every pending confirmation as not acked.
type fakeChannel struct {
	decide func(msg amqp.Publishing) outcome

	confirmed atomic.Bool
	closed    atomic.Bool
	returns   chan amqp.Return
	closes    chan *amqp.Error

	mu      sync.Mutex
	pending []*fakeConfirmation
	// serial keeps one "reader goroutine": the broker's frames for a channel
	// are handled in order.
	serial sync.Mutex
}

func (c *fakeChannel) Confirm(bool) error {
	c.confirmed.Store(true)
	return nil
}

func (c *fakeChannel) NotifyReturn(ch chan amqp.Return) chan amqp.Return {
	c.returns = ch
	return ch
}

func (c *fakeChannel) NotifyClose(ch chan *amqp.Error) chan *amqp.Error {
	c.closes = ch
	return ch
}

func (c *fakeChannel) IsClosed() bool { return c.closed.Load() }

func (c *fakeChannel) PublishDeferred(_ context.Context, exchange, key string, mandatory, _ bool, msg amqp.Publishing) (Confirmation, error) {
	if c.closed.Load() {
		return nil, amqp.ErrClosed
	}
	if !mandatory {
		return nil, errors.New("published without mandatory: returns would be lost")
	}
	conf := &fakeConfirmation{done: make(chan struct{})}
	c.mu.Lock()
	c.pending = append(c.pending, conf)
	c.mu.Unlock()

	switch c.decide(msg) {
	case ack:
		go c.reader(func() { c.settle(conf, true) })
	case nack:
		go c.reader(func() { c.settle(conf, false) })
	case returnThenAck:
		go c.reader(func() {
			c.returns <- amqp.Return{ReplyCode: 312, ReplyText: "NO_ROUTE", Exchange: exchange, RoutingKey: key, MessageId: msg.MessageId}
			c.settle(conf, true)
		})
	case silence:
	}
	return conf, nil
}

func (c *fakeChannel) reader(f func()) {
	c.serial.Lock()
	defer c.serial.Unlock()
	f()
}

func (c *fakeChannel) settle(conf *fakeConfirmation, ack bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, p := range c.pending {
		if p == conf {
			c.pending = append(c.pending[:i], c.pending[i+1:]...)
			conf.ack = ack
			close(conf.done)
			return
		}
	}
}

// close shuts the channel the way amqp091 does.
func (c *fakeChannel) close(reason *amqp.Error) {
	c.serial.Lock()
	defer c.serial.Unlock()
	c.closed.Store(true)
	if reason != nil {
		c.closes <- reason
	}
	c.mu.Lock()
	for _, p := range c.pending {
		p.ack = false
		close(p.done)
	}
	c.pending = nil
	c.mu.Unlock()
	close(c.returns)
	close(c.closes)
}

func newTestPublisher(t *testing.T, timeout time.Duration, decide func(amqp.Publishing) outcome) (*Publisher, *fakeChannel) {
	t.Helper()
	ch := &fakeChannel{decide: decide}
	p, err := New(ch, timeout)
	if err != nil {
		t.Fatal(err)
	}
	if !ch.confirmed.Load() {
		t.Fatal("New did not put the channel in confirm mode")
	}
	return p, ch
}

func always(o outcome) func(amqp.Publishing) outcome {
	return func(amqp.Publishing) outcome { return o }
}

func publish(p *Publisher, id string) error {
	return p.Publish(context.Background(), "events", "order.created", amqp.Publishing{MessageId: id})
}

func TestAnAckedPublishSucceeds(t *testing.T) {
	p, _ := newTestPublisher(t, time.Second, always(ack))
	if err := publish(p, "m-1"); err != nil {
		t.Fatalf("Publish = %v, want nil after the ack", err)
	}
}

func TestANackIsErrNacked(t *testing.T) {
	p, _ := newTestPublisher(t, time.Second, always(nack))
	if err := publish(p, "m-1"); !errors.Is(err, ErrNacked) {
		t.Fatalf("Publish = %v, want ErrNacked", err)
	}
}

// RabbitMQ acks an unroutable mandatory message after returning it, so the
// ack alone says nothing; the return has to be matched to the publish.
func TestAReturnBeforeTheAckIsErrUnroutable(t *testing.T) {
	p, _ := newTestPublisher(t, time.Second, always(returnThenAck))

	err := publish(p, "m-1")
	var returned *ReturnedError
	if !errors.As(err, &returned) || !errors.Is(err, ErrUnroutable) {
		t.Fatalf("Publish = %v, want a *ReturnedError matching ErrUnroutable", err)
	}
	if returned.ReplyCode != 312 || returned.ReplyText != "NO_ROUTE" || returned.RoutingKey != "order.created" {
		t.Errorf("returned = %+v, want the broker's reply code, text and routing key", returned)
	}
}

// Many publishes at once, half of them unroutable: each must get its own
// outcome, never a neighbour's return and never a missed one.
func TestConcurrentPublishesEachGetTheirOwnOutcome(t *testing.T) {
	p, _ := newTestPublisher(t, 5*time.Second, func(msg amqp.Publishing) outcome {
		if strings.HasSuffix(msg.MessageId, "-unroutable") {
			return returnThenAck
		}
		return ack
	})

	const n = 1000
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("m-%d", i)
			if i%2 == 1 {
				id += "-unroutable"
			}
			errs[i] = publish(p, id)
		}()
	}
	wg.Wait()

	for i, err := range errs {
		wantReturned := i%2 == 1
		if gotReturned := errors.Is(err, ErrUnroutable); gotReturned != wantReturned || (!wantReturned && err != nil) {
			t.Fatalf("publish %d: err = %v, want returned=%v", i, err, wantReturned)
		}
	}
}

func TestNoConfirmationIsErrConfirmTimeout(t *testing.T) {
	p, _ := newTestPublisher(t, 50*time.Millisecond, always(silence))

	start := time.Now()
	err := publish(p, "m-1")
	if !errors.Is(err, ErrConfirmTimeout) {
		t.Fatalf("Publish = %v, want ErrConfirmTimeout", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("the timeout took %v, want about 50ms", elapsed)
	}
}

func TestACancelledContextReturnsItsError(t *testing.T) {
	p, _ := newTestPublisher(t, time.Minute, always(silence))
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)

	if err := p.Publish(ctx, "events", "order.created", amqp.Publishing{MessageId: "m-1"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish = %v, want context.Canceled", err)
	}

	done, cancelDone := context.WithCancel(context.Background())
	cancelDone()
	if err := p.Publish(done, "events", "order.created", amqp.Publishing{MessageId: "m-2"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish with a context already done = %v, want context.Canceled", err)
	}
}

// A channel closing resolves the pending confirmation as not acked, which
// must not be mistaken for the broker's nack.
func TestAChannelClosedBeforeTheConfirmationIsTheChannelError(t *testing.T) {
	p, ch := newTestPublisher(t, time.Minute, always(silence))
	reason := &amqp.Error{Code: 320, Reason: "CONNECTION_FORCED"}

	result := make(chan error, 1)
	go func() { result <- publish(p, "m-1") }()
	time.Sleep(20 * time.Millisecond)
	ch.close(reason)

	err := <-result
	if errors.Is(err, ErrNacked) {
		t.Fatalf("Publish = %v; a closed channel was reported as a nack", err)
	}
	if !strings.Contains(err.Error(), "channel closed") {
		t.Errorf("Publish = %v, want the channel's close named", err)
	}

	if err := publish(p, "m-2"); !errors.Is(err, ErrNotSent) {
		t.Errorf("Publish after the close = %v, want ErrNotSent", err)
	}
}

func TestAMessageWithoutAnIDIsGivenOne(t *testing.T) {
	var seen atomic.Value
	p, _ := newTestPublisher(t, time.Second, func(msg amqp.Publishing) outcome {
		seen.Store(msg.MessageId)
		return returnThenAck
	})

	if err := p.Publish(context.Background(), "events", "x", amqp.Publishing{}); !errors.Is(err, ErrUnroutable) {
		t.Fatalf("Publish = %v, want the return matched by the id it was given", err)
	}
	if id, _ := seen.Load().(string); id == "" {
		t.Error("the message was published without a MessageId")
	}
}

func TestStatus(t *testing.T) {
	cases := map[string]error{
		"success":  nil,
		"returned": &ReturnedError{ReplyCode: 312},
		"nacked":   fmt.Errorf("publish: %w", ErrNacked),
		"timeout":  ErrConfirmTimeout,
		"error":    errors.New("boom"),
	}
	for want, err := range cases {
		if got := Status(err); got != want {
			t.Errorf("Status(%v) = %q, want %q", err, got, want)
		}
	}
}
