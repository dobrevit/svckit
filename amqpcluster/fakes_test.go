package amqpcluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/dobrevit/svckit/internal/amqpconfirm"
	amqp "github.com/rabbitmq/amqp091-go"
)

// fakeConnection stands in for a broker connection: it can be opened, report
// itself closed, and hand out channels that close with it.
type fakeConnection struct {
	closed atomic.Bool

	mu       sync.Mutex
	channels []*fakeChannel
}

func (c *fakeConnection) Channel() (amqpChannel, error) {
	if c.closed.Load() {
		return nil, errors.New("connection closed")
	}
	ch := &fakeChannel{conn: c}
	c.mu.Lock()
	c.channels = append(c.channels, ch)
	c.mu.Unlock()
	return ch, nil
}

func (c *fakeConnection) IsClosed() bool { return c.closed.Load() }

// Close closes the connection and, as a broker connection does, every
// channel on it.
func (c *fakeConnection) Close() error {
	c.closed.Store(true)
	c.mu.Lock()
	channels := append([]*fakeChannel(nil), c.channels...)
	c.mu.Unlock()
	for _, ch := range channels {
		_ = ch.Close()
	}
	return nil
}

// fakeChannel records what is done on it. Consume returns a deliveries
// channel the test feeds through deliver, and which closes with the channel.
type fakeChannel struct {
	conn   *fakeConnection
	closed atomic.Bool

	// confirm decides what the broker says to a publish in confirm mode;
	// nil acks everything. returns receives a return before the ack, as
	// RabbitMQ sends one for an unroutable mandatory message.
	confirm   func(msg amqp.Publishing) (ack, returned bool)
	confirms  atomic.Int32 // publishes made in confirm mode
	inConfirm atomic.Bool
	returns   chan amqp.Return
	closes    chan *amqp.Error

	mu        sync.Mutex
	qos       []int
	consumers map[string]chan amqp.Delivery // by consumer tag
	declared  []string
	closeOnce sync.Once
}

func (ch *fakeChannel) IsClosed() bool {
	return ch.closed.Load() || (ch.conn != nil && ch.conn.IsClosed())
}

func (ch *fakeChannel) Close() error {
	ch.closed.Store(true)
	ch.closeOnce.Do(func() {
		ch.mu.Lock()
		defer ch.mu.Unlock()
		for _, d := range ch.consumers {
			close(d)
		}
	})
	return nil
}

// Publish fails the way amqp091 does on a channel the broker has closed, and
// succeeds otherwise.
func (ch *fakeChannel) Publish(string, string, bool, bool, amqp.Publishing) error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}
	return nil
}

func (ch *fakeChannel) record(what string) error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.declared = append(ch.declared, what)
	return nil
}

func (ch *fakeChannel) ExchangeDeclare(name, _ string, _, _, _, _ bool, _ amqp.Table) error {
	return ch.record("exchange " + name)
}

func (ch *fakeChannel) QueueDeclare(name string, _, _, _, _ bool, _ amqp.Table) (amqp.Queue, error) {
	return amqp.Queue{Name: name}, ch.record("queue " + name)
}

func (ch *fakeChannel) QueueBind(name, key, exchange string, _ bool, _ amqp.Table) error {
	return ch.record("binding " + name + " " + exchange + " " + key)
}

func (ch *fakeChannel) Qos(prefetch, _ int, _ bool) error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.qos = append(ch.qos, prefetch)
	return nil
}

func (ch *fakeChannel) Consume(_, consumer string, _, _, _, _ bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	if ch.IsClosed() {
		return nil, amqp.ErrClosed
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if _, taken := ch.consumers[consumer]; taken {
		// The broker closes the channel for this (NOT_ALLOWED); failing is
		// enough to make a double registration visible.
		return nil, errors.New("attempt to reuse consumer tag " + consumer)
	}
	if ch.consumers == nil {
		ch.consumers = make(map[string]chan amqp.Delivery)
	}
	d := make(chan amqp.Delivery, 16)
	ch.consumers[consumer] = d
	return d, nil
}

func (ch *fakeChannel) Confirm(bool) error {
	ch.inConfirm.Store(true)
	return nil
}

func (ch *fakeChannel) NotifyReturn(c chan amqp.Return) chan amqp.Return {
	ch.returns = c
	return c
}

func (ch *fakeChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	ch.closes = c
	return c
}

func (ch *fakeChannel) PublishDeferred(_ context.Context, exchange, key string, _, _ bool, msg amqp.Publishing) (amqpconfirm.Confirmation, error) {
	if ch.IsClosed() {
		return nil, amqp.ErrClosed
	}
	if !ch.inConfirm.Load() {
		return nil, errors.New("not in confirm mode")
	}
	ch.confirms.Add(1)
	ack, returned := true, false
	if ch.confirm != nil {
		ack, returned = ch.confirm(msg)
	}
	if returned {
		ch.returns <- amqp.Return{ReplyCode: 312, ReplyText: "NO_ROUTE", Exchange: exchange, RoutingKey: key, MessageId: msg.MessageId}
	}
	return settled{ack}, nil
}

// settled is a confirmation the broker has already answered.
type settled struct{ ack bool }

func (s settled) Done() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func (s settled) Acked() bool { return s.ack }

// deliver hands a message to every consumer on the channel.
func (ch *fakeChannel) deliver(body string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	for _, d := range ch.consumers {
		d <- amqp.Delivery{Body: []byte(body)}
	}
}

func (ch *fakeChannel) consumerCount() int {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return len(ch.consumers)
}

// fakeDialer counts dials and, when gate is set, holds each dial until the
// gate is closed, so a test can look at the adapter while a dial is slow.
type fakeDialer struct {
	gate    chan struct{}
	entered chan struct{} // receives once per dial, when it starts
	err     error

	mu    sync.Mutex
	conns []*fakeConnection
}

func newFakeDialer() *fakeDialer {
	return &fakeDialer{entered: make(chan struct{}, 16)}
}

func (d *fakeDialer) dial(string) (amqpConnection, error) {
	d.entered <- struct{}{}
	if d.gate != nil {
		<-d.gate
	}
	if d.err != nil {
		return nil, d.err
	}
	conn := &fakeConnection{}
	d.mu.Lock()
	d.conns = append(d.conns, conn)
	d.mu.Unlock()
	return conn, nil
}

func (d *fakeDialer) dials() []*fakeConnection {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]*fakeConnection(nil), d.conns...)
}
