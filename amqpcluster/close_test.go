package amqpcluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	"github.com/dobrevit/svckit/internal/inflight"
)

// subscribedTo builds a subscriber over one fake node, subscribes handler to
// "orders" with opts, and hands it one message. It returns the subscriber,
// the message's acknowledger and the node.
func subscribedTo(t *testing.T, handler eventbus.EventHandler, opts SubscriptionOptions, closeTimeout time.Duration) (*ClusterSubscriber, *acknowledger, *Node) {
	t.Helper()
	ca, nodes, _ := clusterOf(t, 1, "rabbit-1")
	cs := subscriberOn(ca, "billing", nil, false)
	WithCloseTimeout(closeTimeout)(cs)

	opts.AllowUnsigned = true
	if err := cs.SubscribeWithOptions("orders", "order.*", handler, opts); err != nil {
		t.Fatalf("SubscribeWithOptions: %v", err)
	}
	d := unsignedDelivery(t, nil)
	subscribeChannel(nodes[0]).deliverTo(d)
	return cs, d.Acknowledger.(*acknowledger), nodes[0]
}

// A handler blocked on its context used to run on context.Background(), so
// Close could neither stop it nor bound how long it waited for it.
func TestCloseCancelsAHandlerAfterTheTimeoutAndRequeuesItsMessage(t *testing.T) {
	started := make(chan struct{})
	handler := handlerFunc(func(ctx context.Context, _ *eventbus.BaseEvent) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	cs, ack, _ := subscribedTo(t, handler, SubscriptionOptions{}, 100*time.Millisecond)
	<-started

	start := time.Now()
	within(t, 2*time.Second, "Close", func() { _ = cs.Close() })
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Errorf("Close returned after %v, before the close timeout", elapsed)
	}
	if !ack.nacked || !ack.requeue {
		t.Errorf("nacked %v, requeue %v; a cancelled handler's message must go back to the queue", ack.nacked, ack.requeue)
	}
}

// A handler that finishes within the close timeout completes normally.
func TestCloseWaitsForAHandlerThatFinishes(t *testing.T) {
	started := make(chan struct{})
	handler := handlerFunc(func(ctx context.Context, _ *eventbus.BaseEvent) error {
		close(started)
		time.Sleep(100 * time.Millisecond)
		return ctx.Err()
	})
	cs, ack, _ := subscribedTo(t, handler, SubscriptionOptions{}, 5*time.Second)
	<-started

	within(t, 2*time.Second, "Close", func() { _ = cs.Close() })
	if !ack.acked {
		t.Error("a handler that finished within the close timeout was not acked")
	}
}

func TestCloseIsBoundedByAHandlerThatIgnoresItsContext(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	handler := handlerFunc(func(context.Context, *eventbus.BaseEvent) error {
		close(started)
		<-release
		return nil
	})
	cs, _, _ := subscribedTo(t, handler, SubscriptionOptions{}, 100*time.Millisecond)
	<-started

	start := time.Now()
	within(t, 100*time.Millisecond+2*inflight.Grace+time.Second, "Close", func() { _ = cs.Close() })
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond+2*inflight.Grace+500*time.Millisecond {
		t.Errorf("Close took %v with a handler that ignores its context", elapsed)
	}
}

func TestHandlerTimeoutCancelsASlowHandler(t *testing.T) {
	got := make(chan error, 1)
	handler := handlerFunc(func(ctx context.Context, _ *eventbus.BaseEvent) error {
		<-ctx.Done()
		got <- ctx.Err()
		return eventbus.Permanent(ctx.Err())
	})
	cs, _, _ := subscribedTo(t, handler, SubscriptionOptions{HandlerTimeout: 50 * time.Millisecond}, time.Second)
	defer cs.Close()

	select {
	case err := <-got:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("the handler saw %v, want its deadline", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the handler was not cancelled by its timeout")
	}
}

func TestCloseCancelsTheConsumers(t *testing.T) {
	cs, _, node := subscribedTo(t, handlerFunc(func(context.Context, *eventbus.BaseEvent) error { return nil }), SubscriptionOptions{}, time.Second)
	ch := subscribeChannel(node)

	_ = cs.Close()

	ch.mu.Lock()
	defer ch.mu.Unlock()
	if len(ch.cancelled) != 1 || ch.cancelled[0] != "billing-orders-rabbit-1" {
		t.Errorf("cancelled consumers = %v, want the subscription's", ch.cancelled)
	}
}
