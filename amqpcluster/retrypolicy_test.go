package amqpcluster

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
)

var testPolicy = &eventbus.RetryPolicy{Delays: []time.Duration{time.Second, 2 * time.Second}, MaxAttempts: 3}

// The original may be acknowledged only after its copy is safely in the
// retry queue: acking first, and crashing before the republish, would lose
// the message.
func TestTheRetryIsPublishedBeforeTheOriginalIsAcked(t *testing.T) {
	ca, nodes, _ := clusterOf(t, 1, "rabbit-1")
	cs := &ClusterSubscriber{cluster: ca, serviceName: "billing"}

	var mu sync.Mutex
	var order []string
	note := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, s)
	}
	publishChannel(nodes[0]).onPublish = func(key string) { note("publish " + key) }

	d := unsignedDelivery(t, nil)
	d.Acknowledger.(*acknowledger).onSettle = note
	cs.handleMessage("orders", d, failingHandler{eventbus.Retryable(errors.New("stock service down"))},
		SubscriptionOptions{AllowUnsigned: true, Retry: testPolicy})

	if want := []string{"publish orders.retry.1s", "ack"}; !slices.Equal(order, want) {
		t.Errorf("order = %q, want %q", order, want)
	}
}

// If the copy cannot be published the original goes back on its queue: it is
// handled again, not lost and not acknowledged.
func TestAFailedRepublishRequeuesTheOriginal(t *testing.T) {
	ca, nodes, _ := clusterOf(t, 1, "rabbit-1")
	cs := &ClusterSubscriber{cluster: ca, serviceName: "billing"}
	_ = publishChannel(nodes[0]).Close()

	d := unsignedDelivery(t, nil)
	cs.handleMessage("orders", d, failingHandler{eventbus.Retryable(errors.New("down"))},
		SubscriptionOptions{AllowUnsigned: true, Retry: testPolicy})

	a := d.Acknowledger.(*acknowledger)
	if a.acked || !a.nacked || !a.requeue {
		t.Errorf("acked %v, nacked %v, requeue %v; want requeued", a.acked, a.nacked, a.requeue)
	}
}

func TestHandlersSeeTheirAttempt(t *testing.T) {
	ca, _, _ := clusterOf(t, 1, "rabbit-1")
	cs := &ClusterSubscriber{cluster: ca, serviceName: "billing"}

	var seen int
	handler := handlerFunc(func(ctx context.Context, _ *eventbus.BaseEvent) error {
		seen = eventbus.Attempt(ctx)
		return nil
	})

	d := unsignedDelivery(t, nil)
	d.Headers = map[string]any{"x-svckit-attempt": int32(2)}
	cs.handleMessage("orders", d, handler, SubscriptionOptions{AllowUnsigned: true, Retry: testPolicy})
	if seen != 2 {
		t.Errorf("Attempt(ctx) = %d, want 2", seen)
	}

	cs.handleMessage("orders", unsignedDelivery(t, nil), handler, SubscriptionOptions{AllowUnsigned: true})
	if seen != 1 {
		t.Errorf("Attempt(ctx) on a first delivery = %d, want 1", seen)
	}
}

func TestAnUnusablePolicyIsRefused(t *testing.T) {
	cs := &ClusterSubscriber{}
	err := cs.SubscribeWithOptions("orders", "order.*", failingHandler{}, SubscriptionOptions{Retry: &eventbus.RetryPolicy{MaxAttempts: 0}})
	if err == nil {
		t.Error("a policy with no attempts was accepted")
	}
}

func TestRetryTopologyKeepsDeadLettersOffTheTopicExchange(t *testing.T) {
	ca, nodes, _ := clusterOf(t, 1, "rabbit-1")
	cs := &ClusterSubscriber{cluster: ca, exchange: "events"}
	if err := cs.setupQueue("orders", "order.*", SubscriptionOptions{Durable: true, Retry: testPolicy}); err != nil {
		t.Fatal(err)
	}

	ch := subscribeChannel(nodes[0])
	for _, d := range ch.declared {
		if d == "binding orders.dlq events orders.dlq" {
			t.Error("the DLQ is bound to the topic exchange")
		}
	}
	main := ch.queueArgs["orders"]
	if main["x-dead-letter-exchange"] != "" || main["x-dead-letter-routing-key"] != "orders.dlq" {
		t.Errorf("main queue dead-letter arguments = %v", main)
	}
	for _, q := range []string{"orders.retry.1s", "orders.retry.2s"} {
		if _, ok := ch.queueArgs[q]; !ok {
			t.Errorf("retry queue %s was not declared", q)
		}
	}
}

type handlerFunc func(context.Context, *eventbus.BaseEvent) error

func (f handlerFunc) Handle(ctx context.Context, e *eventbus.BaseEvent) error { return f(ctx, e) }
