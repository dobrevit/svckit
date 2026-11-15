//go:build integration

package amqpcluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	amqp "github.com/rabbitmq/amqp091-go"
)

func confirmingPublisher(t *testing.T) *ClusterPublisher {
	t.Helper()
	pub, err := NewClusterPublisherWithConfig(ClusterConfig{
		Nodes:             []string{testRabbitURL()},
		ServiceName:       "amqpcluster-it",
		PublisherConfirms: true,
		ConfirmTimeout:    30 * time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("NewClusterPublisherWithConfig: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	return pub
}

// queueFor declares a queue bound to the events exchange for one routing
// key, with the given arguments, deleted when the test ends.
func queueFor(t *testing.T, key string, args amqp.Table) string {
	t.Helper()
	queue := fmt.Sprintf("svckit.it.cluster.%d", time.Now().UnixNano())
	withBroker(t, func(ch *amqp.Channel) {
		if _, err := ch.QueueDeclare(queue, false, false, false, false, args); err != nil {
			t.Fatalf("declare %s: %v", queue, err)
		}
		if err := ch.QueueBind(queue, key, eventbus.DefaultExchange, false, nil); err != nil {
			t.Fatalf("bind %s: %v", queue, err)
		}
	})
	t.Cleanup(func() {
		withBroker(t, func(ch *amqp.Channel) { _, _ = ch.QueueDelete(queue, false, false, false) })
	})
	return queue
}

func TestClusterConfirmedPublishOutcomes(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	pub := confirmingPublisher(t)
	ctx := context.Background()

	t.Run("a bound queue confirms", func(t *testing.T) {
		key := fmt.Sprintf("it.cluster.bound.%d", time.Now().UnixNano())
		queueFor(t, key, nil)
		if err := pub.Publish(ctx, eventbus.NewEvent(key, "amqpcluster-it", nil)); err != nil {
			t.Fatalf("Publish = %v, want nil", err)
		}
	})

	t.Run("no binding is unroutable", func(t *testing.T) {
		key := fmt.Sprintf("it.cluster.nobody.%d", time.Now().UnixNano())
		if err := pub.Publish(ctx, eventbus.NewEvent(key, "amqpcluster-it", nil)); !errors.Is(err, eventbus.ErrUnroutable) {
			t.Fatalf("Publish = %v, want ErrUnroutable", err)
		}
	})

	t.Run("a queue that rejects publishes nacks", func(t *testing.T) {
		key := fmt.Sprintf("it.cluster.reject.%d", time.Now().UnixNano())
		queueFor(t, key, amqp.Table{"x-max-length": int32(0), "x-overflow": "reject-publish"})
		if err := pub.Publish(ctx, eventbus.NewEvent(key, "amqpcluster-it", nil)); !errors.Is(err, eventbus.ErrNacked) {
			t.Fatalf("Publish = %v, want ErrNacked", err)
		}
	})

	if state := pub.cluster.GetNodeStats()["node-0"].State; state != NodeHealthy {
		t.Errorf("the node is %v after a return and a nack, want healthy", state)
	}
}

func TestClusterConcurrentConfirmedPublishes(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	pub := confirmingPublisher(t)
	routable := fmt.Sprintf("it.cluster.many.%d", time.Now().UnixNano())
	queue := queueFor(t, routable, nil)
	unroutable := routable + ".nobody"

	const n = 1000
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := routable
			if i%2 == 1 {
				key = unroutable
			}
			errs[i] = pub.Publish(context.Background(), eventbus.NewEvent(key, "amqpcluster-it", nil))
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if i%2 == 0 && err != nil {
			t.Fatalf("routable publish %d: %v", i, err)
		}
		if i%2 == 1 && !errors.Is(err, eventbus.ErrUnroutable) {
			t.Fatalf("unroutable publish %d: %v, want ErrUnroutable", i, err)
		}
	}
	withBroker(t, func(ch *amqp.Channel) {
		state, err := ch.QueueDeclarePassive(queue, false, false, false, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		if state.Messages != n/2 {
			t.Errorf("%d messages in the bound queue, want %d", state.Messages, n/2)
		}
	})
}
