package amqpcluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	amqp "github.com/rabbitmq/amqp091-go"
)

// confirmingCluster is a cluster of connected fake nodes with
// PublisherConfirms on, and round-robin selection starting at the first.
func confirmingCluster(t *testing.T, names ...string) (*ClusterAdapter, []*Node) {
	t.Helper()
	nodes := make([]*Node, len(names))
	for i, name := range names {
		nodes[i] = node(name, NodeFailed)
	}
	ca := newTestAdapter(ClusterConfig{LoadBalanceStrategy: "round_robin", PublisherConfirms: true, ConfirmTimeout: time.Second}, nodes...)
	ca.dial = newFakeDialer().dial
	for _, n := range nodes {
		if err := ca.connectToNode(n); err != nil {
			t.Fatalf("connect %s: %v", n.Name, err)
		}
	}
	ca.nodeIndex = -1 // the first selection is nodes[0]
	return ca, nodes
}

func publishErr(ca *ClusterAdapter) error {
	return ca.PublishWithContext(context.Background(), "events", "order.created", false, false, amqp.Publishing{MessageId: "m-1"})
}

func TestEveryPublishChannelIsInConfirmMode(t *testing.T) {
	ca, nodes := confirmingCluster(t, "rabbit-1")
	n := nodes[0]
	if !publishChannel(n).inConfirm.Load() || n.confirmer == nil {
		t.Fatal("a connected node's publish channel is not in confirm mode")
	}

	// A channel the health check recreates.
	_ = publishChannel(n).Close()
	n.mutex.Lock()
	n.lastCheck = time.Now().Add(-time.Minute)
	n.mutex.Unlock()
	ca.checkNodeHealth(n)
	if ch := publishChannel(n); ch.IsClosed() || !ch.inConfirm.Load() {
		t.Error("the recreated publish channel is not in confirm mode")
	}

	if err := publishErr(ca); err != nil {
		t.Errorf("publish on the recreated channel: %v", err)
	}
}

// A nack or a return is a broker's answer about this message. Sending the
// message to another node would publish it twice, or past a broker that
// refused it.
func TestARefusedPublishIsNotFailedOver(t *testing.T) {
	for name, tc := range map[string]struct {
		confirm func(amqp.Publishing) (bool, bool)
		want    error
	}{
		"nacked":   {func(amqp.Publishing) (bool, bool) { return false, false }, eventbus.ErrNacked},
		"returned": {func(amqp.Publishing) (bool, bool) { return true, true }, eventbus.ErrUnroutable},
	} {
		t.Run(name, func(t *testing.T) {
			ca, nodes := confirmingCluster(t, "rabbit-1", "rabbit-2")
			publishChannel(nodes[0]).confirm = tc.confirm

			if err := publishErr(ca); !errors.Is(err, tc.want) {
				t.Fatalf("Publish = %v, want %v", err, tc.want)
			}
			if n := publishChannel(nodes[1]).confirms.Load(); n != 0 {
				t.Errorf("the message was also published on rabbit-2 (%d times)", n)
			}
			if state := ca.GetNodeStats()["rabbit-1"].State; state != NodeHealthy {
				t.Errorf("rabbit-1 is %v after answering; a broker that says no is not unhealthy", state)
			}
		})
	}
}

// A publish that never reached a broker is safe to send elsewhere.
func TestAPublishThatWasNotSentFailsOver(t *testing.T) {
	ca, nodes := confirmingCluster(t, "rabbit-1", "rabbit-2")
	_ = publishChannel(nodes[0]).Close()

	if err := publishErr(ca); err != nil {
		t.Fatalf("Publish = %v, want it delivered through rabbit-2", err)
	}
	if n := publishChannel(nodes[1]).confirms.Load(); n != 1 {
		t.Errorf("rabbit-2 received %d publishes, want 1", n)
	}
	if state := ca.GetNodeStats()["rabbit-1"].State; state != NodeDegraded {
		t.Errorf("rabbit-1 is %v after failing to send, want NodeDegraded", state)
	}
}

func TestACancelledContextIsNotFailedOver(t *testing.T) {
	ca, nodes := confirmingCluster(t, "rabbit-1", "rabbit-2")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ca.PublishWithContext(ctx, "events", "order.created", false, false, amqp.Publishing{MessageId: "m-1"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish = %v, want context.Canceled", err)
	}
	for _, n := range nodes {
		if c := publishChannel(n).confirms.Load(); c != 0 {
			t.Errorf("%s received a publish after the context was cancelled", n.Name)
		}
	}
}
