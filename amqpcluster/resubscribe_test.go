package amqpcluster

import (
	"slices"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// clusterOf builds an adapter over fake nodes; the first connected of them
// are connected through the fake dialer, the rest start failed with no
// connection, as a node that was down at startup does.
func clusterOf(t *testing.T, connected int, names ...string) (*ClusterAdapter, []*Node, *fakeDialer) {
	t.Helper()
	dialer := newFakeDialer()
	nodes := make([]*Node, len(names))
	for i, name := range names {
		nodes[i] = node(name, NodeFailed)
	}
	ca := newTestAdapter(ClusterConfig{LoadBalanceStrategy: "round_robin"}, nodes...)
	ca.dial = dialer.dial
	for _, n := range nodes[:connected] {
		if err := ca.connectToNode(n); err != nil {
			t.Fatalf("connect %s: %v", n.Name, err)
		}
	}
	return ca, nodes, dialer
}

func subscribeChannel(n *Node) *fakeChannel {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.subscribeCh.(*fakeChannel)
}

func publishChannel(n *Node) *fakeChannel {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.publishCh.(*fakeChannel)
}

func receive(t *testing.T, out <-chan amqp.Delivery, want string) {
	t.Helper()
	select {
	case msg, ok := <-out:
		if !ok {
			t.Fatalf("the subscription closed while waiting for %q", want)
		}
		if string(msg.Body) != want {
			t.Fatalf("received %q, want %q", msg.Body, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("%q did not arrive", want)
	}
}

// A node's consumers die with its connection. Nothing used to start them
// again after the reconnect, so a subscriber silently stopped receiving from
// that node.
func TestSubscriptionResumesOnAReconnectedNode(t *testing.T) {
	ca, nodes, _ := clusterOf(t, 1, "rabbit-1")
	n := nodes[0]

	out, err := ca.Subscribe("orders", "billing", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	subscribeChannel(n).deliver("before")
	receive(t, out, "before")

	n.mutex.RLock()
	_ = n.conn.Close() // the broker drops the connection
	n.mutex.RUnlock()
	if err := ca.connectToNode(n); err != nil {
		t.Fatalf("reconnect: %v", err)
	}

	resumed := subscribeChannel(n)
	if resumed.consumerCount() != 1 {
		t.Fatalf("%d consumers on the new channel, want the subscription resumed once", resumed.consumerCount())
	}
	resumed.deliver("after")
	receive(t, out, "after")
}

// A node down when Subscribe was called was never consumed from.
func TestSubscriptionStartsOnANodeThatConnectsLater(t *testing.T) {
	ca, nodes, _ := clusterOf(t, 1, "rabbit-1", "rabbit-2")

	out, err := ca.Subscribe("orders", "billing", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := ca.connectToNode(nodes[1]); err != nil {
		t.Fatalf("connect rabbit-2: %v", err)
	}
	subscribeChannel(nodes[1]).deliver("from rabbit-2")
	receive(t, out, "from rabbit-2")
}

func TestDeclarationsAreReplayedOnReconnect(t *testing.T) {
	ca, nodes, _ := clusterOf(t, 1, "rabbit-1", "rabbit-2")

	if err := ca.DeclareExchange("events", "topic", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := ca.DeclareQueue("orders", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := ca.BindQueue("orders", "order.*", "events", false, nil); err != nil {
		t.Fatal(err)
	}
	// Declaring again replaces, rather than repeats, the remembered one.
	if err := ca.DeclareQueue("orders", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}

	if err := ca.connectToNode(nodes[1]); err != nil {
		t.Fatalf("connect rabbit-2: %v", err)
	}

	if got := publishChannel(nodes[1]).declared; !slices.Equal(got, []string{"exchange events"}) {
		t.Errorf("publish channel declarations = %q, want the exchange", got)
	}
	want := []string{"queue orders", "binding orders events order.*"}
	if got := subscribeChannel(nodes[1]).declared; !slices.Equal(got, want) {
		t.Errorf("subscribe channel declarations = %q, want %q", got, want)
	}
}

// SubscriptionOptions.PrefetchCount was accepted and never applied, so a
// consumer got every ready message at once.
func TestPrefetchIsSetBeforeEveryConsume(t *testing.T) {
	ca, nodes, _ := clusterOf(t, 1, "rabbit-1")
	n := nodes[0]

	if _, err := ca.subscribe(&subscription{queue: "orders", consumerTag: "billing", prefetch: 1}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if got := subscribeChannel(n).qos; !slices.Equal(got, []int{1}) {
		t.Errorf("Qos calls = %v, want [1] before consuming", got)
	}

	n.mutex.RLock()
	_ = n.conn.Close()
	n.mutex.RUnlock()
	if err := ca.connectToNode(n); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if got := subscribeChannel(n).qos; !slices.Equal(got, []int{1}) {
		t.Errorf("Qos calls after the reconnect = %v, want [1] again", got)
	}
}

// Subscribe's own signature has no prefetch, and it sets none, as before.
func TestSubscribeSetsNoPrefetch(t *testing.T) {
	ca, nodes, _ := clusterOf(t, 1, "rabbit-1")
	if _, err := ca.Subscribe("orders", "billing", false, false, false, false, nil); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if got := subscribeChannel(nodes[0]).qos; len(got) != 0 {
		t.Errorf("Qos calls = %v, want none", got)
	}
}

// A restore racing a Subscribe, or two restores, must not register the
// subscription twice on one channel: the broker closes the channel for a
// reused consumer tag.
func TestASubscriptionConsumesOncePerChannel(t *testing.T) {
	ca, nodes, _ := clusterOf(t, 1, "rabbit-1")
	if _, err := ca.Subscribe("orders", "billing", false, false, false, false, nil); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ca.restoreNode(nodes[0])
	ca.restoreNode(nodes[0])

	if got := subscribeChannel(nodes[0]).consumerCount(); got != 1 {
		t.Errorf("%d consumers on the channel, want 1", got)
	}
}

// A channel-level error (a redeclare with other arguments, an ack of an
// unknown tag) closes the subscribe channel and its consumers but leaves the
// connection open.
func TestHealthCheckRestoresConsumersOnARecreatedSubscribeChannel(t *testing.T) {
	ca, nodes, _ := clusterOf(t, 1, "rabbit-1")
	n := nodes[0]
	out, err := ca.Subscribe("orders", "billing", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	_ = subscribeChannel(n).Close()
	n.mutex.Lock()
	n.lastCheck = time.Now().Add(-time.Minute)
	n.mutex.Unlock()
	ca.checkNodeHealth(n)

	recreated := subscribeChannel(n)
	if recreated.IsClosed() || recreated.consumerCount() != 1 {
		t.Fatal("the subscribe channel was not recreated with its consumer")
	}
	recreated.deliver("after the channel error")
	receive(t, out, "after the channel error")
}

func TestCloseEndsTheSubscription(t *testing.T) {
	ca, _, _ := clusterOf(t, 2, "rabbit-1", "rabbit-2")
	out, err := ca.Subscribe("orders", "billing", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	within(t, 2*time.Second, "Close", func() { _ = ca.Close() })

	select {
	case _, ok := <-out:
		if ok {
			t.Error("received a message after Close")
		}
	case <-time.After(time.Second):
		t.Error("the subscription channel was not closed by Close")
	}
}
