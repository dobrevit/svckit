package amqpcluster

import (
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// fakeClock is a clock the test moves by hand, so health intervals pass
// without waiting for them.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

const testInterval = 30 * time.Second

// connectedAdapter is a single-node adapter whose node is connected and
// healthy, on a fake clock, with the default recovery threshold.
func connectedAdapter() (*ClusterAdapter, *Node, *fakeClock) {
	clock := &fakeClock{now: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)}
	conn := &fakeConnection{}
	n := node("rabbit-1", NodeHealthy)
	n.conn = conn
	n.publishCh = &fakeChannel{conn: conn}
	n.subscribeCh = &fakeChannel{conn: conn}
	n.lastCheck = clock.Now()
	n.windowStart = clock.Now()

	ca := newTestAdapter(ClusterConfig{HealthCheckInterval: testInterval, LoadBalanceStrategy: "round_robin"}, n)
	ca.now = clock.Now
	ca.dial = newFakeDialer().dial
	return ca, n, clock
}

// failOnePublish makes the node's channel fail one publish, as a channel the
// broker closed does, and then gives the node a working channel back, as the
// health check's channel recreation would.
func failOnePublish(t *testing.T, ca *ClusterAdapter, n *Node) {
	t.Helper()
	n.mutex.Lock()
	broken := &fakeChannel{conn: n.conn.(*fakeConnection)}
	broken.closed.Store(true)
	n.publishCh = broken
	n.mutex.Unlock()

	if err := ca.publishToNode(n, "events", "order.created", false, false, amqp.Publishing{}); err == nil {
		t.Fatal("a publish on a closed channel succeeded")
	}
}

func state(ca *ClusterAdapter, n *Node) NodeState {
	return ca.GetNodeStats()[n.Name].State
}

// A degraded node used to stay degraded until restart: the recovery check
// divided the error count by the time since lastCheck right after setting
// lastCheck to now, so the rate was infinite.
func TestDegradedNodeRecoversAfterAQuietInterval(t *testing.T) {
	ca, n, clock := connectedAdapter()

	clock.Advance(5 * time.Second)
	failOnePublish(t, ca, n)
	if got := state(ca, n); got != NodeDegraded {
		t.Fatalf("state after a failed publish = %v, want NodeDegraded", got)
	}

	// The window with the error in it: two errors a minute, above the threshold.
	clock.Advance(testInterval)
	ca.checkNodeHealth(n)
	if got := state(ca, n); got != NodeDegraded {
		t.Fatalf("state at the check that closes the erroring window = %v, want NodeDegraded", got)
	}

	// A window with no errors.
	clock.Advance(testInterval)
	ca.checkNodeHealth(n)
	if got := state(ca, n); got != NodeHealthy {
		t.Fatalf("state after one quiet interval = %v, want NodeHealthy", got)
	}
}

func TestNodeErroringEveryIntervalStaysDegraded(t *testing.T) {
	ca, n, clock := connectedAdapter()

	for i := range 5 {
		failOnePublish(t, ca, n)
		clock.Advance(testInterval)
		ca.checkNodeHealth(n)
		if got := state(ca, n); got != NodeDegraded {
			t.Fatalf("interval %d: state = %v, want NodeDegraded while the node keeps failing", i, got)
		}
	}
}

// The threshold is a rate: a higher one tolerates the error in the window.
func TestDegradedErrorRateIsConfigurable(t *testing.T) {
	ca, n, clock := connectedAdapter()
	ca.config.DegradedErrorRate = 5

	failOnePublish(t, ca, n)
	clock.Advance(testInterval)
	ca.checkNodeHealth(n)
	if got := state(ca, n); got != NodeHealthy {
		t.Errorf("state = %v, want NodeHealthy at two errors a minute against a threshold of five", got)
	}
}

// ErrorCount in GetNodeStats is the total since the node was created; the
// per-window count that decides recovery must not reset it.
func TestErrorCountStaysATotal(t *testing.T) {
	ca, n, clock := connectedAdapter()

	for range 3 {
		failOnePublish(t, ca, n)
		clock.Advance(testInterval)
		ca.checkNodeHealth(n)
	}

	if got := ca.GetNodeStats()[n.Name].ErrorCount; got != 3 {
		t.Errorf("ErrorCount = %d, want 3 across three windows", got)
	}
}

// A channel the broker closes leaves the connection open. Only a nil channel
// used to be recreated, so a closed one kept failing every publish.
func TestHealthCheckRecreatesAClosedChannel(t *testing.T) {
	ca, n, clock := connectedAdapter()
	failOnePublish(t, ca, n)

	clock.Advance(testInterval)
	ca.checkNodeHealth(n)

	n.mutex.RLock()
	ch := n.publishCh
	n.mutex.RUnlock()
	if ch == nil || ch.IsClosed() {
		t.Fatal("the closed publish channel was not recreated")
	}
}

// With one node, a single failed publish used to end publishing until the
// process restarted ("failed to publish message to any healthy node").
func TestSingleNodeAdapterPublishesAgainAfterATransientError(t *testing.T) {
	ca, n, clock := connectedAdapter()
	failOnePublish(t, ca, n)

	if err := ca.Publish("events", "order.created", false, false, amqp.Publishing{}); err == nil {
		t.Fatal("published with the only node degraded")
	}

	for range 2 {
		clock.Advance(testInterval)
		ca.checkNodeHealth(n)
	}

	if err := ca.Publish("events", "order.created", false, false, amqp.Publishing{}); err != nil {
		t.Fatalf("publish after the node recovered: %v", err)
	}
}

// A check within half an interval of the previous one is skipped; one a full
// interval later is not, whatever the interval. The skip used to be a fixed
// ten seconds, so an interval below that checked at ten-second steps.
func TestHealthCheckSkipsOnlyWithinHalfAnInterval(t *testing.T) {
	ca, n, clock := connectedAdapter()
	ca.config.HealthCheckInterval = 2 * time.Second
	failOnePublish(t, ca, n)

	clock.Advance(500 * time.Millisecond)
	ca.checkNodeHealth(n)
	n.mutex.RLock()
	skipped := n.publishCh.IsClosed()
	n.mutex.RUnlock()
	if !skipped {
		t.Error("a check a quarter interval after the last one was not skipped")
	}

	clock.Advance(2 * time.Second)
	ca.checkNodeHealth(n)
	n.mutex.RLock()
	recreated := !n.publishCh.IsClosed()
	n.mutex.RUnlock()
	if !recreated {
		t.Error("a check a full two-second interval later was skipped")
	}
}
