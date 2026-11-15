package amqpcluster

import (
	"errors"
	"testing"
	"time"
)

// nodeWithClosedConnection is a node whose broker connection has dropped, as
// the health check finds it after a broker restart or a network blip, and
// whose last check is old enough that the next one acts.
func nodeWithClosedConnection(name string) (*Node, *fakeConnection) {
	dropped := &fakeConnection{}
	dropped.closed.Store(true)
	n := node(name, NodeHealthy)
	n.conn = dropped
	n.publishCh = &fakeChannel{conn: dropped}
	n.subscribeCh = &fakeChannel{conn: dropped}
	n.lastCheck = time.Now().Add(-time.Minute)
	return n, dropped
}

// within runs f and fails the test if it has not returned after d. A
// deadlock shows up here as a timeout rather than a hung test binary.
func within(t *testing.T, d time.Duration, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %v", what, d)
	}
}

// The health check used to take the node lock and then call connectToNode,
// which takes it again: sync.RWMutex is not re-entrant, so the first closed
// connection hung the health goroutine with the lock held, and every publish
// after it.
func TestHealthCheckReconnectsAClosedNode(t *testing.T) {
	n, dropped := nodeWithClosedConnection("rabbit-1")
	dialer := newFakeDialer()
	ca := newTestAdapter(ClusterConfig{LoadBalanceStrategy: "round_robin"}, n)
	ca.dial = dialer.dial

	within(t, 2*time.Second, "checkNodeHealth on a closed connection", func() {
		ca.checkNodeHealth(n)
	})

	var healthy []*Node
	within(t, time.Second, "getHealthyNodes after the reconnect", func() {
		healthy = ca.getHealthyNodes()
	})
	if len(healthy) != 1 || healthy[0] != n {
		t.Fatalf("healthy nodes = %v, want the reconnected node", healthy)
	}

	conns := dialer.dials()
	if len(conns) != 1 {
		t.Fatalf("dialled %d times, want once", len(conns))
	}
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	if n.conn == amqpConnection(dropped) || n.conn != amqpConnection(conns[0]) {
		t.Error("the node did not take the new connection")
	}
	if n.publishCh == nil || n.publishCh.IsClosed() || n.subscribeCh == nil || n.subscribeCh.IsClosed() {
		t.Error("the node's channels were not replaced with open ones")
	}
}

// A dial can take as long as the TCP and AMQP handshakes. Node selection, which
// every publish goes through, must not wait for it.
func TestSlowDialDoesNotBlockNodeSelection(t *testing.T) {
	n, _ := nodeWithClosedConnection("rabbit-1")
	other := node("rabbit-2", NodeHealthy)
	dialer := newFakeDialer()
	dialer.gate = make(chan struct{})
	ca := newTestAdapter(ClusterConfig{LoadBalanceStrategy: "round_robin"}, n, other)
	ca.dial = dialer.dial

	checked := make(chan struct{})
	go func() {
		defer close(checked)
		ca.checkNodeHealth(n)
	}()
	<-dialer.entered

	within(t, time.Second, "selectNodeForPublish during a dial", func() {
		if ca.selectNodeForPublish("") == nil {
			t.Error("no node selected while another node was dialling")
		}
	})

	close(dialer.gate)
	within(t, 2*time.Second, "the health check after the dial", func() { <-checked })
}

// Two health checks of one node, or a health check and a caller, must not both
// dial it: the loser's connection would leak.
func TestConcurrentReconnectsDialOnce(t *testing.T) {
	n, _ := nodeWithClosedConnection("rabbit-1")
	dialer := newFakeDialer()
	dialer.gate = make(chan struct{})
	ca := newTestAdapter(ClusterConfig{}, n)
	ca.dial = dialer.dial

	checked := make(chan struct{})
	go func() {
		defer close(checked)
		ca.checkNodeHealth(n)
	}()
	<-dialer.entered

	if err := ca.connectToNode(n); !errors.Is(err, errConnectInProgress) {
		t.Errorf("a second connect during a dial returned %v, want errConnectInProgress", err)
	}

	close(dialer.gate)
	<-checked
	if got := len(dialer.dials()); got != 1 {
		t.Errorf("dialled %d times, want once", got)
	}
}

// A reconnect that finishes after Close must not install a connection that
// nothing will close.
func TestReconnectAfterCloseIsDiscarded(t *testing.T) {
	n, dropped := nodeWithClosedConnection("rabbit-1")
	dialer := newFakeDialer()
	ca := newTestAdapter(ClusterConfig{}, n)
	ca.dial = dialer.dial
	ca.closed.Store(true)

	if err := ca.connectToNode(n); err == nil {
		t.Fatal("connectToNode succeeded on a closed adapter")
	}

	conns := dialer.dials()
	if len(conns) != 1 || !conns[0].IsClosed() {
		t.Error("the connection dialled after Close was left open")
	}
	if n.conn != amqpConnection(dropped) {
		t.Error("a connection dialled after Close was installed on the node")
	}
}

func TestFailedReconnectMarksTheNodeFailed(t *testing.T) {
	n, _ := nodeWithClosedConnection("rabbit-1")
	dialer := newFakeDialer()
	dialer.err = errors.New("connection refused")
	ca := newTestAdapter(ClusterConfig{}, n)
	ca.dial = dialer.dial

	within(t, 2*time.Second, "checkNodeHealth with a failing dial", func() {
		ca.checkNodeHealth(n)
	})

	stats := ca.GetNodeStats()["rabbit-1"]
	if stats.State != NodeFailed {
		t.Errorf("state = %v, want NodeFailed", stats.State)
	}
	if stats.ErrorCount != 1 || stats.LastError == nil {
		t.Errorf("error count %d, last error %v; want the failed dial recorded", stats.ErrorCount, stats.LastError)
	}
	if healthy := ca.getHealthyNodes(); len(healthy) != 0 {
		t.Errorf("a node that failed to reconnect is still offered: %v", healthy)
	}
}
