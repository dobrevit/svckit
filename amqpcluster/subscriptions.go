package amqpcluster

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/dobrevit/svckit/logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

// subscription is a consumer the adapter keeps alive. It is started on every
// node connected when it is created, and again on any node that connects or
// reconnects later; every node's deliveries feed the one out channel.
type subscription struct {
	queue       string
	consumerTag string
	autoAck     bool
	exclusive   bool
	noLocal     bool
	noWait      bool
	args        amqp.Table
	prefetch    int // per-consumer prefetch; zero leaves the channel's (unlimited) default

	out chan amqp.Delivery
}

// declaration is an exchange, queue or binding the adapter declared, kept so
// it can be replayed on a node that (re)connects: a node that was down when
// it was first declared, or a broker that lost a non-durable declaration,
// would otherwise have nothing for consumers to consume from.
type declaration struct {
	key         string // identifies the declaration; a repeat replaces it
	onPublishCh bool   // exchanges are declared on the publish channel, queues and bindings on the subscribe channel
	apply       func(ch amqpChannel) error
}

// registry holds what the adapter restores on a node after it connects.
type registry struct {
	mu            sync.Mutex
	subscriptions []*subscription
	declarations  []declaration
}

func (r *registry) addSubscription(s *subscription) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subscriptions = append(r.subscriptions, s)
}

func (r *registry) removeSubscription(s *subscription) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, existing := range r.subscriptions {
		if existing == s {
			r.subscriptions = append(r.subscriptions[:i], r.subscriptions[i+1:]...)
			return
		}
	}
}

func (r *registry) remember(d declaration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, existing := range r.declarations {
		if existing.key == d.key {
			r.declarations[i] = d
			return
		}
	}
	r.declarations = append(r.declarations, d)
}

func (r *registry) snapshot() ([]declaration, []*subscription) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]declaration(nil), r.declarations...), append([]*subscription(nil), r.subscriptions...)
}

// subscribe registers s and starts it on every healthy node. It is
// registered first, so that a node reconnecting meanwhile starts it too;
// consumeOnNode keeps the two from consuming twice on one channel.
func (ca *ClusterAdapter) subscribe(s *subscription) (<-chan amqp.Delivery, error) {
	healthy := ca.getHealthyNodes()
	if len(healthy) == 0 {
		return nil, errors.New("no healthy nodes available for subscription")
	}

	s.out = make(chan amqp.Delivery, 100) // Buffer for better performance
	ca.registry.addSubscription(s)

	var started int
	var lastErr error
	for _, node := range healthy {
		if err := ca.consumeOnNode(node, s); err != nil {
			logging.Error("Failed to consume from node %s: %v", node.Name, err)
			lastErr = err
			continue
		}
		started++
	}

	if started == 0 {
		ca.registry.removeSubscription(s)
		return nil, fmt.Errorf("failed to subscribe to any healthy node: %w", lastErr)
	}

	logging.Info("Subscribed to queue %s on %d/%d nodes", s.queue, started, len(healthy))
	return s.out, nil
}

// consumeOnNode starts s on the node's current subscribe channel, unless it
// is already running there, and forwards the node's deliveries to s.out.
func (ca *ClusterAdapter) consumeOnNode(node *Node, s *subscription) error {
	node.consumeMu.Lock()
	defer node.consumeMu.Unlock()

	node.mutex.RLock()
	ch := node.subscribeCh
	node.mutex.RUnlock()

	if ch == nil || ch.IsClosed() {
		return errors.New("subscribe channel is not open")
	}
	if node.consumers[s] == ch {
		return nil
	}

	// Without a prefetch limit the broker pushes every ready message to the
	// consumer at once. Qos with global=false applies to the consumers
	// started after it on this channel, so it is set before each Consume.
	if s.prefetch > 0 {
		if err := ch.Qos(s.prefetch, 0, false); err != nil {
			return fmt.Errorf("set prefetch %d: %w", s.prefetch, err)
		}
	}

	msgs, err := ch.Consume(s.queue, fmt.Sprintf("%s-%s", s.consumerTag, node.Name),
		s.autoAck, s.exclusive, s.noLocal, s.noWait, s.args)
	if err != nil {
		return err
	}

	// Close waits for the forwarders and then closes s.out; a forwarder added
	// after that wait began would send on a closed channel.
	ca.forwardersMu.Lock()
	defer ca.forwardersMu.Unlock()
	if ca.closed.Load() {
		return errors.New("the adapter is closed")
	}
	if node.consumers == nil {
		node.consumers = make(map[*subscription]amqpChannel)
	}
	node.consumers[s] = ch
	ca.forwarders.Add(1)
	go ca.forward(node, s, msgs)
	return nil
}

// forward moves one node's deliveries for s to s.out. It ends when the
// node's channel closes (the deliveries channel is closed with it) or the
// adapter is closed.
func (ca *ClusterAdapter) forward(node *Node, s *subscription, msgs <-chan amqp.Delivery) {
	defer ca.forwarders.Done()
	for msg := range msgs {
		atomic.AddInt64(&node.subscribeCount, 1)
		select {
		case s.out <- msg:
		case <-ca.stopping():
			return
		}
	}
}

// restoreNode replays the remembered declarations on a node that has just
// connected, or whose subscribe channel was recreated, and starts every
// subscription on it.
func (ca *ClusterAdapter) restoreNode(node *Node) {
	declarations, subscriptions := ca.registry.snapshot()
	if len(declarations) == 0 && len(subscriptions) == 0 {
		return
	}

	node.mutex.RLock()
	publishCh, subscribeCh := node.publishCh, node.subscribeCh
	node.mutex.RUnlock()

	for _, d := range declarations {
		ch := subscribeCh
		if d.onPublishCh {
			ch = publishCh
		}
		if ch == nil || ch.IsClosed() {
			continue
		}
		if err := d.apply(ch); err != nil {
			logging.Error("Failed to restore %s on node %s: %v", d.key, node.Name, err)
		}
	}

	for _, s := range subscriptions {
		if err := ca.consumeOnNode(node, s); err != nil {
			logging.Error("Failed to resume consuming from %s on node %s: %v", s.queue, node.Name, err)
		}
	}
}

// stopping is closed when the adapter closes.
func (ca *ClusterAdapter) stopping() <-chan struct{} {
	ca.stopOnce.Do(func() { ca.stop = make(chan struct{}) })
	return ca.stop
}
