package eventbus

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestWithTransientSetsTheDeliveryMode(t *testing.T) {
	p := &Publisher{deliveryMode: amqp.Persistent}
	WithTransient().applyPublisher(p)
	if p.deliveryMode != amqp.Transient {
		t.Errorf("delivery mode = %d, want transient (%d)", p.deliveryMode, amqp.Transient)
	}
}

// The default must stay 300000: the broker refuses to redeclare an existing
// queue with a different TTL.
func TestQueueArguments(t *testing.T) {
	if got := queueArguments("orders.dlq", 0)["x-message-ttl"]; got != 300000 {
		t.Errorf("default x-message-ttl = %#v, want the int 300000 every existing queue was declared with", got)
	}
	if got := queueArguments("orders.dlq", 90*time.Second)["x-message-ttl"]; got != int64(90000) {
		t.Errorf("x-message-ttl for 90s = %#v, want int64(90000)", got)
	}
	for _, ttl := range []time.Duration{NoQueueTTL, -time.Hour} {
		if _, ok := queueArguments("orders.dlq", ttl)["x-message-ttl"]; ok {
			t.Errorf("a queue declared with QueueTTL %v has an x-message-ttl", ttl)
		}
	}
	if args := queueArguments("orders.dlq", NoQueueTTL); args["x-dead-letter-routing-key"] != "orders.dlq" {
		t.Errorf("dead-lettering was dropped with the TTL: %v", args)
	}
}
