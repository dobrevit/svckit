package eventbus

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestWithTransientSetsTheDeliveryMode(t *testing.T) {
	p := &Publisher{deliveryMode: amqp.Persistent}
	WithTransient()(p)
	if p.deliveryMode != amqp.Transient {
		t.Errorf("delivery mode = %d, want transient (%d)", p.deliveryMode, amqp.Transient)
	}
}
