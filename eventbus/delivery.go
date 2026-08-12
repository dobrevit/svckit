package eventbus

import (
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dobrevit/svckit/logging"
)

// ack and nack record the outcome of a delivery and log a failure to do so.
// A failed acknowledgement is not cosmetic: the broker never learns the
// message was handled, so it redelivers it, and the duplicate surfaces far
// from here. Logging is all that can be done — the channel is already in
// trouble if these fail.
func ack(delivery amqp.Delivery) {
	if err := delivery.Ack(false); err != nil {
		logging.Error("Failed to ack message (it will be redelivered): %v", err)
	}
}

func nack(delivery amqp.Delivery, requeue bool) {
	if err := delivery.Nack(false, requeue); err != nil {
		logging.Error("Failed to nack message (it will be redelivered): %v", err)
	}
}
