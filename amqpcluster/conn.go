package amqpcluster

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// amqpConnection is the part of *amqp.Connection the adapter uses. It is an
// interface so that reconnection, which is where the adapter's locking is
// hardest to get right, can be tested without a broker.
type amqpConnection interface {
	Channel() (amqpChannel, error)
	IsClosed() bool
	Close() error
}

// amqpChannel is the part of *amqp.Channel the adapter uses; *amqp.Channel
// satisfies it as it is.
type amqpChannel interface {
	IsClosed() bool
	Close() error
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
}

// dialFunc opens a connection to one node.
type dialFunc func(url string) (amqpConnection, error)

// brokerConnection adapts *amqp.Connection, whose Channel method returns the
// concrete channel type, to amqpConnection.
type brokerConnection struct{ *amqp.Connection }

func (c brokerConnection) Channel() (amqpChannel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		// Not ch: a nil *amqp.Channel in an interface is not a nil interface.
		return nil, err
	}
	return ch, nil
}

func dialBroker(url string) (amqpConnection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	return brokerConnection{conn}, nil
}
