package amqpcluster

import (
	"crypto/tls"
	"time"

	"github.com/dobrevit/svckit/internal/amqpconfirm"
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

// amqpChannel is the part of *amqp.Channel the adapter uses, plus the
// confirm-mode publish of amqpconfirm.Channel; amqpconfirm.AMQPChannel, a
// *amqp.Channel with that one method added, satisfies it.
type amqpChannel interface {
	amqpconfirm.Channel

	IsClosed() bool
	Close() error
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Qos(prefetchCount, prefetchSize int, global bool) error
	Cancel(consumer string, noWait bool) error
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
	return amqpconfirm.AMQPChannel{Channel: ch}, nil
}

func dialBroker(url string) (amqpConnection, error) {
	return dialerFor(nil, false)(url)
}

// dialerFor dials with amqp.Dial, or, with TLS settings, amqp.DialConfig
// with amqp.Dial's heartbeat and locale, the TLS config, and EXTERNAL
// authentication when asked for. The TLS config is used for every
// (re)connect, so a GetClientCertificate that returns a renewed
// certificate is used from the next connection on.
func dialerFor(tlsConfig *tls.Config, external bool) dialFunc {
	return func(url string) (amqpConnection, error) {
		var conn *amqp.Connection
		var err error
		if tlsConfig == nil && !external {
			conn, err = amqp.Dial(url)
		} else {
			config := amqp.Config{
				Heartbeat:       10 * time.Second,
				Locale:          "en_US",
				TLSClientConfig: tlsConfig,
			}
			if external {
				config.SASL = []amqp.Authentication{&amqp.ExternalAuth{}}
			}
			conn, err = amqp.DialConfig(url, config)
		}
		if err != nil {
			return nil, err
		}
		return brokerConnection{conn}, nil
	}
}
