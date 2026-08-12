// Package amqpcluster connects to a RabbitMQ cluster rather than a single
// broker: publishers and subscribers hold connections to every configured
// node, fail over when one goes away, and reconnect in the background.
//
// It is the transport eventbus publishes over; use eventbus for the event
// semantics (signing, envelopes, dispatch) and this package for the
// connection behaviour underneath.
package amqpcluster
