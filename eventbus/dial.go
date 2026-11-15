package eventbus

import (
	"crypto/tls"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ConnectionOption configures how a publisher or subscriber connects. It is
// both a PublisherOption and a SubscriberOption.
type ConnectionOption interface {
	PublisherOption
	SubscriberOption
}

// WithTLS connects over TLS with config, which should carry the client's
// certificate through GetClientCertificate so that a renewing source is
// asked again on every connection (vaultx.CertSource provides one). With
// external, the client authenticates with that certificate (SASL EXTERNAL)
// instead of the URL's user and password: the broker takes the user name
// from the certificate, which needs its rabbitmq_auth_mechanism_ssl plugin
// and an amqps:// URL.
func WithTLS(config *tls.Config, external bool) ConnectionOption {
	return tlsOption{dialSettings{tls: config, external: external}}
}

type tlsOption struct{ settings dialSettings }

func (o tlsOption) applyPublisher(p *Publisher)   { p.dialing = o.settings }
func (o tlsOption) applySubscriber(s *Subscriber) { s.dialing = o.settings }

// dialSettings is how to connect: plain amqp.Dial when empty.
type dialSettings struct {
	tls      *tls.Config
	external bool
}

// dial opens a connection. With no TLS settings it is amqp.Dial; otherwise
// amqp.DialConfig with amqp.Dial's heartbeat and locale, the TLS config, and
// EXTERNAL authentication when asked for.
func (d dialSettings) dial(url string) (*amqp.Connection, error) {
	if d.tls == nil && !d.external {
		return amqp.Dial(url)
	}
	config := amqp.Config{
		Heartbeat:       10 * time.Second,
		Locale:          "en_US",
		TLSClientConfig: d.tls,
	}
	if d.external {
		config.SASL = []amqp.Authentication{&amqp.ExternalAuth{}}
	}
	return amqp.DialConfig(url, config)
}
