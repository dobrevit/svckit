package amqpcluster

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	amqp "github.com/rabbitmq/amqp091-go"
)

// stampSigner is a Signer of another scheme, standing in for a certificate
// signer.
type stampSigner struct{}

func (stampSigner) Sign(_ context.Context, e *eventbus.BaseEvent) (*eventbus.SignedEvent, error) {
	return &eventbus.SignedEvent{Event: e, Signature: "stamp", SignedAt: time.Now(), Scheme: "stamp"}, nil
}

// stampVerifier accepts what stampSigner signs.
type stampVerifier struct{}

func (stampVerifier) Verify(_ context.Context, s *eventbus.SignedEvent) error {
	if s.Scheme != "stamp" || s.Signature != "stamp" {
		return errors.New("not stamped")
	}
	return nil
}

func TestClusterPublisherUsesTheSignerItIsGiven(t *testing.T) {
	ca, nodes, _ := clusterOf(t, 1, "rabbit-1")
	pub := &ClusterPublisher{cluster: ca, serviceName: "orders", exchange: "events", signer: eventbus.NewHMAC(testSigning)}
	WithSigner(stampSigner{})(pub)

	if err := pub.Publish(context.Background(), eventbus.NewEvent("order.created", "orders", nil)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg, ok := publishChannel(nodes[0]).lastPublished()
	if !ok {
		t.Fatal("nothing was published")
	}
	var got eventbus.SignedEvent
	if err := json.Unmarshal(msg.Body, &got); err != nil || got.Scheme != "stamp" {
		t.Errorf("published scheme %q, err %v; want the given signer's, not the config's HMAC", got.Scheme, err)
	}
}

func TestClusterSubscriberUsesTheVerifierItIsGiven(t *testing.T) {
	cs := subscriberOn(nil, "billing", nil, false)
	WithVerifier(stampVerifier{})(cs)

	signed, _ := stampSigner{}.Sign(context.Background(), eventbus.NewEvent("order.created", "orders", nil))
	body, _ := json.Marshal(signed)
	d := amqp.Delivery{Body: body, Headers: amqp.Table{"signed": true}, Acknowledger: &acknowledger{}}
	h := &recordingHandler{}
	cs.handleMessage("orders", d, h, SubscriptionOptions{})
	if h.calls != 1 {
		t.Error("a message the given verifier accepts did not reach the handler")
	}

	// And, having a verifier, the subscriber refuses unsigned messages.
	unsigned := unsignedDelivery(t, nil)
	cs.handleMessage("orders", unsigned, h, SubscriptionOptions{})
	if h.calls != 1 {
		t.Error("an unsigned message reached a subscriber with a verifier")
	}
}

func TestLoadConfigFromEnvHonoursEventSigningKeyRequired(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	t.Setenv("RABBITMQ_CLUSTER_NODES", "")
	t.Setenv("EVENT_SIGNING_KEY_REQUIRED", "true")

	t.Setenv("EVENT_SIGNING_KEY", "")
	if _, err := LoadConfigFromEnv("orders"); err == nil {
		t.Error("no key with EVENT_SIGNING_KEY_REQUIRED=true was accepted")
	}
	t.Setenv("EVENT_SIGNING_KEY", eventbus.DevelopmentSigningKey)
	if _, err := LoadConfigFromEnv("orders"); err == nil {
		t.Error("the published development key with EVENT_SIGNING_KEY_REQUIRED=true was accepted")
	}
	t.Setenv("EVENT_SIGNING_KEY", "a-real-key-0123456789abcdef")
	if _, err := LoadConfigFromEnv("orders"); err != nil {
		t.Errorf("a real key was refused: %v", err)
	}
}
