package amqpcluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/dobrevit/svckit/eventbus"
	amqp "github.com/rabbitmq/amqp091-go"
)

// captureLogs sends slog's default logger, which the logging helpers write
// through, to a buffer at level, until the test ends.
func captureLogs(t *testing.T, level slog.Level) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: level})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

const (
	personalUserID = "user-7f3a-personal"
	bodyMarker     = "body-marker-not-for-logs"
)

// oneMessage publishes a signed event on behalf of a user through a
// ClusterPublisher over a fake node, then hands the published message to a
// verifying subscriber whose handler returns handlerErr.
func oneMessage(t *testing.T, logs *bytes.Buffer, handlerErr error) (logged string, event *eventbus.BaseEvent) {
	t.Helper()
	ca, _, _ := clusterOf(t, 1, "rabbit-1")
	pub := &ClusterPublisher{cluster: ca, serviceName: "orders", signer: eventbus.NewEventSigner(testSigning), exchange: "events"}
	sub := subscriberOn(ca, "billing", testSigning, false)

	event = eventbus.NewEvent("order.created", "orders", map[string]any{"note": bodyMarker})
	event.SetUserID(personalUserID)

	logs.Reset() // only what the message itself causes
	if err := pub.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	signed, err := eventbus.SignEvent(event, testSigning)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(signed)
	d := amqp.Delivery{Body: body, Headers: amqp.Table{"signed": true, "user_id": personalUserID}, Acknowledger: &acknowledger{}}
	sub.handleMessage("orders", d, failingHandler{handlerErr}, SubscriptionOptions{})
	return logs.String(), event
}

// A line per message at Info is noise at any volume, and a user id is
// personal data in most deployments.
func TestSuccessfulMessagesLogNothingAtInfo(t *testing.T) {
	logs := captureLogs(t, slog.LevelInfo)
	if logged, _ := oneMessage(t, logs, nil); logged != "" {
		t.Errorf("a published, verified and handled message logged at Info:\n%s", logged)
	}
}

func TestDebugLinesCarryNoUserIDAndNoBody(t *testing.T) {
	logs := captureLogs(t, slog.LevelDebug)
	logged, _ := oneMessage(t, logs, nil)
	if logged == "" {
		t.Fatal("nothing was logged at Debug; the test would prove nothing")
	}
	for _, leak := range []string{personalUserID, bodyMarker} {
		if strings.Contains(logged, leak) {
			t.Errorf("a log line contains %q:\n%s", leak, logged)
		}
	}
}

func TestAFailureNamesTheEventButNotItsContents(t *testing.T) {
	logs := captureLogs(t, slog.LevelInfo)
	logged, event := oneMessage(t, logs, eventbus.Permanent(errors.New("stock service said no")))

	if !strings.Contains(logged, "order.created") || !strings.Contains(logged, event.ID) || !strings.Contains(logged, "stock service said no") {
		t.Errorf("the failure line does not name the type, the id and the error:\n%s", logged)
	}
	for _, leak := range []string{personalUserID, bodyMarker} {
		if strings.Contains(logged, leak) {
			t.Errorf("a log line contains %q:\n%s", leak, logged)
		}
	}
}
