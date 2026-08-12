package eventbus_test

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
)

const testKey = "a-test-signing-key-long-enough"

func testConfig() *eventbus.SignatureConfig {
	return &eventbus.SignatureConfig{
		SigningKey:     []byte(testKey),
		ExpiryWindow:   30 * time.Minute,
		RequiredClaims: []string{"id", "type", "timestamp", "source"},
	}
}

func signedTestEvent(t *testing.T, config *eventbus.SignatureConfig) *eventbus.SignedEvent {
	t.Helper()
	event := eventbus.NewEvent("order.created", "orders", map[string]any{"order_id": "o-1"})
	signed, err := eventbus.SignEvent(event, config)
	if err != nil {
		t.Fatalf("SignEvent: %v", err)
	}
	return signed
}

func TestSignAndVerifyRoundTrip(t *testing.T) {
	config := testConfig()
	signed := signedTestEvent(t, config)

	if signed.Signature == "" {
		t.Fatal("no signature was produced")
	}
	if !signed.ExpiresAt.After(signed.SignedAt) {
		t.Errorf("ExpiresAt %v is not after SignedAt %v", signed.ExpiresAt, signed.SignedAt)
	}
	if err := eventbus.VerifyEvent(signed, config); err != nil {
		t.Errorf("a freshly signed event failed verification: %v", err)
	}
}

// The whole point of signing: a peer without the key cannot pass verification.
func TestVerifyRejectsAnotherKey(t *testing.T) {
	signed := signedTestEvent(t, testConfig())

	other := testConfig()
	other.SigningKey = []byte("a-different-key-also-long-enough")

	if err := eventbus.VerifyEvent(signed, other); err == nil {
		t.Fatal("an event signed with another key passed verification")
	}
}

func TestVerifyRejectsATamperedEvent(t *testing.T) {
	config := testConfig()

	cases := map[string]func(*eventbus.SignedEvent){
		"changed data":      func(s *eventbus.SignedEvent) { s.Event.Data["order_id"] = "o-999" },
		"changed type":      func(s *eventbus.SignedEvent) { s.Event.Type = "order.deleted" },
		"changed source":    func(s *eventbus.SignedEvent) { s.Event.Source = "impostor" },
		"changed ID":        func(s *eventbus.SignedEvent) { s.Event.ID = "e-forged" },
		"changed signature": func(s *eventbus.SignedEvent) { s.Signature = "0000000000" },
		"extended expiry":   func(s *eventbus.SignedEvent) { s.ExpiresAt = s.ExpiresAt.Add(time.Hour) },
	}

	for name, tamper := range cases {
		t.Run(name, func(t *testing.T) {
			signed := signedTestEvent(t, config)
			tamper(signed)

			if err := eventbus.VerifyEvent(signed, config); err == nil {
				t.Errorf("verification passed after %s", name)
			}
		})
	}
}

// An expired signature is what bounds a replay: a captured event stops being
// accepted once its window has passed.
func TestVerifyRejectsAnExpiredSignature(t *testing.T) {
	config := testConfig()
	config.ExpiryWindow = -time.Second // already past when signed

	signed := signedTestEvent(t, config)

	err := eventbus.VerifyEvent(signed, config)
	if err == nil {
		t.Fatal("an expired signature was accepted")
	}
	if !strings.Contains(err.Error(), "expired") {
		t.Errorf("error = %v, want it to name expiry", err)
	}
}

func TestSignRejectsAnEventMissingRequiredClaims(t *testing.T) {
	config := testConfig()

	incomplete := &eventbus.BaseEvent{Type: "order.created", Source: "orders", Timestamp: time.Now()}
	if _, err := eventbus.SignEvent(incomplete, config); err == nil {
		t.Error("an event with no ID was signed")
	}
}

func TestSignedEventSurvivesJSON(t *testing.T) {
	config := testConfig()
	signed := signedTestEvent(t, config)

	encoded, err := signed.Event.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	decoded, err := eventbus.FromJSON(encoded)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}

	// Verification has to survive the wire round trip, or nothing published
	// could ever be verified by the consumer.
	roundTripped := &eventbus.SignedEvent{
		Event:     decoded,
		Signature: signed.Signature,
		SignedAt:  signed.SignedAt,
		ExpiresAt: signed.ExpiresAt,
	}
	if err := eventbus.VerifyEvent(roundTripped, config); err != nil {
		t.Errorf("an event failed verification after a JSON round trip: %v", err)
	}
}

func TestRotateSigningKeyKeepsTheOtherSettings(t *testing.T) {
	old := testConfig()
	rotated := eventbus.RotateSigningKey(old, []byte("the-new-key-also-long-enough"))

	if string(rotated.SigningKey) == testKey {
		t.Error("the key was not rotated")
	}
	if rotated.ExpiryWindow != old.ExpiryWindow {
		t.Errorf("ExpiryWindow = %v, want %v", rotated.ExpiryWindow, old.ExpiryWindow)
	}
	if len(rotated.RequiredClaims) != len(old.RequiredClaims) {
		t.Errorf("RequiredClaims = %v, want %v", rotated.RequiredClaims, old.RequiredClaims)
	}
	// Events signed with the old key must stop verifying under the new one.
	if err := eventbus.VerifyEvent(signedTestEvent(t, old), rotated); err == nil {
		t.Error("an event signed with the retired key still verifies")
	}
}

func TestSignerAndVerifierWrapTheFunctions(t *testing.T) {
	config := testConfig()
	signer := eventbus.NewEventSigner(config)
	verifier := eventbus.NewEventVerifier(config)

	signed, err := signer.Sign(eventbus.NewEvent("order.created", "orders", nil))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if err := verifier.Verify(signed); err != nil {
		t.Errorf("Verify: %v", err)
	}
}

// Key material must never reach stdout or a log aggregator. Signing was fixed
// for this earlier; verification kept printing the first eight characters and
// the exact length of the key on every call.
func TestNoKeyMaterialIsPrinted(t *testing.T) {
	config := testConfig()
	signed := signedTestEvent(t, config)

	stdout, stderr := os.Stdout, os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("creating pipe: %v", err)
	}
	os.Stdout, os.Stderr = w, w
	t.Cleanup(func() { os.Stdout, os.Stderr = stdout, stderr })

	if err := eventbus.VerifyEvent(signed, config); err != nil {
		t.Fatalf("VerifyEvent: %v", err)
	}
	_ = w.Close()

	var captured bytes.Buffer
	if _, err := io.Copy(&captured, r); err != nil {
		t.Fatalf("reading captured output: %v", err)
	}
	os.Stdout, os.Stderr = stdout, stderr

	output := captured.String()
	if strings.Contains(output, testKey[:8]) {
		t.Errorf("verification printed key material: %q", output)
	}
	if strings.Contains(strings.ToLower(output), "using key") {
		t.Errorf("verification printed a key debug line: %q", output)
	}
}

func TestLoadSignatureConfigReadsTheEnvironment(t *testing.T) {
	t.Setenv("EVENT_SIGNING_KEY", "an-environment-key-long-enough")
	t.Setenv("SECRETS_BACKEND", "env")

	config, err := eventbus.LoadSignatureConfig(nil)
	if err != nil {
		t.Fatalf("LoadSignatureConfig: %v", err)
	}
	if string(config.SigningKey) != "an-environment-key-long-enough" {
		t.Errorf("SigningKey = %q", config.SigningKey)
	}
	if config.ExpiryWindow <= 0 {
		t.Errorf("ExpiryWindow = %v, want a positive window", config.ExpiryWindow)
	}
}

// A short key is a configuration mistake that must fail loudly at startup
// rather than quietly weaken every signature.
func TestLoadSignatureConfigRejectsAShortKey(t *testing.T) {
	t.Setenv("EVENT_SIGNING_KEY", "tooshort")
	t.Setenv("SECRETS_BACKEND", "env")

	if _, err := eventbus.LoadSignatureConfig(nil); err == nil {
		t.Fatal("a key shorter than the minimum was accepted")
	}
}
