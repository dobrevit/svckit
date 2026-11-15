package eventbus

import (
	"context"
	"strings"
	"testing"
	"time"
)

func hmacAt(window time.Duration, at *time.Time) *HMAC {
	h := NewHMAC(&SignatureConfig{
		SigningKey:     []byte("signer-test-key-0123456789"),
		ExpiryWindow:   window,
		RequiredClaims: []string{"id", "type", "timestamp", "source"},
	})
	h.now = func() time.Time { return *at }
	return h
}

func TestHMACRoundTripsThroughTheInterfaces(t *testing.T) {
	now := time.Now()
	h := hmacAt(30*time.Minute, &now)
	var signer Signer = h
	var verifier Verifier = h

	signed, err := signer.Sign(context.Background(), NewEvent("order.created", "orders", map[string]any{"id": "o-1"}))
	if err != nil {
		t.Fatal(err)
	}
	if signed.Scheme != SchemeHMACSHA256 {
		t.Errorf("Scheme = %q, want %q", signed.Scheme, SchemeHMACSHA256)
	}
	if err := verifier.Verify(context.Background(), signed); err != nil {
		t.Errorf("Verify: %v", err)
	}
}

// A retry schedule or an outage can outlast the 30 minutes that used to be
// fixed; the verifier's window decides.
func TestTheVerifiersWindowDecidesTheMaximumAge(t *testing.T) {
	signedAt := time.Now()
	at := signedAt
	signed, err := hmacAt(30*time.Minute, &at).Sign(context.Background(), NewEvent("order.created", "orders", nil))
	if err != nil {
		t.Fatal(err)
	}

	verifier := hmacAt(2*time.Hour, &at)
	at = signedAt.Add(time.Hour)
	if err := verifier.Verify(context.Background(), signed); err != nil {
		t.Errorf("a message one hour old was refused under a two-hour window: %v", err)
	}
	at = signedAt.Add(3 * time.Hour)
	if err := verifier.Verify(context.Background(), signed); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Errorf("a message three hours old was accepted under a two-hour window (err %v)", err)
	}
}

// Messages from before the field existed have no Scheme; a scheme this
// verifier does not implement is refused rather than misread.
func TestTheSchemeIsChecked(t *testing.T) {
	now := time.Now()
	h := hmacAt(30*time.Minute, &now)
	signed, err := h.Sign(context.Background(), NewEvent("order.created", "orders", nil))
	if err != nil {
		t.Fatal(err)
	}

	signed.Scheme = ""
	if err := h.Verify(context.Background(), signed); err != nil {
		t.Errorf("a message without a scheme was refused: %v", err)
	}
	signed.Scheme = "x509-ecdsa-p256"
	if err := h.Verify(context.Background(), signed); err == nil {
		t.Error("a message in another scheme was accepted")
	}
}

func TestEventSignatureMaxAge(t *testing.T) {
	t.Setenv("EVENT_SIGNING_KEY", "max-age-test-key-0123456789")
	t.Setenv("EVENT_SIGNING_KEY_REQUIRED", "")

	t.Setenv("EVENT_SIGNATURE_MAX_AGE", "2h")
	config, err := LoadSignatureConfig(nil)
	if err != nil || config.ExpiryWindow != 2*time.Hour {
		t.Errorf("EVENT_SIGNATURE_MAX_AGE=2h: window %v, err %v", config.ExpiryWindow, err)
	}
	if DefaultSignatureConfig().ExpiryWindow != 2*time.Hour {
		t.Error("DefaultSignatureConfig ignores EVENT_SIGNATURE_MAX_AGE")
	}

	t.Setenv("EVENT_SIGNATURE_MAX_AGE", "")
	if config, _ := LoadSignatureConfig(nil); config.ExpiryWindow != DefaultSignatureWindow {
		t.Errorf("unset: window %v, want %v", config.ExpiryWindow, DefaultSignatureWindow)
	}

	for _, bad := range []string{"soon", "-1h", "0s"} {
		t.Setenv("EVENT_SIGNATURE_MAX_AGE", bad)
		if _, err := LoadSignatureConfig(nil); err == nil {
			t.Errorf("EVENT_SIGNATURE_MAX_AGE=%q was accepted", bad)
		}
	}
}

// A service with no key configured used to run on a key published in this
// repository, with a Warn.
func TestStrictLoadingRefusesTheFallback(t *testing.T) {
	t.Setenv("EVENT_SIGNATURE_MAX_AGE", "")
	t.Setenv("EVENT_SIGNING_KEY_REQUIRED", "")

	t.Setenv("EVENT_SIGNING_KEY", "")
	if _, err := LoadSignatureConfigStrict(nil); err == nil {
		t.Error("strict loading with no key succeeded")
	}
	t.Setenv("EVENT_SIGNING_KEY", DevelopmentSigningKey)
	if _, err := LoadSignatureConfigStrict(nil); err == nil {
		t.Error("strict loading accepted the published development key")
	}
	t.Setenv("EVENT_SIGNING_KEY", "a-real-key-0123456789abcdef")
	if _, err := LoadSignatureConfigStrict(nil); err != nil {
		t.Errorf("strict loading refused a real key: %v", err)
	}

	t.Setenv("EVENT_SIGNING_KEY", "")
	if config, err := LoadSignatureConfig(nil); err != nil || string(config.SigningKey) != DevelopmentSigningKey {
		t.Errorf("lenient loading without a key: %v, want the fallback as before", err)
	}
	t.Setenv("EVENT_SIGNING_KEY_REQUIRED", "true")
	if _, err := LoadSignatureConfig(nil); err == nil {
		t.Error("EVENT_SIGNING_KEY_REQUIRED=true did not make LoadSignatureConfig strict")
	}
}
