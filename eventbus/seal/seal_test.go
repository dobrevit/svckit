package seal

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
)

const secret = "Jane Example, born 1970-01-01, member no. 4711"

func testWrapper(t *testing.T) *StaticWrapper {
	t.Helper()
	kek := make([]byte, 32)
	_, _ = rand.Read(kek)
	w, err := NewStaticWrapper(kek, "1")
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func personEvent() *eventbus.BaseEvent {
	return eventbus.NewEvent("member.joined", "members", map[string]any{"person": secret, "branch": 12})
}

// sealed returns a sealed event and what its Data was, as a subscriber would
// decode it.
func sealed(t *testing.T, w KeyWrapper) (*eventbus.BaseEvent, map[string]any) {
	t.Helper()
	e := personEvent()
	raw, _ := json.Marshal(e.Data)
	var want map[string]any
	_ = json.Unmarshal(raw, &want)
	if err := NewSealer(w, "members", "member.v1").Seal(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	return e, want
}

func TestOpenReturnsTheSealedData(t *testing.T) {
	w := testWrapper(t)
	e, want := sealed(t, w)
	if e.Data != nil || e.Sealed == nil {
		t.Fatalf("after Seal: Data %v, Sealed %v", e.Data, e.Sealed)
	}
	if e.Sealed.Algorithm != Algorithm || e.Sealed.KeyName != "members" || e.Sealed.KeyVersion != "1" || e.Sealed.ContractVersion != "member.v1" {
		t.Errorf("envelope labels = %+v", e.Sealed)
	}

	got, err := NewOpener(w).Open(context.Background(), e)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Open = %v, want %v", got, want)
	}
}

func TestAChangedLabelOrCiphertextDoesNotOpen(t *testing.T) {
	w := testWrapper(t)
	for name, change := range map[string]func(*eventbus.BaseEvent){
		"id":               func(e *eventbus.BaseEvent) { e.ID = "another-id" },
		"type":             func(e *eventbus.BaseEvent) { e.Type = "member.left" },
		"source":           func(e *eventbus.BaseEvent) { e.Source = "billing" },
		"contract version": func(e *eventbus.BaseEvent) { e.Sealed.ContractVersion = "member.v2" },
		"key name":         func(e *eventbus.BaseEvent) { e.Sealed.KeyName = "other" },
		"ciphertext byte":  func(e *eventbus.BaseEvent) { e.Sealed.Ciphertext[3] ^= 1 },
		"last byte (tag)":  func(e *eventbus.BaseEvent) { e.Sealed.Ciphertext[len(e.Sealed.Ciphertext)-1] ^= 1 },
		"nonce":            func(e *eventbus.BaseEvent) { e.Sealed.Nonce[0] ^= 1 },
		"wrapped key":      func(e *eventbus.BaseEvent) { e.Sealed.WrappedKey[20] ^= 1 },
	} {
		e, _ := sealed(t, w)
		change(e)
		if _, err := NewOpener(w).Open(context.Background(), e); !errors.Is(err, ErrOpen) {
			t.Errorf("%s changed: Open = %v, want ErrOpen", name, err)
		}
	}

	// Every byte of the ciphertext, not just a sample.
	e, _ := sealed(t, w)
	for i := range e.Sealed.Ciphertext {
		e.Sealed.Ciphertext[i] ^= 0x80
		if _, err := NewOpener(w).Open(context.Background(), e); err == nil {
			t.Fatalf("a change to ciphertext byte %d still opened", i)
		}
		e.Sealed.Ciphertext[i] ^= 0x80
	}
}

func TestAnotherKeyDoesNotOpen(t *testing.T) {
	e, _ := sealed(t, testWrapper(t))
	if _, err := NewOpener(testWrapper(t)).Open(context.Background(), e); !errors.Is(err, ErrOpen) {
		t.Errorf("Open under another key = %v", err)
	}
}

func TestTwoSealsOfOnePayloadDiffer(t *testing.T) {
	w := testWrapper(t)
	a, _ := sealed(t, w)
	b, _ := sealed(t, w)
	if bytes.Equal(a.Sealed.Ciphertext, b.Sealed.Ciphertext) || bytes.Equal(a.Sealed.WrappedKey, b.Sealed.WrappedKey) || bytes.Equal(a.Sealed.Nonce, b.Sealed.Nonce) {
		t.Error("two seals share a ciphertext, a wrapped key or a nonce")
	}
}

// What sits in the broker, and its backups, must not contain the data.
func TestTheMarshalledEventHoldsNoPlaintext(t *testing.T) {
	e, _ := sealed(t, testWrapper(t))
	wire, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{secret, "Jane", "4711", "1970-01-01"} {
		if bytes.Contains(wire, []byte(fragment)) {
			t.Errorf("the sealed event contains %q:\n%s", fragment, wire)
		}
	}
}

func TestAnUnsealedEventOpensToItsData(t *testing.T) {
	e := personEvent()
	got, err := NewOpener(testWrapper(t)).Open(context.Background(), e)
	if err != nil || got["person"] != secret {
		t.Errorf("Open of an unsealed event = %v, %v", got, err)
	}
}

// Sign after sealing: the signature covers the sealed payload, so changing
// it is caught before anyone opens it.
func TestAnHMACSignatureCoversTheSealedPayload(t *testing.T) {
	e, _ := sealed(t, testWrapper(t))
	h := eventbus.NewHMAC(&eventbus.SignatureConfig{
		SigningKey:     []byte("seal-test-signing-key-0123"),
		ExpiryWindow:   time.Hour,
		RequiredClaims: []string{"id", "type", "timestamp", "source"},
	})
	signed, err := h.Sign(context.Background(), e)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Verify(context.Background(), signed); err != nil {
		t.Fatalf("Verify of a sealed event: %v", err)
	}
	signed.Event.Sealed.Ciphertext[0] ^= 1
	if err := h.Verify(context.Background(), signed); err == nil {
		t.Error("a signature still verified after the sealed payload changed")
	}
}

func TestSealingTwiceIsRefused(t *testing.T) {
	w := testWrapper(t)
	e, _ := sealed(t, w)
	if err := NewSealer(w, "members", "member.v1").Seal(context.Background(), e); err == nil {
		t.Error("an already sealed event was sealed again")
	}
}
