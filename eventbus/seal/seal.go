// Package seal encrypts an event's data so that a message carrying personal
// data is not readable in the broker's queues, its dead-letter queues, or
// their backups.
//
// Each message gets a fresh 256-bit data key. The data is encrypted with
// AES-256-GCM under a random 96-bit nonce; the data key is wrapped by a
// KeyWrapper (a key service such as Vault Transit, in the vaultx module, or
// StaticWrapper for tests and development) and travels wrapped, next to the
// ciphertext, in BaseEvent.Sealed. The plaintext data key is zeroed as soon
// as it has been used.
//
// The GCM additional data binds the ciphertext to the event's id, type and
// source, to the contract version and to the key it was wrapped with, so a
// ciphertext moved to another event, or relabelled, does not open. Sign
// after sealing: both signature schemes cover the sealed payload, so a
// consumer verifies before it opens.
package seal

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dobrevit/svckit/eventbus"
)

// Algorithm is SealedPayload.Algorithm for this package's envelopes.
const Algorithm = "AES-256-GCM"

// KeyWrapper issues and unwraps per-message data keys under a named
// key-encryption key.
type KeyWrapper interface {
	// NewDataKey returns a fresh 32-byte data key, the same key wrapped
	// under keyName, and the version of keyName that wrapped it.
	NewDataKey(ctx context.Context, keyName string) (plaintext, wrapped []byte, keyVersion string, err error)

	// Unwrap returns the data key that wrapped was made from.
	Unwrap(ctx context.Context, keyName string, wrapped []byte) ([]byte, error)
}

// Sealer seals events under one key-encryption key.
type Sealer struct {
	wrapper         KeyWrapper
	keyName         string
	contractVersion string
}

// NewSealer returns a Sealer that wraps data keys under keyName and stamps
// envelopes with contractVersion, the version of the message contract the
// data follows; both are bound into the ciphertext.
func NewSealer(wrapper KeyWrapper, keyName, contractVersion string) *Sealer {
	return &Sealer{wrapper: wrapper, keyName: keyName, contractVersion: contractVersion}
}

// Seal encrypts event.Data into event.Sealed and empties event.Data. Seal
// before signing.
func (s *Sealer) Seal(ctx context.Context, event *eventbus.BaseEvent) error {
	if event.Sealed != nil {
		return errors.New("seal: the event is already sealed")
	}
	plaintext, err := json.Marshal(event.Data)
	if err != nil {
		return fmt.Errorf("seal: encode data: %w", err)
	}

	dataKey, wrapped, keyVersion, err := s.wrapper.NewDataKey(ctx, s.keyName)
	if err != nil {
		return fmt.Errorf("seal: new data key: %w", err)
	}
	defer clear(dataKey)
	if len(dataKey) != 32 {
		return fmt.Errorf("seal: the key wrapper returned a %d-byte data key, want 32", len(dataKey))
	}

	payload := &eventbus.SealedPayload{
		Algorithm:       Algorithm,
		KeyName:         s.keyName,
		KeyVersion:      keyVersion,
		ContractVersion: s.contractVersion,
		WrappedKey:      wrapped,
		Nonce:           make([]byte, 12),
	}
	if _, err := rand.Read(payload.Nonce); err != nil {
		return fmt.Errorf("seal: nonce: %w", err)
	}

	aead, err := gcm(dataKey)
	if err != nil {
		return err
	}
	payload.Ciphertext = aead.Seal(nil, payload.Nonce, plaintext, additionalData(event, payload))
	clear(plaintext)

	event.Sealed = payload
	event.Data = nil
	return nil
}

// Opener opens sealed events.
type Opener struct {
	wrapper KeyWrapper
}

// NewOpener returns an Opener that unwraps data keys with wrapper.
func NewOpener(wrapper KeyWrapper) *Opener {
	return &Opener{wrapper: wrapper}
}

// ErrOpen wraps every failure to open an envelope that is there: a wrong
// key, or a ciphertext or label that was changed.
var ErrOpen = errors.New("seal: cannot open the sealed payload")

// Open returns the data sealed in event, leaving the event as it is. An
// unsealed event's Data is returned as it is.
func (o *Opener) Open(ctx context.Context, event *eventbus.BaseEvent) (map[string]any, error) {
	payload := event.Sealed
	if payload == nil {
		return event.Data, nil
	}
	if payload.Algorithm != Algorithm {
		return nil, fmt.Errorf("%w: algorithm %q is not %s", ErrOpen, payload.Algorithm, Algorithm)
	}

	dataKey, err := o.wrapper.Unwrap(ctx, payload.KeyName, payload.WrappedKey)
	if err != nil {
		return nil, fmt.Errorf("%w: unwrap the data key: %w", ErrOpen, err)
	}
	defer clear(dataKey)

	aead, err := gcm(dataKey)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrOpen, err)
	}
	plaintext, err := aead.Open(nil, payload.Nonce, payload.Ciphertext, additionalData(event, payload))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrOpen, err)
	}
	defer clear(plaintext)

	// Decoded as a subscriber decodes an unsealed event's data, so a handler
	// sees the same types either way.
	var data map[string]any
	if err := json.Unmarshal(plaintext, &data); err != nil {
		return nil, fmt.Errorf("%w: decode data: %w", ErrOpen, err)
	}
	return data, nil
}

func gcm(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("seal: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("seal: %w", err)
	}
	return aead, nil
}

// additionalData is what the ciphertext is bound to. Each field is length
// prefixed, so no two different sets of values give the same bytes.
func additionalData(event *eventbus.BaseEvent, payload *eventbus.SealedPayload) []byte {
	var b bytes.Buffer
	for _, field := range []string{
		"svckit-seal-v1", event.ID, event.Type, event.Source,
		payload.ContractVersion, payload.Algorithm, payload.KeyName, payload.KeyVersion,
	} {
		fmt.Fprintf(&b, "%d:%s;", len(field), field)
	}
	return b.Bytes()
}
