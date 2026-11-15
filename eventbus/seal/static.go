package seal

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
)

// StaticWrapper wraps data keys under one local 32-byte key with
// AES-256-GCM, the key name bound as additional data. It is for tests and
// development: the key-encryption key sits in the process, so whoever can
// read the process can open every message. Production wraps with a key
// service (vaultx.TransitWrapper), where the key never leaves it.
type StaticWrapper struct {
	kek     []byte
	version string
}

// NewStaticWrapper returns a StaticWrapper over a 32-byte key; version is
// reported as each envelope's key version.
func NewStaticWrapper(kek []byte, version string) (*StaticWrapper, error) {
	if len(kek) != 32 {
		return nil, fmt.Errorf("seal: the static key must be 32 bytes, got %d", len(kek))
	}
	return &StaticWrapper{kek: append([]byte(nil), kek...), version: version}, nil
}

// NewDataKey returns a fresh data key and that key wrapped under the static
// key.
func (w *StaticWrapper) NewDataKey(_ context.Context, keyName string) ([]byte, []byte, string, error) {
	dataKey := make([]byte, 32)
	if _, err := rand.Read(dataKey); err != nil {
		return nil, nil, "", err
	}
	aead, err := gcm(w.kek)
	if err != nil {
		return nil, nil, "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, "", err
	}
	wrapped := aead.Seal(nonce, nonce, dataKey, []byte(keyName))
	return dataKey, wrapped, w.version, nil
}

// Unwrap returns the data key wrapped under the static key.
func (w *StaticWrapper) Unwrap(_ context.Context, keyName string, wrapped []byte) ([]byte, error) {
	aead, err := gcm(w.kek)
	if err != nil {
		return nil, err
	}
	if len(wrapped) < aead.NonceSize() {
		return nil, errors.New("seal: wrapped key too short")
	}
	return aead.Open(nil, wrapped[:aead.NonceSize()], wrapped[aead.NonceSize():], []byte(keyName))
}
