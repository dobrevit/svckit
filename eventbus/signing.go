package eventbus

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/dobrevit/svckit/logging"
)

// SignatureConfig holds configuration for event signing
type SignatureConfig struct {
	SigningKey []byte

	// ExpiryWindow is how long a signature is valid. The signer writes
	// SignedAt plus its window into ExpiresAt; a verifier with a window of
	// its own rejects a message older than that window, measured from
	// SignedAt, whatever the message's ExpiresAt says, so the consumer
	// decides how old a message it accepts (a retry schedule that outlasts
	// the publisher's window needs that). A verifier with no window (zero
	// or less) falls back to the message's ExpiresAt. EVENT_SIGNATURE_MAX_AGE
	// sets it for LoadSignatureConfig and DefaultSignatureConfig; the
	// default is 30 minutes.
	ExpiryWindow   time.Duration
	RequiredClaims []string // Required fields in the event for signing
}

// SchemeHMACSHA256 is the signature scheme of HMAC; a SignedEvent with an
// empty Scheme, as every message before the field existed, is this one.
const SchemeHMACSHA256 = "hmac-sha256"

// SignedEvent extends BaseEvent with cryptographic signature
type SignedEvent struct {
	Event     *BaseEvent `json:"event"`
	Signature string     `json:"signature"`
	SignedAt  time.Time  `json:"signed_at"`
	ExpiresAt time.Time  `json:"expires_at"`

	// Scheme names the signature scheme, so a verifier refuses a message
	// signed with one it does not implement instead of misreading it.
	Scheme string `json:"scheme,omitempty"`

	// Certificate is the signer's leaf certificate (DER), for schemes that
	// sign with a certificate's key (eventbus/x509sig). Empty for HMAC.
	Certificate []byte `json:"certificate,omitempty"`
}

// Signer signs events. HMAC is the shared-key implementation; publishers
// take any Signer through WithSigner.
type Signer interface {
	Sign(ctx context.Context, event *BaseEvent) (*SignedEvent, error)
}

// Verifier checks a signed event's signature, scheme and age. HMAC is the
// shared-key implementation; subscribers take any Verifier through
// WithVerifier.
type Verifier interface {
	Verify(ctx context.Context, signed *SignedEvent) error
}

// DevelopmentSigningKey is the key DefaultSignatureConfig and
// LoadSignatureConfig fall back to when none is configured. It is published
// in this repository, so a service running on it signs nothing anyone could
// not forge; LoadSignatureConfigStrict refuses it.
const DevelopmentSigningKey = "dev-signing-key-change-in-production"

// DefaultSignatureWindow is ExpiryWindow when EVENT_SIGNATURE_MAX_AGE is not
// set.
const DefaultSignatureWindow = 30 * time.Minute

// DefaultSignatureConfig returns the default configuration for event signing
func DefaultSignatureConfig() *SignatureConfig {
	signingKey := []byte(os.Getenv("EVENT_SIGNING_KEY"))
	if len(signingKey) == 0 {
		// Fallback for development - DO NOT USE IN PRODUCTION
		signingKey = []byte(DevelopmentSigningKey)
	}

	window, err := signatureMaxAge()
	if err != nil {
		logging.Warn("Ignoring %v; using %v", err, DefaultSignatureWindow)
		window = DefaultSignatureWindow
	}

	return &SignatureConfig{
		SigningKey:     signingKey,
		ExpiryWindow:   window,
		RequiredClaims: []string{"id", "type", "timestamp", "source"},
	}
}

// signatureMaxAge reads EVENT_SIGNATURE_MAX_AGE, a Go duration.
func signatureMaxAge() (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv("EVENT_SIGNATURE_MAX_AGE"))
	if raw == "" {
		return DefaultSignatureWindow, nil
	}
	window, err := time.ParseDuration(raw)
	if err != nil || window <= 0 {
		return 0, fmt.Errorf("EVENT_SIGNATURE_MAX_AGE %q is not a positive duration", raw)
	}
	return window, nil
}

// HMAC signs and verifies events with HMAC-SHA256 over one shared key. It
// implements Signer and Verifier. Every holder of the key can sign as any
// service; where that matters, use certificate signatures instead.
type HMAC struct {
	config *SignatureConfig
	now    func() time.Time // nil means time.Now; tests replace it
}

// NewHMAC returns the HMAC scheme over config (DefaultSignatureConfig when
// nil).
func NewHMAC(config *SignatureConfig) *HMAC {
	if config == nil {
		config = DefaultSignatureConfig()
	}
	return &HMAC{config: config}
}

func (h *HMAC) clock() time.Time {
	if h.now != nil {
		return h.now()
	}
	return time.Now()
}

// Sign signs event.
func (h *HMAC) Sign(_ context.Context, event *BaseEvent) (*SignedEvent, error) {
	return signEventAt(event, h.config, h.clock())
}

// Verify checks signed's scheme, age and signature.
func (h *HMAC) Verify(_ context.Context, signed *SignedEvent) error {
	return verifyEventAt(signed, h.config, h.clock())
}

// SignEvent creates a cryptographically signed version of the event
func SignEvent(event *BaseEvent, config *SignatureConfig) (*SignedEvent, error) {
	return signEventAt(event, config, time.Now())
}

func signEventAt(event *BaseEvent, config *SignatureConfig, now time.Time) (*SignedEvent, error) {
	if config == nil {
		config = DefaultSignatureConfig()
	}

	// No key material is logged here. This used to print the first eight
	// characters and the exact length of the signing key on every publish,
	// which both leaked key material into any log aggregator and — because the
	// development default is a constant in this repository — announced in
	// production logs precisely which key was in use.

	// Validate required claims
	if err := validateRequiredClaims(event, config.RequiredClaims); err != nil {
		return nil, fmt.Errorf("event validation failed: %w", err)
	}

	signedAt := now
	expiresAt := signedAt.Add(config.ExpiryWindow)

	// Create canonical representation for signing
	canonical, err := createCanonicalEventData(event, signedAt, expiresAt)
	if err != nil {
		return nil, fmt.Errorf("failed to create canonical data: %w", err)
	}

	// Generate HMAC-SHA256 signature
	signature, err := generateSignature(canonical, config.SigningKey)
	if err != nil {
		return nil, fmt.Errorf("failed to generate signature: %w", err)
	}

	return &SignedEvent{
		Event:     event,
		Signature: signature,
		SignedAt:  signedAt,
		ExpiresAt: expiresAt,
		Scheme:    SchemeHMACSHA256,
	}, nil
}

// VerifyEvent validates the cryptographic signature of a signed event
func VerifyEvent(signedEvent *SignedEvent, config *SignatureConfig) error {
	return verifyEventAt(signedEvent, config, time.Now())
}

func verifyEventAt(signedEvent *SignedEvent, config *SignatureConfig, now time.Time) error {
	if config == nil {
		config = DefaultSignatureConfig()
	}

	// No key material is logged here, for the same reason it is not logged in
	// SignEvent: printing the first eight characters and the exact length on
	// every verification leaks key material into any log aggregator, and since
	// the development default is a constant in this repository it also
	// announces which key is in use. The signing side was fixed earlier; this
	// half was missed.

	if signedEvent == nil || signedEvent.Event == nil {
		return fmt.Errorf("signed event has no event")
	}
	if signedEvent.Scheme != "" && signedEvent.Scheme != SchemeHMACSHA256 {
		return fmt.Errorf("signature scheme %q is not %s", signedEvent.Scheme, SchemeHMACSHA256)
	}

	// Check if signature has expired: the verifier's own window when it has
	// one, the message's ExpiresAt otherwise.
	deadline := signedEvent.ExpiresAt
	if config.ExpiryWindow > 0 {
		deadline = signedEvent.SignedAt.Add(config.ExpiryWindow)
	}
	if now.After(deadline) {
		return fmt.Errorf("event signature has expired")
	}

	// Validate required claims
	if err := validateRequiredClaims(signedEvent.Event, config.RequiredClaims); err != nil {
		return fmt.Errorf("event validation failed: %w", err)
	}

	// Recreate canonical representation
	canonical, err := createCanonicalEventData(signedEvent.Event, signedEvent.SignedAt, signedEvent.ExpiresAt)
	if err != nil {
		return fmt.Errorf("failed to recreate canonical data: %w", err)
	}

	// Verify signature
	expectedSignature, err := generateSignature(canonical, config.SigningKey)
	if err != nil {
		return fmt.Errorf("failed to generate expected signature: %w", err)
	}

	if !hmac.Equal([]byte(signedEvent.Signature), []byte(expectedSignature)) {
		return fmt.Errorf("invalid signature: expected %.10s, got %.10s", expectedSignature, signedEvent.Signature)
	}

	return nil
}

// CanonicalSigningData is the byte sequence a signature covers: the event's
// id, type, source, timestamp, user id and data, and the signing and expiry
// times, as canonical JSON with sorted keys. HMAC signs it; another Signer
// signs the same bytes, so that what is covered does not depend on the
// scheme.
func CanonicalSigningData(event *BaseEvent, signedAt, expiresAt time.Time) ([]byte, error) {
	return createCanonicalEventData(event, signedAt, expiresAt)
}

// createCanonicalEventData creates a deterministic, canonical representation of event data for signing
func createCanonicalEventData(event *BaseEvent, signedAt, expiresAt time.Time) ([]byte, error) {
	// Normalize the event data by marshaling and unmarshaling to ensure consistent structure
	// This handles the case where data comes from different JSON serialization contexts
	normalizedData, err := normalizeDataStructure(event.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to normalize event data: %w", err)
	}

	// Create a deterministic representation using ordered map
	canonicalData := map[string]any{
		"data":       normalizedData,
		"expires_at": expiresAt.Unix(),
		"id":         event.ID,
		"signed_at":  signedAt.Unix(),
		"source":     event.Source,
		"timestamp":  event.Timestamp.Unix(),
		"type":       event.Type,
		"user_id":    event.UserID,
	}
	// A sealed payload is covered when there is one, so a consumer can
	// verify before it opens; an unsealed event's canonical bytes are what
	// they always were, so existing signatures still verify.
	if event.Sealed != nil {
		canonicalData["sealed"] = map[string]any{
			"alg":              event.Sealed.Algorithm,
			"ciphertext":       base64.StdEncoding.EncodeToString(event.Sealed.Ciphertext),
			"contract_version": event.Sealed.ContractVersion,
			"key_name":         event.Sealed.KeyName,
			"key_version":      event.Sealed.KeyVersion,
			"nonce":            base64.StdEncoding.EncodeToString(event.Sealed.Nonce),
			"wrapped_key":      base64.StdEncoding.EncodeToString(event.Sealed.WrappedKey),
		}
	}

	// Convert to canonical JSON with sorted keys
	canonical, err := marshalCanonicalJSON(canonicalData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal canonical data: %w", err)
	}

	return canonical, nil
}

// normalizeDataStructure ensures consistent data structure by doing a JSON roundtrip
func normalizeDataStructure(data any) (any, error) {
	if data == nil {
		return nil, nil
	}

	// Marshal to JSON
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	// Unmarshal back to any to normalize structure
	var normalized any
	if err := json.Unmarshal(jsonBytes, &normalized); err != nil {
		return nil, err
	}

	return normalized, nil
}

// marshalCanonicalJSON marshals data to JSON with sorted keys for deterministic output
func marshalCanonicalJSON(v any) ([]byte, error) {
	switch val := v.(type) {
	case map[string]any:
		// Sort keys and build JSON manually
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		var parts []string
		for _, k := range keys {
			keyJSON, err := json.Marshal(k)
			if err != nil {
				return nil, err
			}

			valueJSON, err := marshalCanonicalJSON(val[k])
			if err != nil {
				return nil, err
			}

			parts = append(parts, string(keyJSON)+":"+string(valueJSON))
		}

		return []byte("{" + strings.Join(parts, ",") + "}"), nil

	case []any:
		// Handle arrays
		var parts []string
		for _, item := range val {
			itemJSON, err := marshalCanonicalJSON(item)
			if err != nil {
				return nil, err
			}
			parts = append(parts, string(itemJSON))
		}
		return []byte("[" + strings.Join(parts, ",") + "]"), nil

	default:
		// For primitive types, use standard JSON marshaling
		return json.Marshal(val)
	}
}

// generateSignature creates an HMAC-SHA256 signature for the given data
func generateSignature(data []byte, key []byte) (string, error) {
	if len(key) < 16 {
		return "", fmt.Errorf("signing key must be at least 16 bytes long")
	}

	h := hmac.New(sha256.New, key)
	h.Write(data)
	signature := h.Sum(nil)

	return base64.StdEncoding.EncodeToString(signature), nil
}

// validateRequiredClaims ensures all required fields are present in the event
func validateRequiredClaims(event *BaseEvent, requiredClaims []string) error {
	for _, claim := range requiredClaims {
		switch claim {
		case "id":
			if strings.TrimSpace(event.ID) == "" {
				return fmt.Errorf("missing required claim: id")
			}
		case "type":
			if strings.TrimSpace(event.Type) == "" {
				return fmt.Errorf("missing required claim: type")
			}
		case "timestamp":
			if event.Timestamp.IsZero() {
				return fmt.Errorf("missing required claim: timestamp")
			}
		case "source":
			if strings.TrimSpace(event.Source) == "" {
				return fmt.Errorf("missing required claim: source")
			}
		default:
			return fmt.Errorf("unknown required claim: %s", claim)
		}
	}
	return nil
}

// EventSigner provides a convenient interface for signing events
type EventSigner struct {
	config *SignatureConfig
}

// NewEventSigner creates a new event signer with the given configuration
func NewEventSigner(config *SignatureConfig) *EventSigner {
	if config == nil {
		config = DefaultSignatureConfig()
	}
	return &EventSigner{config: config}
}

// Sign signs an event and returns a signed event
func (s *EventSigner) Sign(event *BaseEvent) (*SignedEvent, error) {
	return SignEvent(event, s.config)
}

// Verify verifies a signed event
func (s *EventSigner) Verify(signedEvent *SignedEvent) error {
	return VerifyEvent(signedEvent, s.config)
}

// EventVerifier provides a convenient interface for verifying events
type EventVerifier struct {
	config *SignatureConfig
}

// NewEventVerifier creates a new event verifier with the given configuration
func NewEventVerifier(config *SignatureConfig) *EventVerifier {
	if config == nil {
		config = DefaultSignatureConfig()
	}
	return &EventVerifier{config: config}
}

// Verify verifies a signed event
func (v *EventVerifier) Verify(signedEvent *SignedEvent) error {
	return VerifyEvent(signedEvent, v.config)
}

// RotateSigningKey provides a mechanism for key rotation (for future implementation)
func RotateSigningKey(oldConfig *SignatureConfig, newKey []byte) *SignatureConfig {
	return &SignatureConfig{
		SigningKey:     newKey,
		ExpiryWindow:   oldConfig.ExpiryWindow,
		RequiredClaims: oldConfig.RequiredClaims,
	}
}

// GetEventSignatureInfo extracts signature information for debugging/logging
func GetEventSignatureInfo(signedEvent *SignedEvent) map[string]any {
	return map[string]any{
		"has_signature":  len(signedEvent.Signature) > 0,
		"signed_at":      signedEvent.SignedAt,
		"expires_at":     signedEvent.ExpiresAt,
		"is_expired":     time.Now().After(signedEvent.ExpiresAt),
		"signature_hash": base64.StdEncoding.EncodeToString([]byte(signedEvent.Signature))[:8] + "...",
	}
}

// SecurityMetrics tracks security-related metrics for monitoring
type SecurityMetrics struct {
	SignedEventsPublished   int64
	UnsignedEventsPublished int64
	VerificationSuccesses   int64
	VerificationFailures    int64
	ExpiredSignatures       int64
	InvalidSignatures       int64
}

// Global security metrics (can be exported to Prometheus)
var GlobalSecurityMetrics = &SecurityMetrics{}
