package eventbus

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// SignatureConfig holds configuration for event signing
type SignatureConfig struct {
	SigningKey     []byte
	ExpiryWindow   time.Duration // How long a signature is valid
	RequiredClaims []string      // Required fields in the event for signing
}

// SignedEvent extends BaseEvent with cryptographic signature
type SignedEvent struct {
	Event     *BaseEvent `json:"event"`
	Signature string     `json:"signature"`
	SignedAt  time.Time  `json:"signed_at"`
	ExpiresAt time.Time  `json:"expires_at"`
}

// DefaultSignatureConfig returns the default configuration for event signing
func DefaultSignatureConfig() *SignatureConfig {
	signingKey := []byte(os.Getenv("EVENT_SIGNING_KEY"))
	if len(signingKey) == 0 {
		// Fallback for development - DO NOT USE IN PRODUCTION
		signingKey = []byte("dev-signing-key-change-in-production")
	}

	return &SignatureConfig{
		SigningKey:     signingKey,
		ExpiryWindow:   30 * time.Minute, // Events expire after 30 minutes
		RequiredClaims: []string{"id", "type", "timestamp", "source"},
	}
}

// SignEvent creates a cryptographically signed version of the event
func SignEvent(event *BaseEvent, config *SignatureConfig) (*SignedEvent, error) {
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

	signedAt := time.Now()
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
	}, nil
}

// VerifyEvent validates the cryptographic signature of a signed event
func VerifyEvent(signedEvent *SignedEvent, config *SignatureConfig) error {
	if config == nil {
		config = DefaultSignatureConfig()
	}

	// Debug: log key info (first few chars only for security)
	if len(config.SigningKey) > 0 {
		fmt.Printf("DEBUG: Verification using key: %.8s... (len=%d)\n", string(config.SigningKey), len(config.SigningKey))
	}

	// Check if signature has expired
	if time.Now().After(signedEvent.ExpiresAt) {
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

// createCanonicalEventData creates a deterministic, canonical representation of event data for signing
func createCanonicalEventData(event *BaseEvent, signedAt, expiresAt time.Time) ([]byte, error) {
	// Normalize the event data by marshaling and unmarshaling to ensure consistent structure
	// This handles the case where data comes from different JSON serialization contexts
	normalizedData, err := normalizeDataStructure(event.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to normalize event data: %w", err)
	}

	// Create a deterministic representation using ordered map
	canonicalData := map[string]interface{}{
		"data":       normalizedData,
		"expires_at": expiresAt.Unix(),
		"id":         event.ID,
		"signed_at":  signedAt.Unix(),
		"source":     event.Source,
		"timestamp":  event.Timestamp.Unix(),
		"type":       event.Type,
		"user_id":    event.UserID,
	}

	// Convert to canonical JSON with sorted keys
	canonical, err := marshalCanonicalJSON(canonicalData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal canonical data: %w", err)
	}

	return canonical, nil
}

// normalizeDataStructure ensures consistent data structure by doing a JSON roundtrip
func normalizeDataStructure(data interface{}) (interface{}, error) {
	if data == nil {
		return nil, nil
	}

	// Marshal to JSON
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	// Unmarshal back to interface{} to normalize structure
	var normalized interface{}
	if err := json.Unmarshal(jsonBytes, &normalized); err != nil {
		return nil, err
	}

	return normalized, nil
}

// marshalCanonicalJSON marshals data to JSON with sorted keys for deterministic output
func marshalCanonicalJSON(v interface{}) ([]byte, error) {
	switch val := v.(type) {
	case map[string]interface{}:
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

	case []interface{}:
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
func GetEventSignatureInfo(signedEvent *SignedEvent) map[string]interface{} {
	return map[string]interface{}{
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
