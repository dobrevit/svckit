package middleware

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dobrevit/svckit/httpx"
)

// ServiceKeyValidator resolves a service API key to the calling service's
// identity, or reports an error when the key is not valid.
type ServiceKeyValidator interface {
	ValidateServiceKey(apiKey string) (*ServiceKeyInfo, error)
}

// ServiceKeyInfo describes a validated service key.
type ServiceKeyInfo struct {
	ServiceName string     `json:"service_name"`
	Scopes      []string   `json:"scopes"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
}

// APIKeyHeader is the primary header carrying a service API key. A key
// presented as a bearer token is accepted as well.
const APIKeyHeader = "X-API-Key"

// ServiceKeyFromRequest extracts a service API key from r, preferring the
// X-API-Key header and falling back to a bearer token.
func ServiceKeyFromRequest(r *http.Request) string {
	if key := r.Header.Get(APIKeyHeader); key != "" {
		return key
	}
	if token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
		return strings.TrimSpace(token)
	}
	return ""
}

// ServiceAuthRequired returns a middleware that rejects any request not
// carrying a valid service API key, and publishes the caller's identity on the
// request context for downstream handlers.
func ServiceAuthRequired(validator ServiceKeyValidator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			apiKey := ServiceKeyFromRequest(r)
			if apiKey == "" {
				httpx.WriteErrorCode(w, http.StatusUnauthorized, "Service API key is required", "MISSING_API_KEY")
				return
			}

			info, err := validator.ValidateServiceKey(apiKey)
			if err != nil {
				httpx.WriteErrorCode(w, http.StatusUnauthorized, "Invalid service API key", "INVALID_API_KEY")
				return
			}

			next.ServeHTTP(w, r.WithContext(WithServiceIdentity(r.Context(), info)))
		})
	}
}

// OptionalServiceAuth returns a middleware that validates a service API key
// when one is presented and lets unauthenticated requests through untouched. A
// key that is presented but invalid is still rejected.
func OptionalServiceAuth(validator ServiceKeyValidator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			apiKey := ServiceKeyFromRequest(r)
			if apiKey == "" {
				next.ServeHTTP(w, r)
				return
			}

			info, err := validator.ValidateServiceKey(apiKey)
			if err != nil {
				httpx.WriteErrorCode(w, http.StatusUnauthorized, "Invalid service API key", "INVALID_API_KEY")
				return
			}

			next.ServeHTTP(w, r.WithContext(WithServiceIdentity(r.Context(), info)))
		})
	}
}

// ServiceAuthOrUserAuth returns a middleware that authenticates a request
// carrying a service API key as a service, and otherwise delegates to
// userAuth.
func ServiceAuthOrUserAuth(validator ServiceKeyValidator, userAuth func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		userAuthenticated := userAuth(next)

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			apiKey := ServiceKeyFromRequest(r)
			if apiKey == "" {
				userAuthenticated.ServeHTTP(w, r.WithContext(WithAuthType(r.Context(), AuthTypeUser)))
				return
			}

			info, err := validator.ValidateServiceKey(apiKey)
			if err != nil {
				httpx.WriteErrorCode(w, http.StatusUnauthorized, "Invalid service API key", "INVALID_API_KEY")
				return
			}

			next.ServeHTTP(w, r.WithContext(WithServiceIdentity(r.Context(), info)))
		})
	}
}

// RequireServiceScope returns a middleware that rejects an authenticated
// service lacking requiredScope. It must run after ServiceAuthRequired.
func RequireServiceScope(requiredScope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info, ok := ServiceIdentity(r.Context())
			if !ok {
				httpx.WriteErrorCode(w, http.StatusForbidden, "Service scopes not found", "NO_SERVICE_SCOPES")
				return
			}

			if !HasScope(r.Context(), requiredScope) {
				httpx.WriteErrorCode(w, http.StatusForbidden,
					fmt.Sprintf("Service '%s' does not have required scope '%s'", info.ServiceName, requiredScope),
					"INSUFFICIENT_SCOPE")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
