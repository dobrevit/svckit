package auth

import (
	"context"
	"net/http"
	"slices"
	"strings"

	"github.com/dobrevit/svckit/httpx"
)

// Identity is the authenticated caller a middleware publishes on the request
// context.
type Identity struct {
	UserID string
	Roles  []string
}

// HasRole reports whether the identity holds role.
func (i Identity) HasRole(role string) bool { return slices.Contains(i.Roles, role) }

// IdentityProvider turns a bearer token into an Identity, or reports why it
// could not. Implementing it lets a service authenticate against something
// other than this package's JWTs — an IAM service, an opaque session store —
// while reusing the middleware and helpers here.
type IdentityProvider interface {
	Identify(ctx context.Context, token string) (Identity, error)
}

// IdentityProviderFunc adapts a function to IdentityProvider.
type IdentityProviderFunc func(ctx context.Context, token string) (Identity, error)

// Identify calls f.
func (f IdentityProviderFunc) Identify(ctx context.Context, token string) (Identity, error) {
	return f(ctx, token)
}

// Identify implements IdentityProvider over the manager's own JWTs.
func (j *JWTManager) Identify(_ context.Context, token string) (Identity, error) {
	claims, err := j.ValidateToken(token)
	if err != nil {
		return Identity{}, err
	}
	return Identity{UserID: claims.UserID, Roles: claims.Roles}, nil
}

// AdminRole is the role that satisfies every RequireRole check.
const AdminRole = "admin"

// BearerToken returns the token from r's Authorization header, and whether
// the header carried a bearer token at all.
func BearerToken(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", false
	}
	token, ok := strings.CutPrefix(header, "Bearer ")
	if !ok {
		return "", false
	}
	return strings.TrimSpace(token), true
}

// Authenticate returns a middleware that resolves the request's bearer token
// through provider and publishes the resulting Identity on the request
// context. Requests without a usable token, or whose token the provider
// rejects, are answered 401 and never reach the handler.
func Authenticate(provider IdentityProvider) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			header := r.Header.Get("Authorization")
			if header == "" {
				httpx.WriteError(w, http.StatusUnauthorized, "Authorization header required")
				return
			}

			token, ok := BearerToken(r)
			if !ok {
				httpx.WriteError(w, http.StatusUnauthorized, "Invalid authorization format")
				return
			}

			identity, err := provider.Identify(r.Context(), token)
			if err != nil {
				httpx.WriteError(w, http.StatusUnauthorized, "Invalid token")
				return
			}

			next.ServeHTTP(w, r.WithContext(WithIdentity(r.Context(), identity)))
		})
	}
}

// RequireRole returns a middleware that rejects an authenticated caller
// lacking role. AdminRole satisfies every check. It must run after
// Authenticate.
func RequireRole(role string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			identity, ok := IdentityFromContext(r.Context())
			if !ok {
				httpx.WriteError(w, http.StatusForbidden, "No roles found")
				return
			}

			if !identity.HasRole(AdminRole) && !identity.HasRole(role) {
				httpx.WriteError(w, http.StatusForbidden, "Insufficient permissions")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// ctxKey is the unexported key type for the identity this package attaches to
// a request context.
type ctxKey int

const keyIdentity ctxKey = iota

// WithIdentity returns a context carrying identity.
func WithIdentity(ctx context.Context, identity Identity) context.Context {
	return context.WithValue(ctx, keyIdentity, identity)
}

// IdentityFromContext returns the identity carried by ctx, if any.
func IdentityFromContext(ctx context.Context) (Identity, bool) {
	identity, ok := ctx.Value(keyIdentity).(Identity)
	return identity, ok && identity.UserID != ""
}

// UserID returns the authenticated user's ID carried by ctx.
func UserID(ctx context.Context) (string, bool) {
	identity, ok := IdentityFromContext(ctx)
	return identity.UserID, ok
}

// Roles returns the authenticated user's roles carried by ctx.
func Roles(ctx context.Context) ([]string, bool) {
	identity, ok := IdentityFromContext(ctx)
	return identity.Roles, ok
}

// ContextKey is the legacy string key type used by SetUserContext.
//
// Deprecated: use WithIdentity and IdentityFromContext, which use an
// unexported key type that cannot collide with keys set by other packages.
type ContextKey string

// Legacy context keys.
//
// Deprecated: see ContextKey.
const (
	UserIDContextKey ContextKey = "user_id"
	RolesContextKey  ContextKey = "roles"
)

// SetUserContext stores the user under both the current and the legacy keys.
//
// Deprecated: use WithIdentity.
func SetUserContext(ctx context.Context, userID string, roles []string) context.Context {
	ctx = WithIdentity(ctx, Identity{UserID: userID, Roles: roles})
	ctx = context.WithValue(ctx, UserIDContextKey, userID)
	return context.WithValue(ctx, RolesContextKey, roles)
}

// GetUserFromContext reads the user stored by SetUserContext.
//
// Deprecated: use IdentityFromContext.
func GetUserFromContext(ctx context.Context) (string, []string, bool) {
	if identity, ok := IdentityFromContext(ctx); ok {
		return identity.UserID, identity.Roles, true
	}

	userID, ok := ctx.Value(UserIDContextKey).(string)
	if !ok {
		return "", nil, false
	}
	roles, ok := ctx.Value(RolesContextKey).([]string)
	if !ok {
		return "", nil, false
	}
	return userID, roles, true
}
