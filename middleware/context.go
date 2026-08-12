package middleware

import (
	"context"
	"net"
	"net/http"
	"slices"
	"strings"
)

// ctxKey is the unexported key type for values this package attaches to a
// request context. Using a private type keeps the keys from colliding with
// values written by other packages.
type ctxKey int

const (
	keyServiceIdentity ctxKey = iota
	keyUserID
	keyAuthType
	keyRoute
)

// AuthType names how a request was authenticated.
type AuthType string

const (
	// AuthTypeService marks a request authenticated by a service API key.
	AuthTypeService AuthType = "service"
	// AuthTypeUser marks a request authenticated as an end user.
	AuthTypeUser AuthType = "user"
)

// WithServiceIdentity returns a context carrying the validated service key.
func WithServiceIdentity(ctx context.Context, info *ServiceKeyInfo) context.Context {
	ctx = context.WithValue(ctx, keyServiceIdentity, info)
	return context.WithValue(ctx, keyAuthType, AuthTypeService)
}

// ServiceIdentity returns the validated service key carried by ctx, if any.
func ServiceIdentity(ctx context.Context) (*ServiceKeyInfo, bool) {
	info, ok := ctx.Value(keyServiceIdentity).(*ServiceKeyInfo)
	return info, ok && info != nil
}

// ServiceName returns the name of the authenticated calling service.
func ServiceName(ctx context.Context) (string, bool) {
	info, ok := ServiceIdentity(ctx)
	if !ok || info.ServiceName == "" {
		return "", false
	}
	return info.ServiceName, true
}

// HasScope reports whether the authenticated service holds scope. The wildcard
// scope "all" satisfies every check.
func HasScope(ctx context.Context, scope string) bool {
	info, ok := ServiceIdentity(ctx)
	if !ok {
		return false
	}
	return slices.Contains(info.Scopes, scope) || slices.Contains(info.Scopes, ScopeAll)
}

// ScopeAll is the wildcard scope that satisfies any RequireScope check.
const ScopeAll = "all"

// IsServiceAuthenticated reports whether ctx was authenticated as a service.
func IsServiceAuthenticated(ctx context.Context) bool {
	if at, ok := ctx.Value(keyAuthType).(AuthType); ok {
		return at == AuthTypeService
	}
	_, ok := ServiceIdentity(ctx)
	return ok
}

// WithAuthType records how the request was authenticated.
func WithAuthType(ctx context.Context, at AuthType) context.Context {
	return context.WithValue(ctx, keyAuthType, at)
}

// WithUserID returns a context carrying the authenticated end user's ID.
func WithUserID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, keyUserID, id)
}

// UserID returns the authenticated end user's ID carried by ctx, if any.
func UserID(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(keyUserID).(string)
	return id, ok && id != ""
}

// WithRoute returns a context carrying the matched route pattern — the
// templated form such as "/api/v1/users/{id}", not the concrete path.
//
// Routers know their own patterns and the stdlib request does not carry them,
// so the router adapter is responsible for recording one. Metrics and logging
// use it to keep label cardinality bounded; without it they fall back to the
// request path, which for ID-bearing routes means one label value per ID.
func WithRoute(ctx context.Context, pattern string) context.Context {
	return context.WithValue(ctx, keyRoute, pattern)
}

// Route returns the matched route pattern recorded by the router adapter,
// falling back to the request's path when none was recorded.
func Route(r *http.Request) string {
	if pattern, ok := r.Context().Value(keyRoute).(string); ok && pattern != "" {
		return pattern
	}
	return r.URL.Path
}

// ClientIP returns the originating client address, preferring the left-most
// entry of X-Forwarded-For, then X-Real-IP, then the transport's remote
// address.
//
// Both headers are trivially forgeable by a direct caller, so this is only
// trustworthy behind a proxy that overwrites them. Callers exposed directly to
// the internet should not treat the result as an identity.
func ClientIP(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		if first, _, found := strings.Cut(fwd, ","); found {
			fwd = first
		}
		if ip := strings.TrimSpace(fwd); ip != "" {
			return ip
		}
	}
	if ip := strings.TrimSpace(r.Header.Get("X-Real-IP")); ip != "" {
		return ip
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}
