// Package auth authenticates HTTP requests and carries the caller's identity
// on the request context.
//
// JWTManager issues and validates this package's own tokens, but the
// middleware is written against the IdentityProvider interface, so a service
// that authenticates against an IAM service or an opaque session store reuses
// the same middleware and helpers with its own resolver.
package auth
