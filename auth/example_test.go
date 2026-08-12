package auth_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/dobrevit/svckit/auth"
)

// The middleware authenticates a bearer token and publishes the caller on the
// request context, where handlers read it through a typed accessor.
func ExampleAuthenticate() {
	manager := auth.NewJWTManager("a-signing-secret", time.Hour)
	token, _ := manager.GenerateToken("u-1", []string{"editor"})

	handler := auth.Authenticate(manager)(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			id, _ := auth.UserID(r.Context())
			fmt.Println("authenticated:", id)
		}))

	r := httptest.NewRequest(http.MethodGet, "/orders", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(httptest.NewRecorder(), r)

	// Output: authenticated: u-1
}

// A service that authenticates against something other than this package's
// JWTs supplies its own IdentityProvider and keeps the same middleware.
func ExampleIdentityProviderFunc() {
	sessions := map[string]string{"session-abc": "u-9"}

	provider := auth.IdentityProviderFunc(
		func(_ context.Context, token string) (auth.Identity, error) {
			userID, ok := sessions[token]
			if !ok {
				return auth.Identity{}, errors.New("unknown session")
			}
			return auth.Identity{UserID: userID, Roles: []string{"viewer"}}, nil
		})

	handler := auth.Authenticate(provider)(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			id, _ := auth.UserID(r.Context())
			fmt.Println("authenticated:", id)
		}))

	r := httptest.NewRequest(http.MethodGet, "/orders", nil)
	r.Header.Set("Authorization", "Bearer session-abc")
	handler.ServeHTTP(httptest.NewRecorder(), r)

	// Output: authenticated: u-9
}
