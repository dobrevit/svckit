package auth_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dobrevit/svckit/auth"
)

func okHandler(ran *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*ran = true
		w.WriteHeader(http.StatusOK)
	})
}

func TestJWTRoundTrip(t *testing.T) {
	manager := auth.NewJWTManager("secret", time.Hour)

	token, err := manager.GenerateToken("u-1", []string{"admin", "auditor"})
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}

	claims, err := manager.ValidateToken(token)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if claims.UserID != "u-1" {
		t.Errorf("UserID = %q, want u-1", claims.UserID)
	}
	if !claims.HasRole("auditor") || claims.HasRole("nope") {
		t.Errorf("roles = %v", claims.Roles)
	}
}

func TestValidateTokenRejectsAnotherSigningKey(t *testing.T) {
	issuer := auth.NewJWTManager("secret", time.Hour)
	verifier := auth.NewJWTManager("a-different-secret", time.Hour)

	token, err := issuer.GenerateToken("u-1", nil)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}

	if _, err := verifier.ValidateToken(token); err == nil {
		t.Fatal("a token signed with another key was accepted")
	}
}

func TestValidateTokenRejectsAnExpiredToken(t *testing.T) {
	manager := auth.NewJWTManager("secret", -time.Minute)

	token, err := manager.GenerateToken("u-1", nil)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}

	if _, err := manager.ValidateToken(token); err == nil {
		t.Fatal("an expired token was accepted")
	}
}

func TestAuthenticate(t *testing.T) {
	manager := auth.NewJWTManager("secret", time.Hour)
	token, err := manager.GenerateToken("u-1", []string{"editor"})
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}

	cases := []struct {
		name       string
		authHeader string
		wantStatus int
		wantRan    bool
	}{
		{"no header", "", http.StatusUnauthorized, false},
		{"not a bearer token", token, http.StatusUnauthorized, false},
		{"unparseable token", "Bearer not-a-token", http.StatusUnauthorized, false},
		{"valid token", "Bearer " + token, http.StatusOK, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ran bool
			h := auth.Authenticate(manager)(okHandler(&ran))

			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/x", nil)
			if tc.authHeader != "" {
				r.Header.Set("Authorization", tc.authHeader)
			}
			h.ServeHTTP(w, r)

			if w.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", w.Code, tc.wantStatus)
			}
			if ran != tc.wantRan {
				t.Errorf("handler ran = %v, want %v", ran, tc.wantRan)
			}
		})
	}
}

func TestAuthenticatePublishesIdentity(t *testing.T) {
	manager := auth.NewJWTManager("secret", time.Hour)
	token, _ := manager.GenerateToken("u-1", []string{"editor"})

	var got auth.Identity
	h := auth.Authenticate(manager)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = auth.IdentityFromContext(r.Context())
	}))

	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	h.ServeHTTP(httptest.NewRecorder(), r)

	if got.UserID != "u-1" || !got.HasRole("editor") {
		t.Errorf("identity = %+v, want u-1 with role editor", got)
	}
}

// A custom provider is the seam that lets a service authenticate against
// something other than this package's JWTs.
func TestAuthenticateWithACustomProvider(t *testing.T) {
	provider := auth.IdentityProviderFunc(func(_ context.Context, token string) (auth.Identity, error) {
		if token != "session-abc" {
			return auth.Identity{}, errors.New("unknown session")
		}
		return auth.Identity{UserID: "u-9", Roles: []string{"viewer"}}, nil
	})

	var seen string
	h := auth.Authenticate(provider)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen, _ = auth.UserID(r.Context())
	}))

	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	r.Header.Set("Authorization", "Bearer session-abc")
	h.ServeHTTP(httptest.NewRecorder(), r)

	if seen != "u-9" {
		t.Errorf("UserID = %q, want u-9", seen)
	}
}

func TestRequireRole(t *testing.T) {
	cases := []struct {
		name    string
		roles   []string
		require string
		wantRan bool
	}{
		{"exact role passes", []string{"editor"}, "editor", true},
		{"admin passes anything", []string{auth.AdminRole}, "editor", true},
		{"missing role is rejected", []string{"viewer"}, "editor", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ran bool
			h := auth.RequireRole(tc.require)(okHandler(&ran))

			r := httptest.NewRequest(http.MethodGet, "/x", nil)
			r = r.WithContext(auth.WithIdentity(r.Context(),
				auth.Identity{UserID: "u-1", Roles: tc.roles}))

			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)

			if ran != tc.wantRan {
				t.Errorf("handler ran = %v, want %v (status %d)", ran, tc.wantRan, w.Code)
			}
		})
	}
}

func TestRequireRoleRejectsAnUnauthenticatedRequest(t *testing.T) {
	var ran bool
	w := httptest.NewRecorder()
	auth.RequireRole("editor")(okHandler(&ran)).
		ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))

	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", w.Code)
	}
	if ran {
		t.Error("handler ran for an unauthenticated request")
	}
}

func TestDeprecatedContextHelpersInteroperateWithIdentity(t *testing.T) {
	ctx := auth.SetUserContext(context.Background(), "u-1", []string{"editor"})

	userID, roles, ok := auth.GetUserFromContext(ctx)
	if !ok || userID != "u-1" || len(roles) != 1 {
		t.Fatalf("GetUserFromContext = (%q, %v, %v)", userID, roles, ok)
	}

	// The same context must satisfy the current accessor, so a handler reading
	// either way sees the same user.
	identity, ok := auth.IdentityFromContext(ctx)
	if !ok || identity.UserID != "u-1" {
		t.Errorf("IdentityFromContext = (%+v, %v)", identity, ok)
	}
}
