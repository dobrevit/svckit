//go:build integration

package vaultx

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	"github.com/dobrevit/svckit/eventbus/seal"
	"github.com/dobrevit/svckit/middleware"
	"github.com/dobrevit/svckit/testkit/containers"
)

// testVault starts a Vault with Transit, a key named "members", AppRole,
// and two roles: "sealer", which may only make data keys, and "opener",
// which may only decrypt them.
func testVault(t *testing.T) (*containers.TestContainers, string) {
	t.Helper()
	ctx := context.Background()
	tc, err := containers.NewTestContainersWithConfig(ctx, &containers.TestContainerConfig{Vault: true})
	if err != nil {
		t.Fatalf("start Vault: %v", err)
	}
	t.Cleanup(func() { _ = tc.Close() })
	addr, err := tc.GetVaultAddress()
	if err != nil {
		t.Fatal(err)
	}

	must := func(method, path string, body any) map[string]any {
		t.Helper()
		data, err := tc.VaultRequest(ctx, method, path, body)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		return data
	}
	if _, err := tc.EnableTransit(ctx, ""); err != nil {
		t.Fatal(err)
	}
	must(http.MethodPost, "transit/keys/members", map[string]any{})
	must(http.MethodPost, "sys/auth/approle", map[string]any{"type": "approle"})
	must(http.MethodPut, "sys/policies/acl/sealer", map[string]any{"policy": `path "transit/datakey/plaintext/members" { capabilities = ["update"] }`})
	must(http.MethodPut, "sys/policies/acl/opener", map[string]any{"policy": `path "transit/decrypt/members" { capabilities = ["update"] }`})
	return tc, addr
}

// appRole creates an AppRole role with the given policies and token
// lifetimes and returns its login.
func appRole(t *testing.T, tc *containers.TestContainers, name string, policies []string, ttl, maxTTL string) *AppRole {
	t.Helper()
	ctx := context.Background()
	if _, err := tc.VaultRequest(ctx, http.MethodPost, "auth/approle/role/"+name, map[string]any{
		"token_policies": policies, "token_ttl": ttl, "token_max_ttl": maxTTL,
	}); err != nil {
		t.Fatal(err)
	}
	roleID, err := tc.VaultRequest(ctx, http.MethodGet, "auth/approle/role/"+name+"/role-id", nil)
	if err != nil {
		t.Fatal(err)
	}
	secretID, err := tc.VaultRequest(ctx, http.MethodPost, "auth/approle/role/"+name+"/secret-id", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	return &AppRole{RoleID: roleID["role_id"].(string), SecretID: secretID["secret_id"].(string)}
}

func client(t *testing.T, addr string, role *AppRole) *Client {
	t.Helper()
	c, err := New(context.Background(), Config{Address: addr, AppRole: role})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func member() *eventbus.BaseEvent {
	return eventbus.NewEvent("member.joined", "members", map[string]any{"name": "Jane Example"})
}

func TestTransitWrapper(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	tc, addr := testVault(t)
	ctx := context.Background()

	sealerClient := client(t, addr, appRole(t, tc, "sealer", []string{"sealer"}, "1h", "2h"))
	openerClient := client(t, addr, appRole(t, tc, "opener", []string{"opener"}, "1h", "2h"))
	sealer := seal.NewSealer(NewTransitWrapper(sealerClient, ""), "members", "member.v1")
	opener := seal.NewOpener(NewTransitWrapper(openerClient, ""))

	t.Run("a publisher's identity seals but cannot decrypt", func(t *testing.T) {
		e := member()
		if err := sealer.Seal(ctx, e); err != nil {
			t.Fatalf("Seal: %v", err)
		}
		if e.Sealed.KeyVersion != "1" {
			t.Errorf("KeyVersion = %q, want 1", e.Sealed.KeyVersion)
		}
		_, err := NewTransitWrapper(sealerClient, "").Unwrap(ctx, "members", e.Sealed.WrappedKey)
		if !errors.Is(err, ErrForbidden) {
			t.Errorf("Unwrap with the sealer's identity = %v, want ErrForbidden", err)
		}
	})

	t.Run("a consumer's identity opens but cannot make keys", func(t *testing.T) {
		e := member()
		if err := sealer.Seal(ctx, e); err != nil {
			t.Fatal(err)
		}
		data, err := opener.Open(ctx, e)
		if err != nil || data["name"] != "Jane Example" {
			t.Errorf("Open = %v, %v", data, err)
		}
		if _, _, _, err := NewTransitWrapper(openerClient, "").NewDataKey(ctx, "members"); !errors.Is(err, ErrForbidden) {
			t.Errorf("NewDataKey with the opener's identity = %v, want ErrForbidden", err)
		}
	})

	t.Run("after a rotation old envelopes open until the minimum version rises", func(t *testing.T) {
		old := member()
		if err := sealer.Seal(ctx, old); err != nil {
			t.Fatal(err)
		}
		if _, err := tc.VaultRequest(ctx, http.MethodPost, "transit/keys/members/rotate", map[string]any{}); err != nil {
			t.Fatal(err)
		}
		fresh := member()
		if err := sealer.Seal(ctx, fresh); err != nil {
			t.Fatal(err)
		}
		if fresh.Sealed.KeyVersion != "2" {
			t.Errorf("after the rotation KeyVersion = %q, want 2", fresh.Sealed.KeyVersion)
		}
		if _, err := opener.Open(ctx, old); err != nil {
			t.Errorf("an envelope from before the rotation did not open: %v", err)
		}

		if _, err := tc.VaultRequest(ctx, http.MethodPost, "transit/keys/members/config", map[string]any{"min_decryption_version": 2}); err != nil {
			t.Fatal(err)
		}
		if _, err := opener.Open(ctx, old); err == nil {
			t.Error("an envelope under version 1 opened after min_decryption_version became 2")
		}
		if _, err := opener.Open(ctx, fresh); err != nil {
			t.Errorf("a version-2 envelope did not open: %v", err)
		}
	})
}

// A token that expired used to be the end; the client renews it before it
// does, and logs in again once it reaches its maximum TTL.
func TestAppRoleTokenIsKeptAlive(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	tc, addr := testVault(t)
	c := client(t, addr, appRole(t, tc, "short", []string{"sealer"}, "3s", "7s"))
	wrapper := NewTransitWrapper(c, "")

	time.Sleep(10 * time.Second) // past several TTLs and past the maximum

	if _, _, _, err := wrapper.NewDataKey(context.Background(), "members"); err != nil {
		t.Fatalf("after ten seconds on a 3-second token: %v", err)
	}
	c.mu.RLock()
	renewals := c.renewals
	c.mu.RUnlock()
	if renewals == 0 {
		t.Error("the token was never renewed")
	}
}

func TestHealthCheckerReportsAStoppedVault(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	tc, addr := testVault(t)
	c, err := New(context.Background(), Config{Address: addr, Token: containers.VaultRootToken})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	check := NewHealthChecker(c, "")

	if _, status, msg := check.Check(); status != middleware.StatusHealthy {
		t.Fatalf("a running Vault reports %s: %s", status, msg)
	}

	timeout := 10 * time.Second
	if err := tc.VaultContainer.Stop(context.Background(), &timeout); err != nil {
		t.Fatal(err)
	}
	if name, status, msg := check.Check(); status != middleware.StatusUnhealthy || name != "vault" {
		t.Errorf("a stopped Vault reports %s %s: %s", name, status, msg)
	}
}
