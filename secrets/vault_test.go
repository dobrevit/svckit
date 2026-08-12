package secrets

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// fakeVault is a minimal KV v2 endpoint: stores values per request path.
func fakeVault(t *testing.T, secrets map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Vault-Token") != "test-token" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		switch r.Method {
		case http.MethodGet:
			value, ok := secrets[r.URL.Path]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{"data": map[string]string{"value": value}},
			})
		case http.MethodPost, http.MethodPut:
			var payload struct {
				Data map[string]string `json:"data"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			secrets[r.URL.Path] = payload.Data["value"]
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
}

func TestVaultStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	backing := map[string]string{}
	server := fakeVault(t, backing)
	defer server.Close()

	store := NewVaultStore(server.URL, "test-token", "", "")

	if _, err := store.Get(ctx, "service-keys/billing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get on empty vault = %v; want ErrNotFound", err)
	}
	if ok, err := store.Exists(ctx, "service-keys/billing"); err != nil || ok {
		t.Fatalf("Exists = %v, %v; want false", ok, err)
	}

	if err := store.Set(ctx, "service-keys/billing", "sk_seeded"); err != nil {
		t.Fatal(err)
	}
	if _, ok := backing["/v1/secret/data/app/service-keys/billing"]; !ok {
		t.Fatalf("Set wrote to unexpected path; got %v", backing)
	}
	got, err := store.Get(ctx, "service-keys/billing")
	if err != nil || got != "sk_seeded" {
		t.Fatalf("Get = %q, %v; want sk_seeded", got, err)
	}
}

func TestVaultStoreErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("bad token is terminal ErrAccessDenied", func(t *testing.T) {
		server := fakeVault(t, map[string]string{})
		defer server.Close()
		store := NewVaultStore(server.URL, "wrong-token", "", "")
		_, err := store.Get(ctx, "p")
		if !errors.Is(err, ErrAccessDenied) {
			t.Fatalf("want ErrAccessDenied, got %v", err)
		}
		if errors.Is(err, ErrNotFound) || errors.Is(err, ErrUnavailable) {
			t.Fatalf("access denial must not be retryable: %v", err)
		}
	})

	t.Run("5xx is ErrUnavailable", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer server.Close()
		store := NewVaultStore(server.URL, "t", "", "")
		if _, err := store.Get(ctx, "p"); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("want ErrUnavailable, got %v", err)
		}
	})

	t.Run("connection refused is ErrUnavailable", func(t *testing.T) {
		store := NewVaultStore("http://127.0.0.1:1", "t", "", "")
		if _, err := store.Get(ctx, "p"); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("want ErrUnavailable, got %v", err)
		}
	})
}

// flakyStore fails with the given error until succeedAfter calls have been made.
type flakyStore struct {
	fails        int32
	succeedAfter int32
	err          error
	value        string
}

func (f *flakyStore) Get(context.Context, string) (string, error) {
	if atomic.AddInt32(&f.fails, 1) <= f.succeedAfter {
		return "", f.err
	}
	return f.value, nil
}
func (f *flakyStore) Set(context.Context, string, string) error { return nil }
func (f *flakyStore) Exists(context.Context, string) (bool, error) {
	return false, nil
}

func TestRetrying(t *testing.T) {
	t.Run("retries transient errors until success", func(t *testing.T) {
		inner := &flakyStore{succeedAfter: 3, err: ErrUnavailable, value: "v"}
		store := Retrying(inner, time.Millisecond, 4*time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		got, err := store.Get(ctx, "p")
		if err != nil || got != "v" {
			t.Fatalf("Get = %q, %v", got, err)
		}
	})

	t.Run("retries ErrNotFound until seeded", func(t *testing.T) {
		inner := &flakyStore{succeedAfter: 2, err: ErrNotFound, value: "v"}
		store := Retrying(inner, time.Millisecond, 4*time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := store.Get(ctx, "p"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("terminal errors stop immediately", func(t *testing.T) {
		boom := errors.New("permission denied")
		inner := &flakyStore{succeedAfter: 100, err: boom}
		store := Retrying(inner, time.Millisecond, time.Millisecond)
		_, err := store.Get(context.Background(), "p")
		if !errors.Is(err, boom) {
			t.Fatalf("want terminal error, got %v", err)
		}
		if inner.fails != 1 {
			t.Fatalf("attempts = %d, want 1", inner.fails)
		}
	})

	t.Run("gives up when ctx expires", func(t *testing.T) {
		inner := &flakyStore{succeedAfter: 1 << 30, err: ErrNotFound}
		store := Retrying(inner, time.Millisecond, time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		_, err := store.Get(ctx, "p")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want DeadlineExceeded, got %v", err)
		}
	})
}

func TestNewFromEnvVaultBackend(t *testing.T) {
	ctx := context.Background()
	backing := map[string]string{}
	server := fakeVault(t, backing)
	defer server.Close()

	t.Setenv("SECRETS_BACKEND", "vault")
	t.Setenv("VAULT_ADDR", server.URL)
	t.Setenv("VAULT_TOKEN", "test-token")

	t.Run("resolves from vault", func(t *testing.T) {
		backing["/v1/secret/data/app/service-keys/billing"] = "sk_vault"
		store, err := NewFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		got, err := store.Get(ctx, ServiceKeyPath("billing"))
		if err != nil || got != "sk_vault" {
			t.Fatalf("Get = %q, %v", got, err)
		}
	})

	t.Run("falls back to env while vault is unseeded", func(t *testing.T) {
		t.Setenv("SERVICE_API_KEY", "sk_from_dotenv")
		store, err := NewFromEnv(WithAlias(ServiceKeyPath("pii-service"), "SERVICE_API_KEY"))
		if err != nil {
			t.Fatal(err)
		}
		got, err := store.Get(ctx, ServiceKeyPath("pii-service"))
		if err != nil || got != "sk_from_dotenv" {
			t.Fatalf("Get = %q, %v", got, err)
		}
	})

	t.Run("missing VAULT_TOKEN errors", func(t *testing.T) {
		t.Setenv("VAULT_TOKEN", "")
		if _, err := NewFromEnv(); err == nil {
			t.Fatal("want error without VAULT_TOKEN")
		}
	})
}
