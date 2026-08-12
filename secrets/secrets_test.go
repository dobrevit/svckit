package secrets

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestEnvVarName(t *testing.T) {
	cases := map[string]string{
		"service-keys/billing":  "SECRET_SERVICE_KEYS_BILLING",
		"kms/master-key":        "SECRET_KMS_MASTER_KEY",
		"kms/event-signing-key": "SECRET_KMS_EVENT_SIGNING_KEY",
	}
	for path, want := range cases {
		if got := EnvVarName(path); got != want {
			t.Errorf("EnvVarName(%q) = %q, want %q", path, got, want)
		}
	}
}

func TestEnvStoreGet(t *testing.T) {
	ctx := context.Background()
	path := ServiceKeyPath("billing")

	t.Run("generic transform", func(t *testing.T) {
		t.Setenv("SECRET_SERVICE_KEYS_BILLING", "sk_generic")
		got, err := NewEnvStore().Get(ctx, path)
		if err != nil || got != "sk_generic" {
			t.Fatalf("Get = %q, %v; want sk_generic", got, err)
		}
	})

	t.Run("alias takes precedence", func(t *testing.T) {
		t.Setenv("SECRET_SERVICE_KEYS_BILLING", "sk_generic")
		t.Setenv("SERVICE_API_KEY", "sk_legacy")
		store := NewEnvStore(WithAlias(path, "SERVICE_API_KEY"))
		got, err := store.Get(ctx, path)
		if err != nil || got != "sk_legacy" {
			t.Fatalf("Get = %q, %v; want sk_legacy", got, err)
		}
	})

	t.Run("missing yields ErrNotFound", func(t *testing.T) {
		_, err := NewEnvStore().Get(ctx, "service-keys/no-such-service")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get error = %v; want ErrNotFound", err)
		}
	})

	t.Run("alias does not leak to other paths", func(t *testing.T) {
		t.Setenv("SERVICE_API_KEY", "sk_own")
		store := NewEnvStore(WithAlias(path, "SERVICE_API_KEY"))
		_, err := store.Get(ctx, ServiceKeyPath("location-service"))
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound for foreign path, got %v", err)
		}
	})
}

func TestEnvStoreSetIsReadOnly(t *testing.T) {
	err := NewEnvStore().Set(context.Background(), "service-keys/x", "v")
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Set error = %v; want ErrReadOnly", err)
	}
}

func TestEnvStoreExists(t *testing.T) {
	ctx := context.Background()
	t.Setenv("SECRET_KMS_MASTER_KEY", "material")

	if ok, _ := NewEnvStore().Exists(ctx, "kms/master-key"); !ok {
		t.Error("Exists = false for a set variable")
	}
	if ok, _ := NewEnvStore().Exists(ctx, "kms/missing"); ok {
		t.Error("Exists = true for an unset variable")
	}
}

// countingStore records Get calls and serves canned values.
type countingStore struct {
	values map[string]string
	gets   int
	getErr error
}

func (s *countingStore) Get(_ context.Context, path string) (string, error) {
	s.gets++
	if s.getErr != nil {
		return "", s.getErr
	}
	if v, ok := s.values[path]; ok {
		return v, nil
	}
	return "", fmt.Errorf("%q: %w", path, ErrNotFound)
}

func (s *countingStore) Set(_ context.Context, path, value string) error {
	if s.values == nil {
		s.values = make(map[string]string)
	}
	s.values[path] = value
	return nil
}

func (s *countingStore) Exists(_ context.Context, path string) (bool, error) {
	_, ok := s.values[path]
	return ok, nil
}

func TestCachedGet(t *testing.T) {
	ctx := context.Background()
	inner := &countingStore{values: map[string]string{"p": "v1"}}
	store := Cached(inner, 50*time.Millisecond)

	for i := 0; i < 3; i++ {
		if got, err := store.Get(ctx, "p"); err != nil || got != "v1" {
			t.Fatalf("Get = %q, %v", got, err)
		}
	}
	if inner.gets != 1 {
		t.Fatalf("inner gets = %d, want 1 (cache hit)", inner.gets)
	}

	time.Sleep(60 * time.Millisecond)
	if _, err := store.Get(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	if inner.gets != 2 {
		t.Fatalf("inner gets = %d, want 2 (cache expired)", inner.gets)
	}
}

func TestCachedDoesNotCacheMisses(t *testing.T) {
	ctx := context.Background()
	inner := &countingStore{}
	store := Cached(inner, time.Minute)

	if _, err := store.Get(ctx, "absent"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	// The secret appears (seeder ran); the next Get must see it.
	inner.values = map[string]string{"absent": "now-present"}
	if got, err := store.Get(ctx, "absent"); err != nil || got != "now-present" {
		t.Fatalf("Get after seed = %q, %v", got, err)
	}
}

func TestCachedSetWritesThrough(t *testing.T) {
	ctx := context.Background()
	inner := &countingStore{values: map[string]string{}}
	store := Cached(inner, time.Minute)

	if err := store.Set(ctx, "p", "v2"); err != nil {
		t.Fatal(err)
	}
	if inner.values["p"] != "v2" {
		t.Fatal("Set did not reach the inner store")
	}
	got, err := store.Get(ctx, "p")
	if err != nil || got != "v2" {
		t.Fatalf("Get = %q, %v; want v2", got, err)
	}
	if inner.gets != 0 {
		t.Fatalf("inner gets = %d, want 0 (Set refreshed the cache)", inner.gets)
	}
}

func TestChain(t *testing.T) {
	ctx := context.Background()

	t.Run("falls through on ErrNotFound", func(t *testing.T) {
		primary := &countingStore{}
		fallback := &countingStore{values: map[string]string{"p": "from-fallback"}}
		got, err := Chain(primary, fallback).Get(ctx, "p")
		if err != nil || got != "from-fallback" {
			t.Fatalf("Get = %q, %v", got, err)
		}
	})

	t.Run("primary wins", func(t *testing.T) {
		primary := &countingStore{values: map[string]string{"p": "from-primary"}}
		fallback := &countingStore{values: map[string]string{"p": "from-fallback"}}
		got, _ := Chain(primary, fallback).Get(ctx, "p")
		if got != "from-primary" {
			t.Fatalf("Get = %q, want from-primary", got)
		}
	})

	t.Run("non-NotFound errors are terminal", func(t *testing.T) {
		boom := errors.New("backend unreachable")
		primary := &countingStore{getErr: boom}
		fallback := &countingStore{values: map[string]string{"p": "from-fallback"}}
		_, err := Chain(primary, fallback).Get(ctx, "p")
		if !errors.Is(err, boom) {
			t.Fatalf("want terminal error, got %v", err)
		}
		if fallback.gets != 0 {
			t.Fatal("fallback consulted despite terminal primary error")
		}
	})
}

func TestNewFromEnv(t *testing.T) {
	ctx := context.Background()

	t.Run("defaults to env backend", func(t *testing.T) {
		t.Setenv("SECRETS_BACKEND", "")
		t.Setenv("SERVICE_API_KEY", "sk_wired")
		store, err := NewFromEnv(WithAlias(ServiceKeyPath("billing"), "SERVICE_API_KEY"))
		if err != nil {
			t.Fatal(err)
		}
		got, err := store.Get(ctx, ServiceKeyPath("billing"))
		if err != nil || got != "sk_wired" {
			t.Fatalf("Get = %q, %v", got, err)
		}
	})

	t.Run("unimplemented backend errors", func(t *testing.T) {
		t.Setenv("SECRETS_BACKEND", "vault")
		if _, err := NewFromEnv(); err == nil {
			t.Fatal("want error for unimplemented backend")
		}
	})

	t.Run("unknown backend errors", func(t *testing.T) {
		t.Setenv("SECRETS_BACKEND", "consul")
		if _, err := NewFromEnv(); err == nil {
			t.Fatal("want error for unknown backend")
		}
	})

	t.Run("invalid TTL errors", func(t *testing.T) {
		t.Setenv("SECRETS_BACKEND", "env")
		t.Setenv("SECRETS_CACHE_TTL", "banana")
		if _, err := NewFromEnv(); err == nil {
			t.Fatal("want error for invalid TTL")
		}
	})
}
