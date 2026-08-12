package secrets

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeK8sAPI is a minimal Secrets endpoint storing data maps per secret name.
func fakeK8sAPI(t *testing.T, store map[string]map[string]string) *httptest.Server {
	t.Helper()
	const prefix = "/api/v1/namespaces/test-ns/secrets"
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if !strings.HasPrefix(r.URL.Path, prefix) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		name := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, prefix), "/")
		switch r.Method {
		case http.MethodGet:
			data, ok := store[name]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"data": data})
		case http.MethodPatch:
			data, ok := store[name]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			var patch struct {
				Data map[string]string `json:"data"`
			}
			body, _ := io.ReadAll(r.Body)
			json.Unmarshal(body, &patch)
			for k, v := range patch.Data {
				data[k] = v
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodPost:
			var manifest struct {
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
				Data map[string]string `json:"data"`
			}
			body, _ := io.ReadAll(r.Body)
			json.Unmarshal(body, &manifest)
			store[manifest.Metadata.Name] = manifest.Data
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
}

func k8sStoreFor(server *httptest.Server) *KubernetesStore {
	return &KubernetesStore{
		apiURL:    server.URL,
		namespace: "test-ns",
		token:     "test-token",
		client:    server.Client(),
	}
}

func TestKubernetesLocate(t *testing.T) {
	k := &KubernetesStore{}
	cases := []struct{ path, secret, key string }{
		{"service-keys/billing", "svc-key-billing", "api-key"},
		{"kms/master-key", "kms-material", "master-key"},
		{"kms/event-signing-key", "kms-material", "event-signing-key"},
		{"other/thing", "other-thing", "value"},
	}
	for _, c := range cases {
		secret, key := k.locate(c.path)
		if secret != c.secret || key != c.key {
			t.Errorf("locate(%q) = %q,%q; want %q,%q", c.path, secret, key, c.secret, c.key)
		}
	}
}

func TestKubernetesRoundTrip(t *testing.T) {
	ctx := context.Background()
	backing := map[string]map[string]string{}
	server := fakeK8sAPI(t, backing)
	defer server.Close()
	store := k8sStoreFor(server)

	if _, err := store.Get(ctx, "service-keys/pii-service"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get on empty cluster = %v; want ErrNotFound", err)
	}

	// Set creates the Secret when absent...
	if err := store.Set(ctx, "service-keys/pii-service", "sk_seeded"); err != nil {
		t.Fatal(err)
	}
	if got := backing["svc-key-pii-service"]["api-key"]; got != base64.StdEncoding.EncodeToString([]byte("sk_seeded")) {
		t.Fatalf("stored data = %q", got)
	}
	got, err := store.Get(ctx, "service-keys/pii-service")
	if err != nil || got != "sk_seeded" {
		t.Fatalf("Get = %q, %v", got, err)
	}

	// ...and merge-patches when present.
	if err := store.Set(ctx, "service-keys/pii-service", "sk_rotated"); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Get(ctx, "service-keys/pii-service"); got != "sk_rotated" {
		t.Fatalf("Get after rotation = %q", got)
	}
}

func TestKubernetesSharedSecretKeysPreserved(t *testing.T) {
	ctx := context.Background()
	backing := map[string]map[string]string{}
	server := fakeK8sAPI(t, backing)
	defer server.Close()
	store := k8sStoreFor(server)

	if err := store.Set(ctx, "kms/master-key", "material-a"); err != nil {
		t.Fatal(err)
	}
	if err := store.Set(ctx, "kms/event-signing-key", "material-b"); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Get(ctx, "kms/master-key"); got != "material-a" {
		t.Fatal("merge patch clobbered a sibling key in the shared secret")
	}
}

func TestKubernetesTombstone(t *testing.T) {
	ctx := context.Background()
	backing := map[string]map[string]string{}
	server := fakeK8sAPI(t, backing)
	defer server.Close()
	store := k8sStoreFor(server)

	if err := store.Set(ctx, "service-keys/pii-service", "sk_live"); err != nil {
		t.Fatal(err)
	}
	// Revocation writes an empty value; Get must report NotFound.
	if err := store.Set(ctx, "service-keys/pii-service", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "service-keys/pii-service"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get on tombstone = %v; want ErrNotFound", err)
	}
}

func TestKubernetesErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("forbidden is terminal ErrAccessDenied", func(t *testing.T) {
		server := fakeK8sAPI(t, map[string]map[string]string{})
		defer server.Close()
		store := k8sStoreFor(server)
		store.token = "wrong"
		_, err := store.Get(ctx, "service-keys/x")
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
		store := k8sStoreFor(server)
		if _, err := store.Get(ctx, "service-keys/x"); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("want ErrUnavailable, got %v", err)
		}
	})
}

func TestNewFromEnvKubernetesBackend(t *testing.T) {
	ctx := context.Background()
	backing := map[string]map[string]string{
		"svc-key-billing": {"api-key": base64.StdEncoding.EncodeToString([]byte("sk_k8s"))},
	}
	server := fakeK8sAPI(t, backing)
	defer server.Close()

	t.Setenv("SECRETS_BACKEND", "kubernetes")
	t.Setenv("KUBERNETES_API_URL", server.URL)
	t.Setenv("SECRETS_K8S_TOKEN", "test-token")
	t.Setenv("SECRETS_K8S_NAMESPACE", "test-ns")

	store, err := NewFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, ServiceKeyPath("billing"))
	if err != nil || got != "sk_k8s" {
		t.Fatalf("Get = %q, %v", got, err)
	}
}
