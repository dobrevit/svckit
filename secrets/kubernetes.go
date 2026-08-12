package secrets

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// In-cluster ServiceAccount paths projected by the kubelet.
const (
	saTokenFile     = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	saNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	saCACertFile    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

// KubernetesStore reads and writes Kubernetes Secrets through the API server
// using the pod's ServiceAccount - plain HTTP like the Vault backend, so the
// vendored service modules stay free of client-go's dependency tree.
//
// Logical paths map onto Secret names and data keys like this:
//
//	service-keys/<svc> -> Secret "svc-key-<svc>", key "api-key"
//	kms/<material>     -> Secret "kms-material",  key "<material>"
//	anything else      -> Secret "<path with / -> ->" , key "value"
//
// RBAC gives each service `get` on only its own secret and the seeder write
// access, so a compromised consumer can read nothing but its own key.
// DefaultManagedByLabel is stamped as app.kubernetes.io/managed-by on
// Secrets this store creates. Deployments that want their own seeder
// identity on the label set SECRETS_MANAGED_BY.
var DefaultManagedByLabel = "secrets-seeder"

// managedByLabel resolves the label value, letting a deployment override the
// default without recompiling.
func managedByLabel() string {
	if label := os.Getenv("SECRETS_MANAGED_BY"); label != "" {
		return label
	}
	return DefaultManagedByLabel
}

type KubernetesStore struct {
	apiURL    string
	namespace string
	token     string
	client    *http.Client
}

// NewKubernetesStoreFromEnv builds a KubernetesStore from the in-cluster
// ServiceAccount, with env overrides for tests and out-of-cluster use:
// KUBERNETES_API_URL, SECRETS_K8S_NAMESPACE, SECRETS_K8S_TOKEN.
func NewKubernetesStoreFromEnv() (*KubernetesStore, error) {
	apiURL := os.Getenv("KUBERNETES_API_URL")
	if apiURL == "" {
		host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
		if host == "" || port == "" {
			return nil, errors.New("kubernetes backend requires an in-cluster environment or KUBERNETES_API_URL")
		}
		apiURL = "https://" + host + ":" + port
	}

	token := os.Getenv("SECRETS_K8S_TOKEN")
	if token == "" {
		raw, err := os.ReadFile(saTokenFile)
		if err != nil {
			return nil, fmt.Errorf("kubernetes backend: no SECRETS_K8S_TOKEN and no ServiceAccount token (is automountServiceAccountToken enabled?): %w", err)
		}
		token = strings.TrimSpace(string(raw))
	}

	namespace := os.Getenv("SECRETS_K8S_NAMESPACE")
	if namespace == "" {
		if raw, err := os.ReadFile(saNamespaceFile); err == nil {
			namespace = strings.TrimSpace(string(raw))
		} else {
			namespace = "default"
		}
	}

	client := &http.Client{Timeout: 10 * time.Second}
	if strings.HasPrefix(apiURL, "https://") {
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		if caCert, err := os.ReadFile(saCACertFile); err == nil {
			pool.AppendCertsFromPEM(caCert)
		}
		client.Transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}
	}

	return &KubernetesStore{apiURL: strings.TrimSuffix(apiURL, "/"), namespace: namespace, token: token, client: client}, nil
}

// locate maps a logical path onto a Secret name and data key.
func (k *KubernetesStore) locate(path string) (secretName, key string) {
	switch {
	case strings.HasPrefix(path, "service-keys/"):
		return "svc-key-" + strings.TrimPrefix(path, "service-keys/"), "api-key"
	case strings.HasPrefix(path, "kms/"):
		return "kms-material", strings.TrimPrefix(path, "kms/")
	default:
		return strings.ReplaceAll(path, "/", "-"), "value"
	}
}

func (k *KubernetesStore) secretURL(secretName string) string {
	url := fmt.Sprintf("%s/api/v1/namespaces/%s/secrets", k.apiURL, k.namespace)
	if secretName != "" {
		url += "/" + secretName
	}
	return url
}

func (k *KubernetesStore) do(ctx context.Context, method, url, contentType string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+k.token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := k.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kubernetes: %s %s: %v: %w", method, url, err, ErrUnavailable)
	}
	return resp, nil
}

// Get performs a single read attempt; compose with Retrying (as NewFromEnv
// does) to absorb startup ordering.
func (k *KubernetesStore) Get(ctx context.Context, path string) (string, error) {
	secretName, key := k.locate(path)
	resp, err := k.do(ctx, http.MethodGet, k.secretURL(secretName), "", nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if err := k.checkStatus(resp, path); err != nil {
		return "", err
	}

	var payload struct {
		Data map[string]string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("kubernetes: decoding secret %q: %w", secretName, err)
	}
	encoded, ok := payload.Data[key]
	if !ok || encoded == "" {
		return "", fmt.Errorf("kubernetes: secret %q has no %q data: %w", secretName, key, ErrNotFound)
	}
	value, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("kubernetes: secret %q key %q is not valid base64: %v", secretName, key, err)
	}
	if len(value) == 0 {
		// An empty value is a tombstone: the secret was revoked, which is
		// distinct from it never having existed.
		return "", fmt.Errorf("kubernetes: secret %q key %q is empty: %w", secretName, key, ErrNotFound)
	}
	return string(value), nil
}

// Set creates or updates the target Secret. Updates use a JSON merge patch so
// other keys in a shared Secret (kms-material) are preserved.
func (k *KubernetesStore) Set(ctx context.Context, path string, value string) error {
	secretName, key := k.locate(path)
	encoded := base64.StdEncoding.EncodeToString([]byte(value))

	patch, err := json.Marshal(map[string]any{"data": map[string]string{key: encoded}})
	if err != nil {
		return err
	}
	resp, err := k.do(ctx, http.MethodPatch, k.secretURL(secretName), "application/merge-patch+json", bytes.NewReader(patch))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusOK {
		return nil
	}
	if resp.StatusCode != http.StatusNotFound {
		return k.checkStatus(resp, path)
	}

	// Secret does not exist yet: create it.
	manifest, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name":   secretName,
			"labels": map[string]string{"app.kubernetes.io/managed-by": managedByLabel()},
		},
		"type": "Opaque",
		"data": map[string]string{key: encoded},
	})
	if err != nil {
		return err
	}
	createResp, err := k.do(ctx, http.MethodPost, k.secretURL(""), "application/json", bytes.NewReader(manifest))
	if err != nil {
		return err
	}
	defer func() { _ = createResp.Body.Close() }()
	if createResp.StatusCode == http.StatusCreated || createResp.StatusCode == http.StatusOK {
		return nil
	}
	return k.checkStatus(createResp, path)
}

// Exists performs a single presence check without retrying.
func (k *KubernetesStore) Exists(ctx context.Context, path string) (bool, error) {
	_, err := k.Get(ctx, path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return false, err
}

func (k *KubernetesStore) checkStatus(resp *http.Response, path string) error {
	switch {
	case resp.StatusCode == http.StatusOK:
		return nil
	case resp.StatusCode == http.StatusNotFound:
		return fmt.Errorf("kubernetes: %q: %w", path, ErrNotFound)
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		return fmt.Errorf("kubernetes: %q (status %d; ServiceAccount lacks RBAC for this secret): %w", path, resp.StatusCode, ErrAccessDenied)
	case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode >= 500:
		return fmt.Errorf("kubernetes: %q returned %d: %w", path, resp.StatusCode, ErrUnavailable)
	default:
		return fmt.Errorf("kubernetes: %q returned unexpected status %d", path, resp.StatusCode)
	}
}
