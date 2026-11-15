package vaultx

import (
	"context"
	"net/http"
	"time"

	"github.com/dobrevit/svckit/middleware"
)

// HealthChecker reports whether Vault answers and is unsealed, for a
// service's readiness: a service that seals or opens messages through Vault
// cannot do its work without it.
type HealthChecker struct {
	client *Client
	name   string
}

var _ middleware.HealthChecker = (*HealthChecker)(nil)

// NewHealthChecker returns a checker named name ("vault" when empty).
func NewHealthChecker(client *Client, name string) *HealthChecker {
	if name == "" {
		name = "vault"
	}
	return &HealthChecker{client: client, name: name}
}

// Check implements middleware.HealthChecker.
func (h *HealthChecker) Check() (string, string, string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// sys/health answers with a status code: 200 for an active, unsealed
	// node, 429 for an unsealed standby; anything else is not serving.
	if _, err := h.client.request(ctx, http.MethodGet, "sys/health?standbyok=true", nil, false); err != nil {
		return h.name, middleware.StatusUnhealthy, "Vault is not available: " + err.Error()
	}
	return h.name, middleware.StatusHealthy, "Vault is available"
}
