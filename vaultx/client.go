package vaultx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dobrevit/svckit/logging"
)

// ErrForbidden is returned when Vault refuses a request with 403: the
// client's policy does not allow it.
var ErrForbidden = errors.New("vaultx: permission denied")

// AppRole is the AppRole login of a Client.
type AppRole struct {
	RoleID   string
	SecretID string
	Mount    string // "approle" when empty
}

// Config configures a Client.
type Config struct {
	// Address is Vault's address, such as https://vault.internal:8200.
	Address string

	// Token authenticates directly. Use it or AppRole.
	Token string

	// AppRole logs in with a role and secret id, and logs in again when the
	// token can no longer be renewed.
	AppRole *AppRole

	// HTTPClient sends the requests; a client with a 30-second timeout when
	// nil. Give it the TLS configuration Vault's certificate needs.
	HTTPClient *http.Client
}

// Client is an authenticated connection to Vault.
type Client struct {
	addr    string
	http    *http.Client
	appRole *AppRole

	mu        sync.RWMutex
	token     string
	renewable bool
	ttl       time.Duration
	obtained  time.Time
	renewals  int // successful renewals, for tests

	stop chan struct{}
	done chan struct{}
}

// New authenticates to Vault and, for a token that expires, starts renewing
// it in the background; Close stops that.
func New(ctx context.Context, config Config) (*Client, error) {
	if config.Address == "" {
		return nil, errors.New("vaultx: Address is required")
	}
	if (config.Token == "") == (config.AppRole == nil) {
		return nil, errors.New("vaultx: give exactly one of Token and AppRole")
	}
	c := &Client{
		addr:    strings.TrimRight(config.Address, "/"),
		http:    config.HTTPClient,
		appRole: config.AppRole,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: 30 * time.Second}
	}
	if c.appRole != nil && c.appRole.Mount == "" {
		c.appRole.Mount = "approle"
	}

	if config.Token != "" {
		c.token = config.Token
		if err := c.lookupSelf(ctx); err != nil {
			return nil, err
		}
	} else if err := c.login(ctx); err != nil {
		return nil, err
	}

	go c.renewLoop()
	return c, nil
}

// Close stops the token renewal.
func (c *Client) Close() {
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
	<-c.done
}

// login authenticates with AppRole.
func (c *Client) login(ctx context.Context) error {
	resp, err := c.request(ctx, http.MethodPost, "auth/"+c.appRole.Mount+"/login", map[string]any{
		"role_id":   c.appRole.RoleID,
		"secret_id": c.appRole.SecretID,
	}, false)
	if err != nil {
		return fmt.Errorf("vaultx: AppRole login: %w", err)
	}
	if resp.Auth == nil || resp.Auth.ClientToken == "" {
		return errors.New("vaultx: AppRole login returned no token")
	}
	c.setToken(resp.Auth.ClientToken, resp.Auth.Renewable, time.Duration(resp.Auth.LeaseDuration)*time.Second)
	return nil
}

// lookupSelf reads a given token's TTL, so that it is renewed too.
func (c *Client) lookupSelf(ctx context.Context) error {
	resp, err := c.request(ctx, http.MethodGet, "auth/token/lookup-self", nil, true)
	if err != nil {
		return fmt.Errorf("vaultx: look up the token: %w", err)
	}
	ttl, _ := resp.Data["ttl"].(float64)
	renewable, _ := resp.Data["renewable"].(bool)
	c.setToken(c.token, renewable, time.Duration(ttl)*time.Second)
	return nil
}

func (c *Client) setToken(token string, renewable bool, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.token, c.renewable, c.ttl, c.obtained = token, renewable, ttl, time.Now()
}

// renewLoop renews the token at two thirds of its lifetime. A token that
// cannot be renewed (not renewable, or at its maximum TTL) is replaced by a
// fresh AppRole login; a static token that cannot be renewed is left to
// expire, and requests then fail with Vault's error. A token with no TTL
// (a root token) needs nothing.
func (c *Client) renewLoop() {
	defer close(c.done)
	for {
		c.mu.RLock()
		ttl, obtained := c.ttl, c.obtained
		c.mu.RUnlock()
		if ttl <= 0 {
			<-c.stop
			return
		}

		wait := time.Until(obtained.Add(ttl * 2 / 3))
		select {
		case <-c.stop:
			return
		case <-time.After(wait):
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := c.renew(ctx)
		cancel()
		if err != nil {
			logging.Error("vaultx: keeping the Vault token alive failed, retrying in 5s: %v", err)
			select {
			case <-c.stop:
				return
			case <-time.After(5 * time.Second):
			}
		}
	}
}

// renew extends the token, or logs in again when it cannot be extended to
// what it was granted.
func (c *Client) renew(ctx context.Context) error {
	c.mu.RLock()
	renewable, ttl := c.renewable, c.ttl
	c.mu.RUnlock()

	if renewable {
		resp, err := c.request(ctx, http.MethodPost, "auth/token/renew-self", map[string]any{}, true)
		if err == nil && resp.Auth != nil {
			granted := time.Duration(resp.Auth.LeaseDuration) * time.Second
			// A renewal capped by the token's maximum TTL comes back short;
			// once it does, the next one would be shorter still, so log in
			// again instead.
			if granted >= ttl || c.appRole == nil {
				c.setToken(resp.Auth.ClientToken, resp.Auth.Renewable, granted)
				c.mu.Lock()
				c.renewals++
				c.mu.Unlock()
				return nil
			}
		} else if err != nil && c.appRole == nil {
			return err
		}
	}
	if c.appRole == nil {
		return errors.New("the token cannot be renewed and there is no AppRole to log in again with")
	}
	return c.login(ctx)
}

// vaultResponse is the envelope of Vault's API responses.
type vaultResponse struct {
	Data map[string]any `json:"data"`
	Auth *struct {
		ClientToken   string `json:"client_token"`
		LeaseDuration int    `json:"lease_duration"`
		Renewable     bool   `json:"renewable"`
	} `json:"auth"`
	Errors []string `json:"errors"`
}

// request calls Vault's API at path (without /v1), with the client's token
// when authenticated is true.
func (c *Client) request(ctx context.Context, method, path string, body any, authenticated bool) (*vaultResponse, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.addr+"/v1/"+path, reader)
	if err != nil {
		return nil, err
	}
	if authenticated {
		c.mu.RLock()
		req.Header.Set("X-Vault-Token", c.token)
		c.mu.RUnlock()
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }() // read in full below
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}

	var decoded vaultResponse
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil, fmt.Errorf("vault %s %s: %d, unreadable response", method, path, resp.StatusCode)
		}
	}
	switch {
	case resp.StatusCode == http.StatusForbidden:
		return nil, fmt.Errorf("%w: %s %s", ErrForbidden, method, path)
	case resp.StatusCode >= 400:
		return nil, fmt.Errorf("vault %s %s: %d %s", method, path, resp.StatusCode, strings.Join(decoded.Errors, "; "))
	}
	return &decoded, nil
}
