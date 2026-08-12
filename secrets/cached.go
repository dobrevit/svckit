package secrets

import (
	"context"
	"sync"
	"time"
)

// Cached wraps a Store with a per-path TTL cache so consumers can read
// secrets at call time instead of once at startup. The TTL is the upper
// bound on how long a rotated-out value keeps being used, and must stay
// below the IAM rotation grace window (see the architecture doc, §7).
// Misses are not cached: a secret that is not there yet (bootstrap
// ordering) must stay retryable.
func Cached(inner Store, ttl time.Duration) Store {
	return &cachedStore{
		inner:   inner,
		ttl:     ttl,
		entries: make(map[string]cacheEntry),
	}
}

type cacheEntry struct {
	value   string
	expires time.Time
}

type cachedStore struct {
	inner   Store
	ttl     time.Duration
	mu      sync.RWMutex
	entries map[string]cacheEntry
}

func (c *cachedStore) Get(ctx context.Context, path string) (string, error) {
	c.mu.RLock()
	entry, ok := c.entries[path]
	c.mu.RUnlock()
	if ok && time.Now().Before(entry.expires) {
		return entry.value, nil
	}

	value, err := c.inner.Get(ctx, path)
	if err != nil {
		return "", err
	}

	c.store(path, value)
	return value, nil
}

// Set writes through to the backend and refreshes the cached value so the
// writer immediately reads what it wrote.
func (c *cachedStore) Set(ctx context.Context, path string, value string) error {
	if err := c.inner.Set(ctx, path, value); err != nil {
		return err
	}
	c.store(path, value)
	return nil
}

func (c *cachedStore) Exists(ctx context.Context, path string) (bool, error) {
	c.mu.RLock()
	entry, ok := c.entries[path]
	c.mu.RUnlock()
	if ok && time.Now().Before(entry.expires) {
		return true, nil
	}
	return c.inner.Exists(ctx, path)
}

func (c *cachedStore) store(path, value string) {
	c.mu.Lock()
	c.entries[path] = cacheEntry{value: value, expires: time.Now().Add(c.ttl)}
	c.mu.Unlock()
}
