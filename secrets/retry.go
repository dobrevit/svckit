package secrets

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Retrying wraps a Store so Get retries with exponential backoff while the
// secret is missing (ErrNotFound - the seeder may not have run yet) or the
// backend is transiently down (ErrUnavailable - the Vault container may still
// be starting). This is what makes compose/K8s startup ordering an
// optimization instead of a correctness requirement. Any other error is
// terminal, and the loop always stops when ctx is done.
//
// Set and Exists are single-shot passthroughs: seeding failures and presence
// checks must surface immediately.
func Retrying(inner Store, initialBackoff, maxBackoff time.Duration) Store {
	return &retryingStore{inner: inner, initial: initialBackoff, max: maxBackoff}
}

type retryingStore struct {
	inner   Store
	initial time.Duration
	max     time.Duration
}

func (r *retryingStore) Get(ctx context.Context, path string) (string, error) {
	backoff := r.initial
	for {
		value, err := r.inner.Get(ctx, path)
		if err == nil {
			return value, nil
		}
		if !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrUnavailable) {
			return "", err
		}

		select {
		case <-ctx.Done():
			return "", fmt.Errorf("%w (last attempt: %v)", ctx.Err(), err)
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > r.max {
			backoff = r.max
		}
	}
}

func (r *retryingStore) Set(ctx context.Context, path string, value string) error {
	return r.inner.Set(ctx, path, value)
}

func (r *retryingStore) Exists(ctx context.Context, path string) (bool, error) {
	return r.inner.Exists(ctx, path)
}
