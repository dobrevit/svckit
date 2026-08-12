package secrets

import (
	"context"
	"errors"
)

// Chain combines stores so that Get falls through to the next store only on
// ErrNotFound - any other error is terminal, so an unreachable or misbehaving
// primary backend surfaces instead of silently serving stale env values.
// This is what makes migration non-breaking: the factory chains the selected
// backend with the env backend, so partially migrated environments keep
// resolving secrets from .env until those variables are removed.
func Chain(stores ...Store) Store {
	return chainStore(stores)
}

type chainStore []Store

func (c chainStore) Get(ctx context.Context, path string) (string, error) {
	lastErr := error(ErrNotFound)
	for _, s := range c {
		value, err := s.Get(ctx, path)
		if err == nil {
			return value, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return "", err
		}
		lastErr = err
	}
	return "", lastErr
}

// Set writes to the first store only: the primary backend is the writable
// one, fallbacks are read-only safety nets.
func (c chainStore) Set(ctx context.Context, path string, value string) error {
	if len(c) == 0 {
		return ErrReadOnly
	}
	return c[0].Set(ctx, path, value)
}

func (c chainStore) Exists(ctx context.Context, path string) (bool, error) {
	for _, s := range c {
		ok, err := s.Exists(ctx, path)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}
