package rediscluster

import (
	"testing"
	"time"
)

// A zero RetryBackoffBase used to reach rand.Int63n(0), which panics in the
// caller's goroutine on the first retry.
func TestCalculateBackoffNeverPanics(t *testing.T) {
	for _, base := range []time.Duration{0, time.Nanosecond, 3 * time.Nanosecond, -time.Second} {
		ca := &ClusterAdapter{config: ClusterConfig{RetryBackoffBase: base, RetryBackoffMax: time.Second}}
		for attempt := range 3 {
			if got := ca.calculateBackoff(attempt); got < 0 {
				t.Errorf("base %v, attempt %d: backoff %v", base, attempt, got)
			}
		}
	}

	ca := &ClusterAdapter{config: ClusterConfig{RetryBackoffBase: 100 * time.Millisecond, RetryBackoffMax: time.Second}}
	if got := ca.calculateBackoff(1); got < 200*time.Millisecond || got > 250*time.Millisecond {
		t.Errorf("backoff for attempt 1 at a 100ms base = %v, want 200ms plus up to a quarter of jitter", got)
	}
}

// The settings that must be positive get the default when they are not:
// zero (left out of a literal) and negative alike.
func TestSetDefaultsReplacesNonPositiveSettings(t *testing.T) {
	for _, v := range []time.Duration{0, -time.Second} {
		c := ClusterConfig{
			HealthCheckInterval:     v,
			HealthCheckTimeout:      v,
			RetryBackoffBase:        v,
			RetryBackoffMax:         v,
			CircuitBreakerThreshold: int(v / time.Second),
		}
		c.setDefaults()
		if c.HealthCheckInterval != 30*time.Second || c.HealthCheckTimeout != 5*time.Second ||
			c.RetryBackoffBase != time.Second || c.RetryBackoffMax != 30*time.Second || c.CircuitBreakerThreshold != 5 {
			t.Errorf("from %v: %+v", v, c)
		}
	}

	set := ClusterConfig{HealthCheckTimeout: 2 * time.Second, CircuitBreakerThreshold: 9}
	set.setDefaults()
	if set.HealthCheckTimeout != 2*time.Second || set.CircuitBreakerThreshold != 9 {
		t.Errorf("setDefaults overrode values the caller set: %+v", set)
	}
}
