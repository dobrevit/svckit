//go:build integration

// The tests in this file need a reachable Redis. They are gated behind the
// "integration" build tag so a plain `go test ./...` never reaches them, and
// keep the -short guard as a second gate for runs that enable the tag but ask
// for the short tier. Point them at a server with REDIS_TEST_URL.

package rediscluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// requireRedis skips the test when no Redis answers at testRedisURL. It
// checks with a client of its own: a failure to build the adapter is what
// these tests look for, and used to be skipped as if Redis were down, which
// hid an adapter that could not connect with a partly filled config.
func requireRedis(t *testing.T) {
	t.Helper()
	opt, err := redis.ParseURL(testRedisURL())
	if err != nil {
		t.Fatalf("REDIS_TEST_URL: %v", err)
	}
	client := redis.NewClient(opt)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no Redis at %s: %v", testRedisURL(), err)
	}
}

// Integration test (requires Redis running)
func TestClusterAdapter_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := &ClusterConfig{
		NodeURLs:                []string{testRedisURL()},
		ServiceName:             "test-service",
		MaxPoolSize:             10,
		MinIdleConns:            2,
		HealthCheckInterval:     1 * time.Second,
		HealthCheckTimeout:      500 * time.Millisecond,
		MaxRetries:              2,
		CircuitBreakerThreshold: 3,
		LoadBalanceStrategy:     "round_robin",
	}

	requireRedis(t)
	adapter, err := NewClusterAdapter(config)
	if err != nil {
		t.Fatalf("Redis answers PING but the adapter could not be built: %v", err)
	}
	defer adapter.Close()

	// Test basic operation
	ctx := context.Background()
	err = adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		return client.Set(ctx, "test:key", "test-value", time.Minute).Err()
	}, "test:key")
	assert.NoError(t, err)

	// Test get operation
	var value string
	err = adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.Get(ctx, "test:key")
		if result.Err() != nil {
			return result.Err()
		}
		value = result.Val()
		return nil
	}, "test:key")
	assert.NoError(t, err)
	assert.Equal(t, "test-value", value)

	// Test cluster health
	health := adapter.GetClusterHealth()
	assert.GreaterOrEqual(t, health.HealthyNodes, 1)
	assert.Equal(t, len(config.NodeURLs), health.TotalNodes)

	// Test metrics
	metrics := adapter.GetMetrics()
	assert.GreaterOrEqual(t, metrics.TotalOperations, int64(2)) // At least 2 operations (set + get)
}

func TestClusterClient_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := &ClusterConfig{
		NodeURLs:            []string{testRedisURL()},
		ServiceName:         "test-client-service",
		MaxPoolSize:         10,
		HealthCheckInterval: 1 * time.Second,
		LoadBalanceStrategy: "round_robin",
	}

	requireRedis(t)
	adapter, err := NewClusterAdapter(config)
	if err != nil {
		t.Fatalf("Redis answers PING but the adapter could not be built: %v", err)
	}
	defer adapter.Close()

	client := NewClusterClient(adapter)
	ctx := context.Background()

	// Test string operations
	err = client.Set(ctx, "client:test", "value123", time.Minute).Err()
	assert.NoError(t, err)

	result := client.Get(ctx, "client:test")
	assert.NoError(t, result.Err())
	assert.Equal(t, "value123", result.Val())

	// Test hash operations
	err = client.HSet(ctx, "client:hash", "field1", "value1", "field2", "value2").Err()
	assert.NoError(t, err)

	hashResult := client.HGetAll(ctx, "client:hash")
	assert.NoError(t, hashResult.Err())
	values := hashResult.Val()
	assert.Equal(t, "value1", values["field1"])
	assert.Equal(t, "value2", values["field2"])

	// Test set operations
	err = client.SAdd(ctx, "client:set", "member1", "member2").Err()
	assert.NoError(t, err)

	setResult := client.SMembers(ctx, "client:set")
	assert.NoError(t, setResult.Err())
	members := setResult.Val()
	assert.Contains(t, members, "member1")
	assert.Contains(t, members, "member2")

	// Clean up
	client.Del(ctx, "client:test", "client:hash", "client:set")
}

// A config literal that leaves settings out used to run with zeros: the
// ping timeout expired before the ping, so NewClusterAdapter failed.
func TestNewClusterAdapterAppliesDefaults(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	requireRedis(t)

	config := &ClusterConfig{NodeURLs: []string{testRedisURL()}, ServiceName: "defaults-test"}
	adapter, err := NewClusterAdapter(config)
	if err != nil {
		t.Fatalf("NewClusterAdapter with only NodeURLs and ServiceName: %v", err)
	}
	defer adapter.Close()

	got := adapter.config
	if got.HealthCheckTimeout != 5*time.Second || got.HealthCheckInterval != 30*time.Second ||
		got.RetryBackoffBase != time.Second || got.CircuitBreakerThreshold != 5 || got.MaxRetries != 3 {
		t.Errorf("the adapter runs with %+v, want the defaults", got)
	}
	if config.HealthCheckTimeout != 0 {
		t.Error("NewClusterAdapter changed the caller's config")
	}

	err = adapter.ExecuteWithRetry(context.Background(), func(ctx context.Context, c *redis.Client) error {
		return c.Set(ctx, "defaults:key", "v", time.Minute).Err()
	}, "defaults:key")
	if err != nil {
		t.Errorf("a command on the adapter: %v", err)
	}
	_ = adapter.Client().Del(context.Background(), "defaults:key").Err()
}

// A failing call with retries used to panic in rand.Int63n when the backoff
// was zero; now it returns an error.
func TestARetriedFailureReturnsAnError(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	requireRedis(t)

	adapter, err := NewClusterAdapter(&ClusterConfig{
		NodeURLs:         []string{testRedisURL()},
		ServiceName:      "defaults-test",
		MaxRetries:       1,
		RetryBackoffBase: time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()

	err = adapter.ExecuteWithRetry(context.Background(), func(context.Context, *redis.Client) error {
		return errors.New("transient")
	})
	if err == nil {
		t.Error("a call that always fails succeeded")
	}
}

// With the threshold left out it was zero, and one error degraded the node.
func TestANodeIsDegradedAtTheDefaultThresholdNotBefore(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	requireRedis(t)

	adapter, err := NewClusterAdapter(&ClusterConfig{
		NodeURLs:         []string{testRedisURL()},
		ServiceName:      "defaults-test",
		RetryBackoffBase: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()

	fail := func() {
		_ = adapter.ExecuteWithRetry(context.Background(), func(context.Context, *redis.Client) error {
			return errors.New("transient")
		})
	}

	fail()
	if healthy := adapter.GetClusterHealth().HealthyNodes; healthy != 1 {
		t.Fatalf("%d healthy nodes after one error, want 1", healthy)
	}
	for range 4 {
		fail()
	}
	if healthy := adapter.GetClusterHealth().HealthyNodes; healthy != 0 {
		t.Errorf("%d healthy nodes after five errors, want the node degraded", healthy)
	}
}
