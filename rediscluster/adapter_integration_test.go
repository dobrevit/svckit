//go:build integration

// The tests in this file need a reachable Redis. They are gated behind the
// "integration" build tag so a plain `go test ./...` never reaches them, and
// keep the -short guard as a second gate for runs that enable the tag but ask
// for the short tier. Point them at a server with REDIS_TEST_URL.

package rediscluster

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

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

	adapter, err := NewClusterAdapter(config)
	if err != nil {
		t.Skipf("Could not connect to Redis: %v", err)
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

	adapter, err := NewClusterAdapter(config)
	if err != nil {
		t.Skipf("Could not connect to Redis: %v", err)
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
