package rediscluster

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  ClusterConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: ClusterConfig{
				NodeURLs:                []string{testRedisURL()},
				ServiceName:             "test-service",
				MaxPoolSize:             10,
				MinIdleConns:            5,
				HealthCheckInterval:     30 * time.Second,
				MaxRetries:              3,
				CircuitBreakerThreshold: 5,
				LoadBalanceStrategy:     "round_robin",
			},
			wantErr: false,
		},
		{
			name: "empty node URLs",
			config: ClusterConfig{
				ServiceName: "test-service",
			},
			wantErr: true,
		},
		{
			name: "empty service name",
			config: ClusterConfig{
				NodeURLs: []string{testRedisURL()},
			},
			wantErr: true,
		},
		{
			name: "invalid load balance strategy",
			config: ClusterConfig{
				NodeURLs:            []string{testRedisURL()},
				ServiceName:         "test-service",
				MaxPoolSize:         10,
				LoadBalanceStrategy: "invalid_strategy",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestClusterConfig_setDefaults(t *testing.T) {
	config := &ClusterConfig{}
	config.setDefaults()

	assert.Equal(t, 100, config.MaxPoolSize)
	assert.Equal(t, 10, config.MinIdleConns)
	assert.Equal(t, 30*time.Second, config.MaxIdleTime)
	assert.Equal(t, 5*time.Minute, config.ConnMaxLifetime)
	assert.Equal(t, 30*time.Second, config.HealthCheckInterval)
	assert.Equal(t, 5*time.Second, config.HealthCheckTimeout)
	assert.Equal(t, 3, config.MaxRetries)
	assert.Equal(t, 1*time.Second, config.RetryBackoffBase)
	assert.Equal(t, 30*time.Second, config.RetryBackoffMax)
	assert.Equal(t, 5, config.CircuitBreakerThreshold)
	assert.Equal(t, 60*time.Second, config.CircuitBreakerTimeout)
}

func TestLoadConfigFromEnv(t *testing.T) {
	// Test with minimal environment
	t.Setenv("REDIS_HOST", "localhost")
	t.Setenv("REDIS_PORT", "6379")
	t.Setenv("REDIS_DB", "0")

	config, err := LoadConfigFromEnv("test-service")
	require.NoError(t, err)
	assert.Equal(t, "test-service", config.ServiceName)
	assert.Len(t, config.NodeURLs, 1)
	assert.Contains(t, config.NodeURLs[0], "localhost:6379")
}

func TestNodeState_String(t *testing.T) {
	assert.Equal(t, "healthy", NodeHealthy.String())
	assert.Equal(t, "degraded", NodeDegraded.String())
	assert.Equal(t, "failed", NodeFailed.String())
}

func TestTombManager(t *testing.T) {
	tm := NewTombManager("test-service")
	assert.NotNil(t, tm)
	assert.Equal(t, "test-service", tm.name)
	assert.False(t, tm.IsShuttingDown())

	// Test context
	ctx := tm.Context()
	assert.NotNil(t, ctx)

	// Test shutdown
	tm.Shutdown()
	assert.True(t, tm.IsShuttingDown())
}

func TestMaskURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "URL with password",
			input:    "redis://:password@localhost:6379/0",
			expected: "redis://:***@localhost:6379/0",
		},
		{
			name:     "URL with username and password",
			input:    "redis://user:password@localhost:6379/0",
			expected: "redis://user:***@localhost:6379/0",
		},
		{
			name:     "URL without credentials",
			input:    "redis://localhost:6379/0",
			expected: "redis://localhost:6379/0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := maskURL(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestClusterMetrics(t *testing.T) {
	metrics := ClusterMetrics{
		TotalOperations:  100,
		FailedOperations: 5,
		AverageLatency:   50 * time.Millisecond,
		MinLatency:       10 * time.Millisecond,
		MaxLatency:       100 * time.Millisecond,
		Timestamp:        time.Now(),
	}

	assert.Equal(t, int64(100), metrics.TotalOperations)
	assert.Equal(t, int64(5), metrics.FailedOperations)
	assert.Equal(t, 50*time.Millisecond, metrics.AverageLatency)
}

func TestClusterHealth(t *testing.T) {
	health := ClusterHealth{
		Nodes: []NodeHealth{
			{
				Name:       "node-1",
				State:      NodeHealthy,
				OpsCount:   100,
				ErrorCount: 5,
			},
			{
				Name:       "node-2",
				State:      NodeFailed,
				OpsCount:   50,
				ErrorCount: 25,
			},
		},
		TotalNodes:   2,
		HealthyNodes: 1,
		Timestamp:    time.Now(),
	}

	assert.Len(t, health.Nodes, 2)
	assert.Equal(t, 2, health.TotalNodes)
	assert.Equal(t, 1, health.HealthyNodes)
}

// testRedisURL is the address the integration tier connects to. The default
// matches the compose file shipped with the test harness; override it with
// REDIS_TEST_URL to point at another server.
func testRedisURL() string {
	if url := os.Getenv("REDIS_TEST_URL"); url != "" {
		return url
	}
	return "redis://localhost:6379"
}
