package rediscluster

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// LoadConfigFromEnv loads Redis cluster configuration from environment variables
func LoadConfigFromEnv(serviceName string) (*ClusterConfig, error) {
	config := &ClusterConfig{
		ServiceName: serviceName,
	}

	// Load connection URLs
	if err := config.loadConnectionURLs(); err != nil {
		return nil, err
	}

	// Load connection pool settings
	config.loadConnectionPoolSettings()

	// Load health and retry settings
	config.loadHealthSettings()

	// Load circuit breaker settings
	config.loadCircuitBreakerSettings()

	// Load read balancing settings
	config.loadReadBalancingSettings()

	// Set defaults
	config.setDefaults()

	return config, config.Validate()
}

// loadConnectionURLs loads Redis connection URLs from environment
func (c *ClusterConfig) loadConnectionURLs() error {
	// Check for multiple node URLs first
	if nodeURLs := os.Getenv("REDIS_CLUSTER_NODES"); nodeURLs != "" {
		c.NodeURLs = strings.Split(nodeURLs, ",")
		for i, url := range c.NodeURLs {
			c.NodeURLs[i] = strings.TrimSpace(url)
		}
		return nil
	}

	// Alternative: individual node URLs
	for i := 1; i <= 10; i++ { // Support up to 10 nodes
		key := fmt.Sprintf("REDIS_NODE_%d_URL", i)
		if url := os.Getenv(key); url != "" {
			c.NodeURLs = append(c.NodeURLs, url)
		}
	}

	// Single Redis URL fallback
	if len(c.NodeURLs) == 0 {
		if url := os.Getenv("REDIS_URL"); url != "" {
			c.NodeURLs = append(c.NodeURLs, url)
		} else {
			// Build from components
			url := c.buildURLFromComponents("")
			if url != "" {
				c.NodeURLs = append(c.NodeURLs, url)
			}
		}
	}

	// Check for additional nodes using components
	if len(c.NodeURLs) == 1 {
		for i := 2; i <= 5; i++ {
			suffix := fmt.Sprintf("_%d", i)
			if url := c.buildURLFromComponents(suffix); url != "" {
				c.NodeURLs = append(c.NodeURLs, url)
			}
		}
	}

	if len(c.NodeURLs) == 0 {
		return fmt.Errorf("no Redis nodes configured")
	}

	// Load password (shared across nodes)
	c.Password = os.Getenv("REDIS_PASSWORD")

	return nil
}

// buildURLFromComponents builds a Redis URL from environment components
func (c *ClusterConfig) buildURLFromComponents(suffix string) string {
	host := os.Getenv("REDIS_HOST" + suffix)
	if host == "" && suffix == "" {
		host = os.Getenv("REDIS_HOST")
		if host == "" {
			host = "localhost"
		}
	} else if host == "" {
		return "" // No host for additional nodes
	}

	port := os.Getenv("REDIS_PORT" + suffix)
	if port == "" {
		port = os.Getenv("REDIS_PORT")
		if port == "" {
			port = "6379"
		}
	}

	db := os.Getenv("REDIS_DB" + suffix)
	if db == "" {
		db = os.Getenv("REDIS_DB")
		if db == "" {
			db = "0"
		}
	}

	// Format: redis://[password@]host:port/database
	if c.Password != "" {
		return fmt.Sprintf("redis://:%s@%s:%s/%s", c.Password, host, port, db)
	}
	return fmt.Sprintf("redis://%s:%s/%s", host, port, db)
}

// loadConnectionPoolSettings loads connection pool configuration
func (c *ClusterConfig) loadConnectionPoolSettings() {
	if maxPool := os.Getenv("REDIS_MAX_POOL_SIZE"); maxPool != "" {
		if val, err := strconv.Atoi(maxPool); err == nil && val > 0 {
			c.MaxPoolSize = val
		}
	}

	if minIdle := os.Getenv("REDIS_MIN_IDLE_CONNS"); minIdle != "" {
		if val, err := strconv.Atoi(minIdle); err == nil && val >= 0 {
			c.MinIdleConns = val
		}
	}

	if maxIdleTime := os.Getenv("REDIS_MAX_IDLE_TIME"); maxIdleTime != "" {
		if val, err := time.ParseDuration(maxIdleTime); err == nil {
			c.MaxIdleTime = val
		}
	}

	if connMaxLifetime := os.Getenv("REDIS_CONN_MAX_LIFETIME"); connMaxLifetime != "" {
		if val, err := time.ParseDuration(connMaxLifetime); err == nil {
			c.ConnMaxLifetime = val
		}
	}
}

// loadHealthSettings loads health check and retry configuration
func (c *ClusterConfig) loadHealthSettings() {
	if interval := os.Getenv("REDIS_HEALTH_CHECK_INTERVAL"); interval != "" {
		if val, err := time.ParseDuration(interval); err == nil {
			c.HealthCheckInterval = val
		}
	}

	if timeout := os.Getenv("REDIS_HEALTH_CHECK_TIMEOUT"); timeout != "" {
		if val, err := time.ParseDuration(timeout); err == nil {
			c.HealthCheckTimeout = val
		}
	}

	if retries := os.Getenv("REDIS_MAX_RETRIES"); retries != "" {
		if val, err := strconv.Atoi(retries); err == nil && val >= 0 {
			c.MaxRetries = val
		}
	}

	if backoff := os.Getenv("REDIS_RETRY_BACKOFF_BASE"); backoff != "" {
		if val, err := time.ParseDuration(backoff); err == nil {
			c.RetryBackoffBase = val
		}
	}

	if maxBackoff := os.Getenv("REDIS_RETRY_BACKOFF_MAX"); maxBackoff != "" {
		if val, err := time.ParseDuration(maxBackoff); err == nil {
			c.RetryBackoffMax = val
		}
	}
}

// loadCircuitBreakerSettings loads circuit breaker configuration
func (c *ClusterConfig) loadCircuitBreakerSettings() {
	if threshold := os.Getenv("REDIS_CIRCUIT_BREAKER_THRESHOLD"); threshold != "" {
		if val, err := strconv.Atoi(threshold); err == nil && val > 0 {
			c.CircuitBreakerThreshold = val
		}
	}

	if timeout := os.Getenv("REDIS_CIRCUIT_BREAKER_TIMEOUT"); timeout != "" {
		if val, err := time.ParseDuration(timeout); err == nil {
			c.CircuitBreakerTimeout = val
		}
	}
}

// loadReadBalancingSettings loads read balancing configuration
func (c *ClusterConfig) loadReadBalancingSettings() {
	strategy := strings.ToLower(os.Getenv("REDIS_LOAD_BALANCE_STRATEGY"))
	validStrategies := []string{"round_robin", "random", "hash"}

	for _, valid := range validStrategies {
		if strategy == valid {
			c.LoadBalanceStrategy = strategy
			return
		}
	}

	// Default strategy
	c.LoadBalanceStrategy = "round_robin"
}

// setDefaults sets default values for unset configuration
func (c *ClusterConfig) setDefaults() {
	if c.MaxPoolSize == 0 {
		c.MaxPoolSize = 100
	}
	if c.MinIdleConns == 0 {
		c.MinIdleConns = 10
	}
	if c.MaxIdleTime == 0 {
		c.MaxIdleTime = 30 * time.Second
	}
	if c.ConnMaxLifetime == 0 {
		c.ConnMaxLifetime = 5 * time.Minute
	}
	if c.HealthCheckInterval == 0 {
		c.HealthCheckInterval = 30 * time.Second
	}
	if c.HealthCheckTimeout == 0 {
		c.HealthCheckTimeout = 5 * time.Second
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 3
	}
	if c.RetryBackoffBase == 0 {
		c.RetryBackoffBase = 1 * time.Second
	}
	if c.RetryBackoffMax == 0 {
		c.RetryBackoffMax = 30 * time.Second
	}
	if c.CircuitBreakerThreshold == 0 {
		c.CircuitBreakerThreshold = 5
	}
	if c.CircuitBreakerTimeout == 0 {
		c.CircuitBreakerTimeout = 60 * time.Second
	}
}

// Validate ensures the configuration is valid
func (c *ClusterConfig) Validate() error {
	if len(c.NodeURLs) == 0 {
		return fmt.Errorf("at least one Redis node URL is required")
	}

	if c.ServiceName == "" {
		return fmt.Errorf("service name is required")
	}

	if c.MaxPoolSize <= 0 {
		return fmt.Errorf("max pool size must be positive")
	}

	if c.MinIdleConns < 0 || c.MinIdleConns > c.MaxPoolSize {
		return fmt.Errorf("min idle connections must be between 0 and max pool size")
	}

	if c.HealthCheckInterval <= 0 {
		return fmt.Errorf("health check interval must be positive")
	}

	if c.MaxRetries < 0 {
		return fmt.Errorf("max retries must be non-negative")
	}

	validStrategies := []string{"round_robin", "random", "hash"}
	validStrategy := false
	for _, strategy := range validStrategies {
		if c.LoadBalanceStrategy == strategy {
			validStrategy = true
			break
		}
	}
	if !validStrategy {
		return fmt.Errorf("invalid load balance strategy: %s", c.LoadBalanceStrategy)
	}

	return nil
}

// String returns a string representation of the configuration
func (c *ClusterConfig) String() string {
	maskedNodes := make([]string, len(c.NodeURLs))
	for i, url := range c.NodeURLs {
		maskedNodes[i] = maskURL(url)
	}

	return fmt.Sprintf("ClusterConfig{Nodes: %v, Strategy: %s, MaxPool: %d, HealthCheck: %v}",
		maskedNodes, c.LoadBalanceStrategy, c.MaxPoolSize, c.HealthCheckInterval)
}

// maskURL masks sensitive information in Redis URLs
func maskURL(url string) string {
	if strings.Contains(url, "://") && strings.Contains(url, "@") {
		parts := strings.Split(url, "@")
		if len(parts) >= 2 {
			schemeParts := strings.Split(parts[0], "://")
			if len(schemeParts) >= 2 {
				// Redis URLs might have just password (no username)
				if strings.HasPrefix(schemeParts[1], ":") {
					return schemeParts[0] + "://:***@" + strings.Join(parts[1:], "@")
				}
				// Or username:password format
				credsParts := strings.Split(schemeParts[1], ":")
				if len(credsParts) >= 2 {
					return schemeParts[0] + "://" + credsParts[0] + ":***@" + strings.Join(parts[1:], "@")
				}
			}
		}
	}
	return url
}

// Example environment configurations

// Single Redis (Development)
const DevEnvExample = `
# Single Redis instance for development
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=

# Connection pooling
REDIS_MAX_POOL_SIZE=50
REDIS_MIN_IDLE_CONNS=5
REDIS_MAX_IDLE_TIME=30s

# Health checks
REDIS_HEALTH_CHECK_INTERVAL=30s
REDIS_MAX_RETRIES=3
`

// Redis Cluster (Production)
const ProdClusterExample = `
# Multiple Redis nodes for production
REDIS_CLUSTER_NODES=redis://redis1:6379/0,redis://redis2:6379/0,redis://redis3:6379/0
REDIS_PASSWORD=secure_password

# Connection pooling
REDIS_MAX_POOL_SIZE=100
REDIS_MIN_IDLE_CONNS=10
REDIS_MAX_IDLE_TIME=30s
REDIS_CONN_MAX_LIFETIME=5m

# Health and retry settings
REDIS_HEALTH_CHECK_INTERVAL=30s
REDIS_HEALTH_CHECK_TIMEOUT=5s
REDIS_MAX_RETRIES=3
REDIS_RETRY_BACKOFF_BASE=1s
REDIS_RETRY_BACKOFF_MAX=30s

# Circuit breaker
REDIS_CIRCUIT_BREAKER_THRESHOLD=5
REDIS_CIRCUIT_BREAKER_TIMEOUT=60s

# Load balancing
REDIS_LOAD_BALANCE_STRATEGY=round_robin
`

// Sentinel Configuration
const SentinelExample = `
# Redis Sentinel configuration
REDIS_SENTINEL_NODES=sentinel1:26379,sentinel2:26379,sentinel3:26379
REDIS_SENTINEL_MASTER_NAME=mymaster
REDIS_PASSWORD=secure_password

# Optimized for failover
REDIS_HEALTH_CHECK_INTERVAL=10s
REDIS_CIRCUIT_BREAKER_THRESHOLD=3
REDIS_CIRCUIT_BREAKER_TIMEOUT=30s
`
