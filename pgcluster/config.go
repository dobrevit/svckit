package pgcluster

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// LoadConfigFromEnv loads database cluster configuration from environment variables
func LoadConfigFromEnv(serviceName string) (*ClusterConfig, error) {
	config := &ClusterConfig{
		ServiceName: serviceName,
	}

	// Load connection URLs
	if err := config.loadConnectionURLs(); err != nil {
		return nil, err
	}

	// Load writer detection settings
	config.loadWriterDetectionSettings()

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

// loadConnectionURLs loads database connection URLs from environment
func (c *ClusterConfig) loadConnectionURLs() error {
	// Primary writer URL
	c.WriterURL = os.Getenv("DATABASE_WRITER_URL")
	if c.WriterURL == "" {
		// Fallback to single database URL
		c.WriterURL = os.Getenv("DATABASE_URL")
	}

	// Read replica URLs
	if readerURLs := os.Getenv("DATABASE_READER_URLS"); readerURLs != "" {
		c.ReaderURLs = strings.Split(readerURLs, ",")
		for i, url := range c.ReaderURLs {
			c.ReaderURLs[i] = strings.TrimSpace(url)
		}
	}

	// Alternative: individual reader URLs
	for i := 1; i <= 10; i++ { // Support up to 10 readers
		key := fmt.Sprintf("DATABASE_READER_%d_URL", i)
		if url := os.Getenv(key); url != "" {
			c.ReaderURLs = append(c.ReaderURLs, url)
		}
	}

	// Build from components if no URLs provided
	if c.WriterURL == "" {
		if url := c.buildURLFromComponents(""); url != "" {
			c.WriterURL = url
		}
	}

	if len(c.ReaderURLs) == 0 {
		// Check for reader components
		for i := 1; i <= 5; i++ {
			suffix := fmt.Sprintf("_%d", i)
			if url := c.buildURLFromComponents(suffix); url != "" {
				c.ReaderURLs = append(c.ReaderURLs, url)
			}
		}
	}

	if c.WriterURL == "" {
		return fmt.Errorf("no database writer URL configured")
	}

	return nil
}

// buildURLFromComponents builds a PostgreSQL URL from environment components
func (c *ClusterConfig) buildURLFromComponents(suffix string) string {
	host := os.Getenv("DB_HOST" + suffix)
	if host == "" {
		host = os.Getenv("DB_HOST")
		if host == "" {
			if suffix == "" {
				host = "localhost"
			} else {
				return "" // No host for additional readers
			}
		}
	}

	port := os.Getenv("DB_PORT" + suffix)
	if port == "" {
		port = os.Getenv("DB_PORT")
		if port == "" {
			port = "5432"
		}
	}

	user := os.Getenv("DB_USER" + suffix)
	if user == "" {
		user = os.Getenv("DB_USER")
		if user == "" {
			user = "postgres"
		}
	}

	password := os.Getenv("DB_PASSWORD" + suffix)
	if password == "" {
		password = os.Getenv("DB_PASSWORD")
		if password == "" {
			password = "dev_password"
		}
	}

	dbname := os.Getenv("DB_NAME" + suffix)
	if dbname == "" {
		dbname = os.Getenv("DB_NAME")
		if dbname == "" {
			dbname = "postgres"
		}
	}

	sslmode := os.Getenv("DB_SSL_MODE" + suffix)
	if sslmode == "" {
		sslmode = os.Getenv("DB_SSL_MODE")
		if sslmode == "" {
			sslmode = "disable"
		}
	}

	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		user, password, host, port, dbname, sslmode)
}

// loadWriterDetectionSettings loads writer detection configuration
func (c *ClusterConfig) loadWriterDetectionSettings() {
	strategy := strings.ToLower(os.Getenv("DB_WRITER_DETECTION_STRATEGY"))
	switch strategy {
	case "dns":
		c.WriterDetectionStrategy = StrategyDNS
		c.WriterDNSName = os.Getenv("DB_WRITER_DNS_NAME")
	case "query":
		c.WriterDetectionStrategy = StrategyQuery
		c.WriterDetectionQuery = os.Getenv("DB_WRITER_DETECTION_QUERY")
	case "probe":
		c.WriterDetectionStrategy = StrategyProbe
	case "config", "":
		c.WriterDetectionStrategy = StrategyConfig
	default:
		c.WriterDetectionStrategy = StrategyConfig
	}
}

// loadConnectionPoolSettings loads connection pool configuration
func (c *ClusterConfig) loadConnectionPoolSettings() {
	if maxOpen := os.Getenv("DB_MAX_OPEN_CONNS"); maxOpen != "" {
		if val, err := strconv.Atoi(maxOpen); err == nil && val > 0 {
			c.MaxOpenConns = val
		}
	}

	if maxIdle := os.Getenv("DB_MAX_IDLE_CONNS"); maxIdle != "" {
		if val, err := strconv.Atoi(maxIdle); err == nil && val > 0 {
			c.MaxIdleConns = val
		}
	}

	if lifetime := os.Getenv("DB_CONN_MAX_LIFETIME"); lifetime != "" {
		if val, err := time.ParseDuration(lifetime); err == nil {
			c.ConnMaxLifetime = val
		}
	}

	if idleTime := os.Getenv("DB_CONN_MAX_IDLE_TIME"); idleTime != "" {
		if val, err := time.ParseDuration(idleTime); err == nil {
			c.ConnMaxIdleTime = val
		}
	}
}

// loadHealthSettings loads health check and retry configuration
func (c *ClusterConfig) loadHealthSettings() {
	if interval := os.Getenv("DB_HEALTH_CHECK_INTERVAL"); interval != "" {
		if val, err := time.ParseDuration(interval); err == nil {
			c.HealthCheckInterval = val
		}
	}

	if attempts := os.Getenv("DB_RETRY_ATTEMPTS"); attempts != "" {
		if val, err := strconv.Atoi(attempts); err == nil && val > 0 {
			c.RetryAttempts = val
		}
	}

	if backoff := os.Getenv("DB_RETRY_BACKOFF_BASE"); backoff != "" {
		if val, err := time.ParseDuration(backoff); err == nil {
			c.RetryBackoffBase = val
		}
	}

	if maxBackoff := os.Getenv("DB_RETRY_BACKOFF_MAX"); maxBackoff != "" {
		if val, err := time.ParseDuration(maxBackoff); err == nil {
			c.RetryBackoffMax = val
		}
	}
}

// loadCircuitBreakerSettings loads circuit breaker configuration
func (c *ClusterConfig) loadCircuitBreakerSettings() {
	if threshold := os.Getenv("DB_CIRCUIT_BREAKER_THRESHOLD"); threshold != "" {
		if val, err := strconv.Atoi(threshold); err == nil && val > 0 {
			c.CircuitBreakerThreshold = val
		}
	}

	if timeout := os.Getenv("DB_CIRCUIT_BREAKER_TIMEOUT"); timeout != "" {
		if val, err := time.ParseDuration(timeout); err == nil {
			c.CircuitBreakerTimeout = val
		}
	}
}

// loadReadBalancingSettings loads read balancing configuration
func (c *ClusterConfig) loadReadBalancingSettings() {
	strategy := os.Getenv("DB_READ_BALANCE_STRATEGY")
	validStrategies := []string{"round_robin", "random", "priority", "least_connections"}

	for _, valid := range validStrategies {
		if strategy == valid {
			c.ReadBalanceStrategy = strategy
			return
		}
	}

	// Default strategy
	c.ReadBalanceStrategy = "round_robin"
}

// setDefaults sets default values for unset configuration
func (c *ClusterConfig) setDefaults() {
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = 25
	}
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = 10
	}
	if c.ConnMaxLifetime == 0 {
		c.ConnMaxLifetime = 5 * time.Minute
	}
	if c.ConnMaxIdleTime == 0 {
		c.ConnMaxIdleTime = 30 * time.Second
	}
	if c.HealthCheckInterval == 0 {
		c.HealthCheckInterval = 30 * time.Second
	}
	if c.RetryAttempts == 0 {
		c.RetryAttempts = 3
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
	if c.ReadBalanceStrategy == "" {
		c.ReadBalanceStrategy = "round_robin"
	}
	if c.DriverName == "" {
		c.DriverName = DefaultDriverName
	}
}

// Validate ensures the configuration is valid
func (c *ClusterConfig) Validate() error {
	if c.WriterURL == "" {
		return fmt.Errorf("writer URL is required")
	}

	if c.ServiceName == "" {
		return fmt.Errorf("service name is required")
	}

	if c.MaxOpenConns <= 0 {
		return fmt.Errorf("max open connections must be positive")
	}

	if c.MaxIdleConns < 0 || c.MaxIdleConns > c.MaxOpenConns {
		return fmt.Errorf("max idle connections must be between 0 and max open connections")
	}

	if c.HealthCheckInterval <= 0 {
		return fmt.Errorf("health check interval must be positive")
	}

	if c.RetryAttempts <= 0 {
		return fmt.Errorf("retry attempts must be positive")
	}

	validStrategies := []string{"round_robin", "random", "priority", "least_connections"}
	validStrategy := false
	for _, strategy := range validStrategies {
		if c.ReadBalanceStrategy == strategy {
			validStrategy = true
			break
		}
	}
	if !validStrategy {
		return fmt.Errorf("invalid read balance strategy: %s", c.ReadBalanceStrategy)
	}

	return nil
}

// String returns a string representation of the configuration
func (c *ClusterConfig) String() string {
	return fmt.Sprintf("ClusterConfig{WriterURL: %s, Readers: %d, Strategy: %s, MaxConns: %d, HealthCheck: %v}",
		maskURL(c.WriterURL), len(c.ReaderURLs), c.ReadBalanceStrategy, c.MaxOpenConns, c.HealthCheckInterval)
}

// maskURL masks sensitive information in database URLs
func maskURL(url string) string {
	if strings.Contains(url, "://") && strings.Contains(url, "@") {
		parts := strings.Split(url, "@")
		if len(parts) >= 2 {
			schemeParts := strings.Split(parts[0], "://")
			if len(schemeParts) >= 2 {
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

// Single Database (Development)
const DevEnvExample = `
# Single database for development
DATABASE_URL=postgres://app_user:dev_password@localhost:5432/appdb?sslmode=disable
DB_MAX_OPEN_CONNS=10
DB_MAX_IDLE_CONNS=5
DB_HEALTH_CHECK_INTERVAL=30s
`

// Primary/Replica Cluster (Production)
const ProdClusterExample = `
# Writer/Reader cluster for production
DATABASE_WRITER_URL=postgres://app_user:prod_password@db-writer.example.com:5432/appdb?sslmode=require
DATABASE_READER_URLS=postgres://app_user:prod_password@db-reader1.example.com:5432/appdb?sslmode=require,postgres://app_user:prod_password@db-reader2.example.com:5432/appdb?sslmode=require

# Writer detection via DNS
DB_WRITER_DETECTION_STRATEGY=dns
DB_WRITER_DNS_NAME=db-writer.example.com

# Connection pooling
DB_MAX_OPEN_CONNS=25
DB_MAX_IDLE_CONNS=10
DB_CONN_MAX_LIFETIME=5m
DB_CONN_MAX_IDLE_TIME=30s

# Health and retry settings
DB_HEALTH_CHECK_INTERVAL=30s
DB_RETRY_ATTEMPTS=3
DB_RETRY_BACKOFF_BASE=1s
DB_RETRY_BACKOFF_MAX=30s

# Circuit breaker
DB_CIRCUIT_BREAKER_THRESHOLD=5
DB_CIRCUIT_BREAKER_TIMEOUT=60s

# Read balancing
DB_READ_BALANCE_STRATEGY=round_robin
`

// Query-Based Detection (Cloud RDS/Aurora)
const CloudRDSExample = `
# AWS RDS/Aurora cluster
DATABASE_WRITER_URL=postgres://user:pass@aurora-cluster.cluster-xyz.us-east-1.rds.amazonaws.com:5432/dbname
DATABASE_READER_URLS=postgres://user:pass@aurora-cluster.cluster-ro-xyz.us-east-1.rds.amazonaws.com:5432/dbname

# Writer detection via SQL query
DB_WRITER_DETECTION_STRATEGY=query
DB_WRITER_DETECTION_QUERY=SELECT NOT pg_is_in_recovery();

# Optimized for cloud
DB_MAX_OPEN_CONNS=20
DB_CONN_MAX_LIFETIME=10m
DB_HEALTH_CHECK_INTERVAL=60s
DB_READ_BALANCE_STRATEGY=least_connections
`
