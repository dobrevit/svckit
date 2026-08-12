package amqpcluster

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config represents RabbitMQ cluster configuration
type Config struct {
	Nodes               []string
	ServiceName         string
	HealthCheckInterval time.Duration
	RetryAttempts       int
	LoadBalanceStrategy string

	// Signing configuration
	EventSigningKey string
	SigningRequired bool
}

// LoadConfigFromEnv loads cluster configuration from environment variables
func LoadConfigFromEnv(serviceName string) (*Config, error) {
	config := &Config{
		ServiceName:         serviceName,
		HealthCheckInterval: 30 * time.Second,
		RetryAttempts:       3,
		LoadBalanceStrategy: "round_robin",
		SigningRequired:     true,
	}

	// Load RabbitMQ node URLs
	if err := config.loadNodes(); err != nil {
		return nil, err
	}

	// Load signing configuration
	config.loadSigningConfig()

	// Load optional settings
	config.loadOptionalSettings()

	return config, nil
}

// loadNodes loads RabbitMQ node URLs from environment
func (c *Config) loadNodes() error {
	// Try cluster nodes first
	if nodesEnv := os.Getenv("RABBITMQ_CLUSTER_NODES"); nodesEnv != "" {
		c.Nodes = strings.Split(nodesEnv, ",")
		for i, node := range c.Nodes {
			c.Nodes[i] = strings.TrimSpace(node)
		}
		return nil
	}

	// Fall back to building URLs from components
	urls := []string{}

	// Check for multiple node configurations
	for i := 1; i <= 5; i++ { // Support up to 5 nodes
		var url string

		if i == 1 {
			// First node (no suffix)
			url = c.buildNodeURL("", "")
		} else {
			// Additional nodes (with suffix)
			suffix := fmt.Sprintf("_%d", i)
			url = c.buildNodeURL(suffix, suffix)
		}

		if url != "" {
			urls = append(urls, url)
		}
	}

	if len(urls) == 0 {
		return fmt.Errorf("no RabbitMQ nodes configured. Set RABBITMQ_CLUSTER_NODES or RABBITMQ_URL")
	}

	c.Nodes = urls
	return nil
}

// buildNodeURL builds a RabbitMQ URL from environment variables
func (c *Config) buildNodeURL(hostSuffix, portSuffix string) string {
	// Try complete URL first
	if url := os.Getenv("RABBITMQ_URL" + portSuffix); url != "" {
		return url
	}

	// Build from components
	user := os.Getenv("RABBITMQ_USER" + portSuffix)
	if user == "" {
		user = os.Getenv("RABBITMQ_USER")
		if user == "" {
			user = "guest"
		}
	}

	pass := os.Getenv("RABBITMQ_PASS" + portSuffix)
	if pass == "" {
		pass = os.Getenv("RABBITMQ_PASS")
		if pass == "" {
			pass = "guest"
		}
	}

	host := os.Getenv("RABBITMQ_HOST" + hostSuffix)
	if host == "" {
		host = os.Getenv("RABBITMQ_HOST")
		if host == "" {
			if hostSuffix == "" {
				host = "localhost"
			} else {
				return "" // No host for additional nodes
			}
		}
	}

	port := os.Getenv("RABBITMQ_PORT" + portSuffix)
	if port == "" {
		port = os.Getenv("RABBITMQ_PORT")
		if port == "" {
			port = "5672"
		}
	}

	vhost := os.Getenv("RABBITMQ_VHOST" + portSuffix)
	if vhost == "" {
		vhost = os.Getenv("RABBITMQ_VHOST")
		if vhost == "" {
			vhost = "/"
		}
	}

	return fmt.Sprintf("amqp://%s:%s@%s:%s%s", user, pass, host, port, vhost)
}

// loadSigningConfig loads event signing configuration
func (c *Config) loadSigningConfig() {
	c.EventSigningKey = os.Getenv("EVENT_SIGNING_KEY")
	if c.EventSigningKey == "" {
		c.EventSigningKey = "dev-signing-key-change-in-production"
	}

	if required := os.Getenv("EVENT_SIGNING_REQUIRED"); required != "" {
		c.SigningRequired = strings.ToLower(required) == "true"
	}
}

// loadOptionalSettings loads optional configuration settings
func (c *Config) loadOptionalSettings() {
	// Health check interval
	if intervalStr := os.Getenv("RABBITMQ_HEALTH_CHECK_INTERVAL"); intervalStr != "" {
		if interval, err := time.ParseDuration(intervalStr); err == nil {
			c.HealthCheckInterval = interval
		}
	}

	// Retry attempts
	if attemptsStr := os.Getenv("RABBITMQ_RETRY_ATTEMPTS"); attemptsStr != "" {
		if attempts, err := strconv.Atoi(attemptsStr); err == nil && attempts > 0 {
			c.RetryAttempts = attempts
		}
	}

	// Load balance strategy
	if strategy := os.Getenv("RABBITMQ_LOAD_BALANCE_STRATEGY"); strategy != "" {
		validStrategies := []string{"round_robin", "random", "hash"}
		for _, valid := range validStrategies {
			if strategy == valid {
				c.LoadBalanceStrategy = strategy
				break
			}
		}
	}
}

// Validate ensures the configuration is valid
func (c *Config) Validate() error {
	if len(c.Nodes) == 0 {
		return fmt.Errorf("at least one RabbitMQ node must be configured")
	}

	if c.ServiceName == "" {
		return fmt.Errorf("service name is required")
	}

	if c.HealthCheckInterval <= 0 {
		return fmt.Errorf("health check interval must be positive")
	}

	if c.RetryAttempts < 1 {
		return fmt.Errorf("retry attempts must be at least 1")
	}

	validStrategies := []string{"round_robin", "random", "hash"}
	isValidStrategy := false
	for _, strategy := range validStrategies {
		if c.LoadBalanceStrategy == strategy {
			isValidStrategy = true
			break
		}
	}
	if !isValidStrategy {
		return fmt.Errorf("invalid load balance strategy: %s", c.LoadBalanceStrategy)
	}

	return nil
}

// String returns a string representation of the configuration
func (c *Config) String() string {
	// Mask sensitive information
	maskedNodes := make([]string, len(c.Nodes))
	for i, node := range c.Nodes {
		maskedNodes[i] = maskCredentials(node)
	}

	return fmt.Sprintf("Config{Nodes: %v, Service: %s, HealthCheck: %v, Retries: %d, Strategy: %s, SigningRequired: %t}",
		maskedNodes, c.ServiceName, c.HealthCheckInterval, c.RetryAttempts, c.LoadBalanceStrategy, c.SigningRequired)
}

// maskCredentials masks sensitive information in URLs
func maskCredentials(url string) string {
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

// Example environment configurations for different deployment scenarios

// Development (single node)
const DevEnvExample = `
# Single RabbitMQ node for development
RABBITMQ_URL=amqp://app_user:dev_password@localhost:5672/
EVENT_SIGNING_KEY=dev-signing-key-change-in-production
EVENT_SIGNING_REQUIRED=true
`

// Production (multiple nodes)
const ProdEnvExample = `
# Multiple RabbitMQ nodes for production
RABBITMQ_CLUSTER_NODES=amqp://user:pass@rabbit1:5672/,amqp://user:pass@rabbit2:5672/,amqp://user:pass@rabbit3:5672/
EVENT_SIGNING_KEY=production-signing-key-secure
EVENT_SIGNING_REQUIRED=true
RABBITMQ_HEALTH_CHECK_INTERVAL=30s
RABBITMQ_RETRY_ATTEMPTS=3
RABBITMQ_LOAD_BALANCE_STRATEGY=round_robin
`

// Docker Compose example
const DockerComposeExample = `
# Docker Compose with separate node configurations
RABBITMQ_HOST_1=rabbitmq-1
RABBITMQ_HOST_2=rabbitmq-2  
RABBITMQ_HOST_3=rabbitmq-3
RABBITMQ_USER=platform_user
RABBITMQ_PASS=platform_password
RABBITMQ_PORT=5672
RABBITMQ_VHOST=/platform
EVENT_SIGNING_KEY=${EVENT_SIGNING_KEY}
EVENT_SIGNING_REQUIRED=true
`
