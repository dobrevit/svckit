package containers

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver for database/sql
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestContainers manages Docker containers for testing
type TestContainers struct {
	PostgresContainer testcontainers.Container
	RedisContainer    testcontainers.Container
	RabbitMQContainer testcontainers.Container
	ctx               context.Context
}

// NewTestContainers creates and starts test containers with the default
// images and startup timeout.
func NewTestContainers(ctx context.Context) (*TestContainers, error) {
	return NewTestContainersWithConfig(ctx, nil)
}

// startPostgres starts a PostgreSQL test container
func (tc *TestContainers) startPostgres(config *TestContainerConfig) error {
	req := testcontainers.ContainerRequest{
		Image:        config.PostgresImage,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_DB":       "test_db",
			"POSTGRES_USER":     "test_user",
			"POSTGRES_PASSWORD": "test_password",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(config.StartTimeout),
	}

	container, err := testcontainers.GenericContainer(tc.ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return fmt.Errorf("failed to start postgres container: %w", err)
	}

	tc.PostgresContainer = container
	return nil
}

// startRedis starts a Redis test container
func (tc *TestContainers) startRedis(config *TestContainerConfig) error {
	req := testcontainers.ContainerRequest{
		Image:        config.RedisImage,
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(config.StartTimeout),
	}

	container, err := testcontainers.GenericContainer(tc.ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return fmt.Errorf("failed to start redis container: %w", err)
	}

	tc.RedisContainer = container
	return nil
}

// startRabbitMQ starts a RabbitMQ test container
func (tc *TestContainers) startRabbitMQ(config *TestContainerConfig) error {
	req := testcontainers.ContainerRequest{
		Image:        config.RabbitMQImage,
		ExposedPorts: []string{"5672/tcp", "15672/tcp"},
		Env: map[string]string{
			"RABBITMQ_DEFAULT_USER": "test_user",
			"RABBITMQ_DEFAULT_PASS": "test_password",
		},
		WaitingFor: wait.ForListeningPort("5672/tcp").WithStartupTimeout(config.StartTimeout),
	}

	container, err := testcontainers.GenericContainer(tc.ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return fmt.Errorf("failed to start rabbitmq container: %w", err)
	}

	tc.RabbitMQContainer = container
	return nil
}

// OpenPostgres opens a connection pool to the test PostgreSQL container. The
// caller closes it.
func (tc *TestContainers) OpenPostgres() (*sql.DB, error) {
	if tc.PostgresContainer == nil {
		return nil, fmt.Errorf("postgres container not started")
	}

	host, err := tc.PostgresContainer.Host(tc.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get postgres host: %w", err)
	}

	mappedPort, err := tc.PostgresContainer.MappedPort(tc.ctx, "5432")
	if err != nil {
		return nil, fmt.Errorf("failed to get postgres port: %w", err)
	}

	dsn := fmt.Sprintf("host=%s port=%s user=test_user password=test_password dbname=test_db sslmode=disable",
		host, mappedPort.Port())

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to test database: %w", err)
	}
	if err := db.PingContext(tc.ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to reach test database: %w", err)
	}

	return db, nil
}

// GetPostgresConnectionString returns a connection string for the test PostgreSQL container
func (tc *TestContainers) GetPostgresConnectionString() (string, error) {
	if tc.PostgresContainer == nil {
		return "", fmt.Errorf("postgres container not started")
	}

	host, err := tc.PostgresContainer.Host(tc.ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get postgres host: %w", err)
	}

	mappedPort, err := tc.PostgresContainer.MappedPort(tc.ctx, "5432")
	if err != nil {
		return "", fmt.Errorf("failed to get postgres port: %w", err)
	}

	return fmt.Sprintf("host=%s port=%s user=test_user password=test_password dbname=test_db sslmode=disable",
		host, mappedPort.Port()), nil
}

// GetRedisConnectionString returns a connection string for the test Redis container
func (tc *TestContainers) GetRedisConnectionString() (string, error) {
	if tc.RedisContainer == nil {
		return "", fmt.Errorf("redis container not started")
	}

	host, err := tc.RedisContainer.Host(tc.ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get redis host: %w", err)
	}

	mappedPort, err := tc.RedisContainer.MappedPort(tc.ctx, "6379")
	if err != nil {
		return "", fmt.Errorf("failed to get redis port: %w", err)
	}

	return fmt.Sprintf("redis://%s:%s", host, mappedPort.Port()), nil
}

// GetRabbitMQConnectionString returns a connection string for the test RabbitMQ container
func (tc *TestContainers) GetRabbitMQConnectionString() (string, error) {
	if tc.RabbitMQContainer == nil {
		return "", fmt.Errorf("rabbitmq container not started")
	}

	host, err := tc.RabbitMQContainer.Host(tc.ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get rabbitmq host: %w", err)
	}

	mappedPort, err := tc.RabbitMQContainer.MappedPort(tc.ctx, "5672")
	if err != nil {
		return "", fmt.Errorf("failed to get rabbitmq port: %w", err)
	}

	return fmt.Sprintf("amqp://test_user:test_password@%s:%s/", host, mappedPort.Port()), nil
}

// WaitForPostgres waits for PostgreSQL to be ready for connections
func (tc *TestContainers) WaitForPostgres() error {
	if tc.PostgresContainer == nil {
		return fmt.Errorf("postgres container not started")
	}

	connStr, err := tc.GetPostgresConnectionString()
	if err != nil {
		return err
	}

	// Wait for database to be ready
	for i := 0; i < 30; i++ {
		db, err := sql.Open("postgres", connStr)
		if err == nil {
			pingErr := db.Ping()
			_ = db.Close()
			if pingErr == nil {
				return nil
			}
		}
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("timeout waiting for postgres to be ready")
}

// WaitForRedis waits for Redis to be ready for connections
func (tc *TestContainers) WaitForRedis() error {
	if tc.RedisContainer == nil {
		return fmt.Errorf("redis container not started")
	}

	// Redis container already waits for the port to be listening
	// Additional health checks can be added here if needed
	return nil
}

// WaitForRabbitMQ waits for RabbitMQ to be ready for connections
func (tc *TestContainers) WaitForRabbitMQ() error {
	if tc.RabbitMQContainer == nil {
		return fmt.Errorf("rabbitmq container not started")
	}

	// RabbitMQ container already waits for the port to be listening
	// Additional health checks can be added here if needed
	return nil
}

// GetContainerLogs retrieves logs from a specific container
func (tc *TestContainers) GetContainerLogs(containerName string) (string, error) {
	var container testcontainers.Container

	switch containerName {
	case "postgres":
		container = tc.PostgresContainer
	case "redis":
		container = tc.RedisContainer
	case "rabbitmq":
		container = tc.RabbitMQContainer
	default:
		return "", fmt.Errorf("unknown container: %s", containerName)
	}

	if container == nil {
		return "", fmt.Errorf("container %s not started", containerName)
	}

	logs, err := container.Logs(tc.ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get logs for %s: %w", containerName, err)
	}
	defer func() { _ = logs.Close() }()

	logBytes, err := io.ReadAll(logs)
	if err != nil {
		return "", fmt.Errorf("failed to read logs for %s: %w", containerName, err)
	}

	return string(logBytes), nil
}

// ExecInContainer executes a command in a specific container
func (tc *TestContainers) ExecInContainer(containerName string, cmd []string) (string, error) {
	var container testcontainers.Container

	switch containerName {
	case "postgres":
		container = tc.PostgresContainer
	case "redis":
		container = tc.RedisContainer
	case "rabbitmq":
		container = tc.RabbitMQContainer
	default:
		return "", fmt.Errorf("unknown container: %s", containerName)
	}

	if container == nil {
		return "", fmt.Errorf("container %s not started", containerName)
	}

	exitCode, reader, err := container.Exec(tc.ctx, cmd)
	if err != nil {
		return "", fmt.Errorf("failed to execute command in %s: %w", containerName, err)
	}

	if exitCode != 0 {
		return "", fmt.Errorf("command failed with exit code %d", exitCode)
	}

	output, err := io.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("failed to read command output: %w", err)
	}

	return string(output), nil
}

// LoadSQLFixture loads SQL fixture files into the test database
func (tc *TestContainers) LoadSQLFixture(fixturePath string) error {
	sqlDB, err := tc.OpenPostgres()
	if err != nil {
		return fmt.Errorf("failed to get database connection: %w", err)
	}
	defer func() { _ = sqlDB.Close() }()

	// Read fixture file
	fixtureData, err := os.ReadFile(fixturePath)
	if err != nil {
		return fmt.Errorf("failed to read fixture file %s: %w", fixturePath, err)
	}

	// Execute fixture SQL
	if _, err := sqlDB.Exec(string(fixtureData)); err != nil {
		return fmt.Errorf("failed to execute fixture SQL: %w", err)
	}

	return nil
}

// LoadFixturesFromDirectory loads all SQL fixture files from a directory
func (tc *TestContainers) LoadFixturesFromDirectory(fixtureDir string) error {
	return filepath.Walk(fixtureDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && filepath.Ext(path) == ".sql" {
			if err := tc.LoadSQLFixture(path); err != nil {
				return fmt.Errorf("failed to load fixture %s: %w", path, err)
			}
		}

		return nil
	})
}

// CleanupDatabase truncates all tables in the test database
func (tc *TestContainers) CleanupDatabase() error {
	sqlDB, err := tc.OpenPostgres()
	if err != nil {
		return fmt.Errorf("failed to get database connection: %w", err)
	}
	defer func() { _ = sqlDB.Close() }()

	// Get all table names
	rows, err := sqlDB.Query(`
		SELECT tablename 
		FROM pg_tables 
		WHERE schemaname = 'public'
	`)
	if err != nil {
		return fmt.Errorf("failed to query table names: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}
	// A failure part-way through iteration leaves a short list, which would
	// silently skip truncating whatever came after it.
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to read table names: %w", err)
	}

	// Truncate all tables
	for _, table := range tables {
		if _, err := sqlDB.Exec(fmt.Sprintf("TRUNCATE TABLE %s CASCADE", table)); err != nil {
			return fmt.Errorf("failed to truncate table %s: %w", table, err)
		}
	}

	return nil
}

// Close stops and removes all test containers
func (tc *TestContainers) Close() error {
	var errors []error

	containers := []struct {
		name      string
		container testcontainers.Container
	}{
		{"postgres", tc.PostgresContainer},
		{"redis", tc.RedisContainer},
		{"rabbitmq", tc.RabbitMQContainer},
	}

	for _, c := range containers {
		if c.container != nil {
			if err := c.container.Terminate(tc.ctx); err != nil {
				errors = append(errors, fmt.Errorf("failed to terminate %s container: %w", c.name, err))
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("errors during container cleanup: %v", errors)
	}

	return nil
}

// TestContainerConfig holds configuration for test containers
type TestContainerConfig struct {
	PostgresImage string
	RedisImage    string
	RabbitMQImage string
	StartTimeout  time.Duration
}

// DefaultTestContainerConfig returns default configuration for test containers
func DefaultTestContainerConfig() *TestContainerConfig {
	return &TestContainerConfig{
		PostgresImage: "postgres:15-alpine",
		RedisImage:    "redis:7-alpine",
		RabbitMQImage: "rabbitmq:3-management-alpine",
		StartTimeout:  60 * time.Second,
	}
}

// NewTestContainersWithConfig creates and starts test containers using the
// given images and startup timeout. A nil config, or any zero field within
// one, falls back to the defaults.
func NewTestContainersWithConfig(ctx context.Context, config *TestContainerConfig) (*TestContainers, error) {
	config = config.withDefaults()

	containers := &TestContainers{ctx: ctx}

	if err := containers.startPostgres(config); err != nil {
		return nil, fmt.Errorf("failed to start postgres container: %w", err)
	}

	if err := containers.startRedis(config); err != nil {
		return nil, fmt.Errorf("failed to start redis container: %w", err)
	}

	if err := containers.startRabbitMQ(config); err != nil {
		return nil, fmt.Errorf("failed to start rabbitmq container: %w", err)
	}

	return containers, nil
}

// withDefaults fills in any field the caller left zero, so a config naming
// only the Postgres image still gets working defaults for the rest.
func (c *TestContainerConfig) withDefaults() *TestContainerConfig {
	defaults := DefaultTestContainerConfig()
	if c == nil {
		return defaults
	}

	filled := *c
	if filled.PostgresImage == "" {
		filled.PostgresImage = defaults.PostgresImage
	}
	if filled.RedisImage == "" {
		filled.RedisImage = defaults.RedisImage
	}
	if filled.RabbitMQImage == "" {
		filled.RabbitMQImage = defaults.RabbitMQImage
	}
	if filled.StartTimeout == 0 {
		filled.StartTimeout = defaults.StartTimeout
	}
	return &filled
}

// HealthCheck represents a health check for a container service
type HealthCheck struct {
	ServiceName string
	CheckFunc   func(*TestContainers) error
	Timeout     time.Duration
	Interval    time.Duration
}

// RunHealthChecks runs health checks for all containers
func (tc *TestContainers) RunHealthChecks() error {
	checks := []HealthCheck{
		{
			ServiceName: "postgres",
			CheckFunc:   (*TestContainers).WaitForPostgres,
			Timeout:     30 * time.Second,
			Interval:    1 * time.Second,
		},
		{
			ServiceName: "redis",
			CheckFunc:   (*TestContainers).WaitForRedis,
			Timeout:     15 * time.Second,
			Interval:    1 * time.Second,
		},
		{
			ServiceName: "rabbitmq",
			CheckFunc:   (*TestContainers).WaitForRabbitMQ,
			Timeout:     30 * time.Second,
			Interval:    1 * time.Second,
		},
	}

	for _, check := range checks {
		if err := check.CheckFunc(tc); err != nil {
			return fmt.Errorf("health check failed for %s: %w", check.ServiceName, err)
		}
	}

	return nil
}
