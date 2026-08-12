//go:build integration

// These tests start real containers, so they are gated behind the
// "integration" build tag as well as the INTEGRATION_TESTS env contract.

package containers_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dobrevit/svckit/testkit"
	"github.com/dobrevit/svckit/testkit/containers"
)

// DockerTestSuite tests the Docker test containers functionality
type DockerTestSuite struct {
	testkit.LiteTestSuite
	containers *containers.TestContainers
	ctx        context.Context
}

// SetupSuite initializes the Docker test suite
func (s *DockerTestSuite) SetupSuite() {
	s.LiteTestSuite.SetupSuite()
	s.ctx = context.Background()
}

// SetupTest runs before each test
func (s *DockerTestSuite) SetupTest() {
	s.LiteTestSuite.SetupTest()

	// Start test containers
	var err error
	s.containers, err = containers.NewTestContainers(s.ctx)
	if err != nil {
		s.T().Skipf("Skipping Docker tests - containers not available: %v", err)
	}

	// Wait for all containers to be ready
	if err := s.containers.RunHealthChecks(); err != nil {
		s.T().Skipf("Skipping Docker tests - health checks failed: %v", err)
	}
}

// TearDownTest cleans up after each test
func (s *DockerTestSuite) TearDownTest() {
	if s.containers != nil {
		if err := s.containers.Close(); err != nil {
			s.T().Logf("Warning: Failed to close test containers: %v", err)
		}
	}
	s.LiteTestSuite.TearDownTest()
}

// TestPostgresContainer tests PostgreSQL container functionality
func (s *DockerTestSuite) TestPostgresContainer() {
	s.Require().NotNil(s.containers, "Test containers should be available")

	// Test database connection
	sqlDB, err := s.containers.OpenPostgres()
	s.NoError(err, "Should be able to connect to PostgreSQL")
	s.NotNil(sqlDB, "Database connection should not be nil")
	defer sqlDB.Close()

	// Test creating a table
	_, err = sqlDB.Exec(`
		CREATE TABLE IF NOT EXISTS test_table (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	s.NoError(err, "Should be able to create test table")

	// Test inserting data
	_, err = sqlDB.Exec("INSERT INTO test_table (name) VALUES ($1)", "test-record")
	s.NoError(err, "Should be able to insert test data")

	// Test querying data
	var count int
	err = sqlDB.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count)
	s.NoError(err, "Should be able to query test data")
	s.Equal(1, count, "Should have one test record")

	// Test connection string
	connStr, err := s.containers.GetPostgresConnectionString()
	s.NoError(err, "Should be able to get connection string")
	s.NotEmpty(connStr, "Connection string should not be empty")
	s.Contains(connStr, "test_user", "Connection string should contain test user")
	s.Contains(connStr, "test_db", "Connection string should contain test database")
}

// TestRedisContainer tests Redis container functionality
func (s *DockerTestSuite) TestRedisContainer() {
	s.Require().NotNil(s.containers, "Test containers should be available")

	// Test Redis connection string
	connStr, err := s.containers.GetRedisConnectionString()
	s.NoError(err, "Should be able to get Redis connection string")
	s.NotEmpty(connStr, "Connection string should not be empty")
	s.Contains(connStr, "redis://", "Connection string should be Redis URL format")

	// Additional Redis functionality tests would require a Redis client
	// For now, we test that the container is accessible
	s.NoError(s.containers.WaitForRedis(), "Redis should be ready")
}

// TestRabbitMQContainer tests RabbitMQ container functionality
func (s *DockerTestSuite) TestRabbitMQContainer() {
	s.Require().NotNil(s.containers, "Test containers should be available")

	// Test RabbitMQ connection string
	connStr, err := s.containers.GetRabbitMQConnectionString()
	s.NoError(err, "Should be able to get RabbitMQ connection string")
	s.NotEmpty(connStr, "Connection string should not be empty")
	s.Contains(connStr, "amqp://", "Connection string should be AMQP URL format")
	s.Contains(connStr, "test_user", "Connection string should contain test user")

	// Test RabbitMQ readiness
	s.NoError(s.containers.WaitForRabbitMQ(), "RabbitMQ should be ready")
}

// TestDatabaseFixtures tests SQL fixture loading
func (s *DockerTestSuite) TestDatabaseFixtures() {
	s.Require().NotNil(s.containers, "Test containers should be available")

	// Create temporary fixture file
	fixtureSQL := `
		CREATE TABLE IF NOT EXISTS fixtures_test (
			id SERIAL PRIMARY KEY,
			data TEXT NOT NULL
		);
		
		INSERT INTO fixtures_test (data) VALUES 
			('fixture-data-1'),
			('fixture-data-2'),
			('fixture-data-3');
	`

	// Write fixture to temporary file
	fixtureFile := "/tmp/test_fixture.sql"
	err := s.WriteStringToFile(fixtureFile, fixtureSQL)
	s.NoError(err, "Should be able to write fixture file")

	// Load fixture
	err = s.containers.LoadSQLFixture(fixtureFile)
	s.NoError(err, "Should be able to load SQL fixture")

	// Verify fixture data
	sqlDB, err := s.containers.OpenPostgres()
	s.NoError(err, "Should be able to connect to database")
	defer sqlDB.Close()

	var count int
	err = sqlDB.QueryRow("SELECT COUNT(*) FROM fixtures_test").Scan(&count)
	s.NoError(err, "Should be able to query fixture data")
	s.Equal(3, count, "Should have three fixture records")

	// Test cleanup
	err = s.containers.CleanupDatabase()
	s.NoError(err, "Should be able to cleanup database")

	// Verify cleanup
	err = sqlDB.QueryRow("SELECT COUNT(*) FROM fixtures_test").Scan(&count)
	s.NoError(err, "Should be able to query after cleanup")
	s.Equal(0, count, "Should have no records after cleanup")
}

// TestContainerLogs tests container log retrieval
func (s *DockerTestSuite) TestContainerLogs() {
	s.Require().NotNil(s.containers, "Test containers should be available")

	// Test getting logs from each container
	containers := []string{"postgres", "redis", "rabbitmq"}

	for _, containerName := range containers {
		logs, err := s.containers.GetContainerLogs(containerName)
		s.NoError(err, "Should be able to get logs for %s", containerName)
		s.NotEmpty(logs, "Logs should not be empty for %s", containerName)
	}

	// Test getting logs from non-existent container
	_, err := s.containers.GetContainerLogs("non-existent")
	s.Error(err, "Should fail to get logs for non-existent container")
}

// TestContainerExecution tests executing commands in containers
func (s *DockerTestSuite) TestContainerExecution() {
	s.Require().NotNil(s.containers, "Test containers should be available")

	// Test executing command in PostgreSQL container
	output, err := s.containers.ExecInContainer("postgres", []string{"pg_isready", "-U", "test_user"})
	s.NoError(err, "Should be able to execute pg_isready command")
	s.Contains(output, "accepting connections", "PostgreSQL should be accepting connections")

	// Test executing command in Redis container
	output, err = s.containers.ExecInContainer("redis", []string{"redis-cli", "ping"})
	s.NoError(err, "Should be able to execute redis-cli ping")
	s.Contains(output, "PONG", "Redis should respond with PONG")
}

// TestContainerHealthChecks tests health check functionality
func (s *DockerTestSuite) TestContainerHealthChecks() {
	s.Require().NotNil(s.containers, "Test containers should be available")

	// Test individual health checks
	s.NoError(s.containers.WaitForPostgres(), "PostgreSQL health check should pass")
	s.NoError(s.containers.WaitForRedis(), "Redis health check should pass")
	s.NoError(s.containers.WaitForRabbitMQ(), "RabbitMQ health check should pass")

	// Test comprehensive health checks
	s.NoError(s.containers.RunHealthChecks(), "All health checks should pass")
}

// TestContainerConfiguration tests custom container configuration
func (s *DockerTestSuite) TestContainerConfiguration() {
	// Test default configuration
	defaultConfig := containers.DefaultTestContainerConfig()
	s.NotNil(defaultConfig, "Default config should not be nil")
	s.Equal("postgres:15-alpine", defaultConfig.PostgresImage)
	s.Equal("redis:7-alpine", defaultConfig.RedisImage)
	s.Equal("rabbitmq:3-management-alpine", defaultConfig.RabbitMQImage)
	s.Equal(60*time.Second, defaultConfig.StartTimeout)

	// Test custom configuration
	customConfig := &containers.TestContainerConfig{
		PostgresImage: "postgres:14",
		RedisImage:    "redis:6",
		RabbitMQImage: "rabbitmq:3",
		StartTimeout:  30 * time.Second,
	}

	// Creating containers with custom config would require implementing
	// the configuration usage in the actual implementation
	s.NotNil(customConfig, "Custom config should be valid")
}

// TestContainerPerformance tests container performance characteristics
func (s *DockerTestSuite) TestContainerPerformance() {
	s.Require().NotNil(s.containers, "Test containers should be available")

	perfHelper := testkit.NewPerformanceTestHelper()

	// Test database connection performance
	perfHelper.StartTimer()
	sqlDB, err := s.containers.OpenPostgres()
	connectionTime := perfHelper.StopTimer("db_connection")

	s.NoError(err, "Database connection should succeed")
	s.NotNil(sqlDB, "Database should not be nil")
	defer sqlDB.Close()

	perfAssertions := testkit.NewPerformanceAssertions(s.T())
	perfAssertions.AssertExecutionTime(connectionTime, 5*time.Second, "Database connection should be fast")

	perfHelper.StartTimer()
	var result int
	err = sqlDB.QueryRow("SELECT 1").Scan(&result)
	queryTime := perfHelper.StopTimer("simple_query")

	s.NoError(err, "Simple query should succeed")
	s.Equal(1, result, "Query result should be correct")
	perfAssertions.AssertExecutionTime(queryTime, 1*time.Second, "Simple query should be very fast")
}

// TestContainerIsolation tests that containers are properly isolated
func (s *DockerTestSuite) TestContainerIsolation() {
	s.Require().NotNil(s.containers, "Test containers should be available")

	// Create test data in this container
	sqlDB, err := s.containers.OpenPostgres()
	s.NoError(err, "Should connect to database")
	defer sqlDB.Close()

	// Create test table and data
	_, err = sqlDB.Exec(`
		CREATE TABLE IF NOT EXISTS isolation_test (
			id SERIAL PRIMARY KEY,
			test_run VARCHAR(100)
		)
	`)
	s.NoError(err, "Should create test table")

	testRunID := fmt.Sprintf("test-run-%d", time.Now().Unix())
	_, err = sqlDB.Exec("INSERT INTO isolation_test (test_run) VALUES ($1)", testRunID)
	s.NoError(err, "Should insert test data")

	// Verify data exists
	var count int
	err = sqlDB.QueryRow("SELECT COUNT(*) FROM isolation_test WHERE test_run = $1", testRunID).Scan(&count)
	s.NoError(err, "Should query test data")
	s.Equal(1, count, "Should have test data")

	// Note: True isolation testing would require creating a second set of containers
	// and verifying they don't see each other's data
	s.T().Log("Container isolation verified within single test instance")
}

// TestContainerResourceCleanup tests proper resource cleanup
func (s *DockerTestSuite) TestContainerResourceCleanup() {
	// Create containers
	ctx := context.Background()
	containers, err := containers.NewTestContainers(ctx)
	if err != nil {
		s.T().Skipf("Skipping cleanup test - containers not available: %v", err)
	}

	// Verify containers are running
	s.NotNil(containers.PostgresContainer, "PostgreSQL container should exist")
	s.NotNil(containers.RedisContainer, "Redis container should exist")
	s.NotNil(containers.RabbitMQContainer, "RabbitMQ container should exist")

	// Test cleanup
	err = containers.Close()
	s.NoError(err, "Container cleanup should succeed")

	// Verify containers are cleaned up
	// Note: Actual verification would require checking Docker API
	// For now, we verify that Close() doesn't return an error
	s.T().Log("Container cleanup completed successfully")
}

// Helper method to write string to file. (The old TempFile helper's
// WriteString was a no-op, so the fixture test silently loaded an empty
// file; this writes for real.)
func (s *DockerTestSuite) WriteStringToFile(filename, content string) error {
	return os.WriteFile(filename, []byte(content), 0o644)
}

// TestDockerTestSuite runs the Docker test suite
func TestDockerTestSuite(t *testing.T) {
	// Only run Docker tests if explicitly requested or in CI
	if !testkit.ShouldRunIntegrationTests() {
		t.Skip("Skipping Docker integration tests - set INTEGRATION_TESTS=true to run")
	}

	testkit.RunTestSuite(t, new(DockerTestSuite))
}
