// Package testkit is the generic test toolkit: infrastructure-free suites,
// HTTP mocks, assertion helpers and the integration-test gate. Event-bus
// helpers live in testkit/eventtest and container fixtures in
// testkit/containers; the platform-composite suites remain in
// shared/testing.
package testkit

import (
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

// RunTestSuite runs a testify suite.
func RunTestSuite(t *testing.T, testSuite suite.TestingSuite) {
	suite.Run(t, testSuite)
}

// ShouldRunIntegrationTests reports whether integration tests that require
// external infrastructure (Docker, Postgres, RabbitMQ) should run.
// Enable them with INTEGRATION_TESTS=true; they never run in -short mode.
func ShouldRunIntegrationTests() bool {
	if testing.Short() {
		return false
	}
	return os.Getenv("INTEGRATION_TESTS") == "true"
}
