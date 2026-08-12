package testkit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/dobrevit/svckit/httpclient"
)

type LiteTestSuite struct {
	suite.Suite
	HTTPClient   *httpclient.Client
	TestTimeout  time.Duration
	CleanupFuncs []func()
}

// SetupSuite initializes lightweight test infrastructure
func (s *LiteTestSuite) SetupSuite() {
	s.TestTimeout = 30 * time.Second
	s.CleanupFuncs = make([]func(), 0)

	// Setup HTTP client only
	s.setupHTTPClient()
}

// TearDownSuite cleans up test infrastructure
func (s *LiteTestSuite) TearDownSuite() {
	// Run cleanup functions in reverse order
	for i := len(s.CleanupFuncs) - 1; i >= 0; i-- {
		s.CleanupFuncs[i]()
	}
}

// SetupTest runs before each test
func (s *LiteTestSuite) SetupTest() {
	// Lightweight setup - no database transactions needed
}

// TearDownTest runs after each test
func (s *LiteTestSuite) TearDownTest() {
	// Lightweight cleanup - no database rollback needed
}

// setupHTTPClient initializes HTTP client for API testing
func (s *LiteTestSuite) setupHTTPClient() {
	s.HTTPClient = httpclient.NewClient(httpclient.Config{
		BaseURL:     "http://localhost",
		ServiceName: "test-client",
		Timeout:     10 * time.Second,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	})
}

// WithTimeout runs a function with the test timeout
func (s *LiteTestSuite) WithTimeout(fn func()) {
	done := make(chan bool, 1)

	go func() {
		fn()
		done <- true
	}()

	select {
	case <-done:
		// Function completed within timeout
	case <-time.After(s.TestTimeout):
		s.Fail("Test timed out after %v", s.TestTimeout)
	}
}

// The old CreateTempFile/TempFile helper was removed: its WriteString was a
// no-op that reported success. Use testing.T.TempDir with os.WriteFile.

// RunLiteTestSuite runs a test suite with the lite framework
func RunLiteTestSuite(t *testing.T, testSuite suite.TestingSuite) {
	suite.Run(t, testSuite)
}
