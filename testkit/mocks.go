package testkit

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/dobrevit/svckit/httpclient"
)

type MockHTTPClient struct {
	mock.Mock
	Responses map[string]MockHTTPResponse
	mutex     sync.RWMutex
}

// MockHTTPResponse represents a mock HTTP response
type MockHTTPResponse struct {
	StatusCode int
	Body       []byte
	Headers    map[string]string
	Error      error
}

// ConvertToHTTPHeaders converts simple string map to http.Header format
func ConvertToHTTPHeaders(headers map[string]string) http.Header {
	httpHeaders := make(http.Header)
	for key, value := range headers {
		httpHeaders.Set(key, value)
	}
	return httpHeaders
}

// ConvertFromHTTPHeaders converts http.Header to simple string map (takes first value).
// Keys come back in canonical MIME form (e.g. "X-Request-Id"), as stored by http.Header.
func ConvertFromHTTPHeaders(headers http.Header) map[string]string {
	result := make(map[string]string)
	for key, values := range headers {
		if len(values) > 0 {
			result[key] = values[0]
		}
	}
	return result
}

// NewMockHTTPClient creates a new mock HTTP client
func NewMockHTTPClient() *MockHTTPClient {
	return &MockHTTPClient{
		Responses: make(map[string]MockHTTPResponse),
	}
}

// Get mocks HTTP GET requests
func (m *MockHTTPClient) Get(ctx context.Context, path string, headers map[string]string) (*httpclient.Response, error) {
	args := m.Called(ctx, path, headers)

	// Check if we have a predefined response
	m.mutex.RLock()
	mockResp, exists := m.Responses[path]
	m.mutex.RUnlock()

	if exists {
		if mockResp.Error != nil {
			return nil, mockResp.Error
		}

		return &httpclient.Response{
			StatusCode: mockResp.StatusCode,
			Body:       mockResp.Body,
			Headers:    ConvertToHTTPHeaders(mockResp.Headers),
		}, nil
	}

	// Return mock result
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*httpclient.Response), nil
}

// Post mocks HTTP POST requests
func (m *MockHTTPClient) Post(ctx context.Context, path string, body []byte, headers map[string]string) (*httpclient.Response, error) {
	args := m.Called(ctx, path, body, headers)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*httpclient.Response), nil
}

// Put mocks HTTP PUT requests
func (m *MockHTTPClient) Put(ctx context.Context, path string, body []byte, headers map[string]string) (*httpclient.Response, error) {
	args := m.Called(ctx, path, body, headers)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*httpclient.Response), nil
}

// Delete mocks HTTP DELETE requests
func (m *MockHTTPClient) Delete(ctx context.Context, path string, headers map[string]string) (*httpclient.Response, error) {
	args := m.Called(ctx, path, headers)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*httpclient.Response), nil
}

// SetMockResponse sets a predefined response for a specific path
func (m *MockHTTPClient) SetMockResponse(path string, response MockHTTPResponse) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Responses[path] = response
}

// ClearMockResponses clears all predefined responses
func (m *MockHTTPClient) ClearMockResponses() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Responses = make(map[string]MockHTTPResponse)
}

// TestEventHandler is a testing event handler that collects events
type MockTime struct {
	FrozenTime time.Time
}

// NewMockTime creates a new mock time instance
func NewMockTime() *MockTime {
	return &MockTime{
		FrozenTime: time.Now(),
	}
}

// Now returns the frozen time
func (m *MockTime) Now() time.Time {
	return m.FrozenTime
}

// SetTime sets the frozen time
func (m *MockTime) SetTime(t time.Time) {
	m.FrozenTime = t
}

// AddTime adds duration to the frozen time
func (m *MockTime) AddTime(d time.Duration) {
	m.FrozenTime = m.FrozenTime.Add(d)
}

// TestDatabase provides utilities for database testing
type TestDatabase struct {
	ConnectionString string
	CleanupQueries   []string
}

// NewTestDatabase creates a new test database instance
func NewTestDatabase(connString string) *TestDatabase {
	return &TestDatabase{
		ConnectionString: connString,
		CleanupQueries:   make([]string, 0),
	}
}

// AddCleanupQuery adds a query to run during cleanup
func (db *TestDatabase) AddCleanupQuery(query string) {
	db.CleanupQueries = append(db.CleanupQueries, query)
}

// SecurityTestHelper provides utilities for testing cryptographic security
type PerformanceTestHelper struct {
	StartTime time.Time
	Metrics   map[string]time.Duration
}

// NewPerformanceTestHelper creates performance testing utilities
func NewPerformanceTestHelper() *PerformanceTestHelper {
	return &PerformanceTestHelper{
		Metrics: make(map[string]time.Duration),
	}
}

// StartTimer starts a performance timer
func (h *PerformanceTestHelper) StartTimer() {
	h.StartTime = time.Now()
}

// StopTimer stops the timer and records the metric
func (h *PerformanceTestHelper) StopTimer(metricName string) time.Duration {
	duration := time.Since(h.StartTime)
	h.Metrics[metricName] = duration
	return duration
}

// GetMetric returns a recorded metric
func (h *PerformanceTestHelper) GetMetric(metricName string) time.Duration {
	return h.Metrics[metricName]
}

// AssertPerformance asserts that a metric is within expected bounds
func (h *PerformanceTestHelper) AssertPerformance(metricName string, maxDuration time.Duration) bool {
	metric := h.Metrics[metricName]
	return metric <= maxDuration
}
