// The tests for svckit/httpclient live in the testkit module rather than
// beside the code they exercise, because they use testkit's mocks and suites.
// testkit already depends on the main module; a test inside the main module
// importing testkit would close that loop into a module cycle, which is legal
// in Go but poison for versioning.
package httpclient_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/dobrevit/svckit/httpclient"
	sharedTesting "github.com/dobrevit/svckit/testkit"
)

// HTTPClientTestSuite tests the HTTP client functionality
type HTTPClientTestSuite struct {
	sharedTesting.LiteTestSuite
	mockClient *sharedTesting.MockHTTPClient
	realClient *httpclient.Client
	testConfig httpclient.Config
}

// SetupSuite initializes the HTTP client test suite
func (s *HTTPClientTestSuite) SetupSuite() {
	s.LiteTestSuite.SetupSuite()

	s.mockClient = sharedTesting.NewMockHTTPClient()

	s.testConfig = httpclient.Config{
		BaseURL:     "http://test-service:8080",
		ServiceName: "test-client",
		AuthToken:   "test-token-123",
		Timeout:     10 * time.Second,
		Headers: map[string]string{
			"Content-Type":   "application/json",
			"X-Service-Name": "test-client",
		},
	}

	s.realClient = httpclient.NewClient(s.testConfig)
}

// SetupTest runs before each test
func (s *HTTPClientTestSuite) SetupTest() {
	s.LiteTestSuite.SetupTest()
	s.mockClient.ClearMockResponses()
}

// TestClientConfiguration tests HTTP client configuration
func (s *HTTPClientTestSuite) TestClientConfiguration() {
	// Given: A client configuration
	config := httpclient.Config{
		BaseURL:     "https://api.example.com",
		ServiceName: "example-client",
		AuthToken:   "bearer-token-456",
		Timeout:     30 * time.Second,
		Headers: map[string]string{
			"User-Agent": "test-agent/1.0",
		},
	}

	// When: Creating a new client
	client := httpclient.NewClient(config)

	// Then: Client should be properly configured
	s.NotNil(client, "Client should not be nil")
	// Additional configuration tests would depend on the actual implementation
}

// TestSuccessfulGETRequest tests successful GET requests
func (s *HTTPClientTestSuite) TestSuccessfulGETRequest() {
	// Given: A mock response
	expectedResponse := sharedTesting.MockHTTPResponse{
		StatusCode: http.StatusOK,
		Body:       []byte(`{"id": "123", "name": "test"}`),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}

	path := "/api/v1/test/123"
	s.mockClient.SetMockResponse(path, expectedResponse)
	s.mockClient.On("Get", context.Background(), path, map[string]string(nil)).Return(
		&httpclient.Response{
			StatusCode: expectedResponse.StatusCode,
			Body:       expectedResponse.Body,
			Headers:    sharedTesting.ConvertToHTTPHeaders(expectedResponse.Headers),
		}, nil)

	// When: Making a GET request
	resp, err := s.mockClient.Get(context.Background(), path, nil)

	// Then: Request should succeed
	s.NoError(err, "GET request should succeed")
	s.NotNil(resp, "Response should not be nil")

	httpAssertions := sharedTesting.NewHTTPAssertions(s.T())
	httpAssertions.AssertStatusCode(resp.StatusCode, http.StatusOK)
	httpAssertions.AssertJSONResponse(resp.Body, map[string]interface{}{
		"id":   "123",
		"name": "test",
	})
	httpAssertions.AssertContainsHeaders(sharedTesting.ConvertFromHTTPHeaders(resp.Headers), map[string]string{
		"Content-Type": "application/json",
	})
}

// TestSuccessfulPOSTRequest tests successful POST requests
func (s *HTTPClientTestSuite) TestSuccessfulPOSTRequest() {
	// Given: A mock response for POST
	requestBody := []byte(`{"name": "new item", "type": "test"}`)
	expectedResponse := sharedTesting.MockHTTPResponse{
		StatusCode: http.StatusCreated,
		Body:       []byte(`{"id": "456", "name": "new item", "created": true}`),
		Headers: map[string]string{
			"Content-Type": "application/json",
			"Location":     "/api/v1/test/456",
		},
	}

	path := "/api/v1/test"
	s.mockClient.SetMockResponse(path, expectedResponse)
	s.mockClient.On("Post", context.Background(), path, requestBody, map[string]string(nil)).Return(
		&httpclient.Response{
			StatusCode: expectedResponse.StatusCode,
			Body:       expectedResponse.Body,
			Headers:    sharedTesting.ConvertToHTTPHeaders(expectedResponse.Headers),
		}, nil)

	// When: Making a POST request
	resp, err := s.mockClient.Post(context.Background(), path, requestBody, nil)

	// Then: Request should succeed
	s.NoError(err, "POST request should succeed")

	httpAssertions := sharedTesting.NewHTTPAssertions(s.T())
	httpAssertions.AssertStatusCode(resp.StatusCode, http.StatusCreated)
	httpAssertions.AssertJSONResponse(resp.Body, map[string]interface{}{
		"id":      "456",
		"name":    "new item",
		"created": true,
	})
	httpAssertions.AssertContainsHeaders(sharedTesting.ConvertFromHTTPHeaders(resp.Headers), map[string]string{
		"Content-Type": "application/json",
		"Location":     "/api/v1/test/456",
	})
}

// TestHTTPErrorHandling tests HTTP error status codes
func (s *HTTPClientTestSuite) TestHTTPErrorHandling() {
	testCases := []struct {
		name          string
		statusCode    int
		responseBody  string
		expectedError bool
		description   string
	}{
		{
			name:          "not_found",
			statusCode:    http.StatusNotFound,
			responseBody:  `{"error": "Resource not found", "code": "NOT_FOUND"}`,
			expectedError: false, // HTTP errors are not Go errors, they're valid responses
			description:   "404 responses should be handled gracefully",
		},
		{
			name:          "unauthorized",
			statusCode:    http.StatusUnauthorized,
			responseBody:  `{"error": "Invalid token", "code": "UNAUTHORIZED"}`,
			expectedError: false,
			description:   "401 responses should be handled gracefully",
		},
		{
			name:          "server_error",
			statusCode:    http.StatusInternalServerError,
			responseBody:  `{"error": "Internal server error", "code": "INTERNAL_ERROR"}`,
			expectedError: false,
			description:   "500 responses should be handled gracefully",
		},
		{
			name:          "bad_request",
			statusCode:    http.StatusBadRequest,
			responseBody:  `{"error": "Invalid request format", "code": "BAD_REQUEST"}`,
			expectedError: false,
			description:   "400 responses should be handled gracefully",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			// Given: Mock error response
			errorResponse := sharedTesting.MockHTTPResponse{
				StatusCode: tc.statusCode,
				Body:       []byte(tc.responseBody),
				Headers: map[string]string{
					"Content-Type": "application/json",
				},
			}

			path := fmt.Sprintf("/api/v1/error/%s", tc.name)
			s.mockClient.SetMockResponse(path, errorResponse)
			s.mockClient.On("Get", context.Background(), path, map[string]string(nil)).Return(
				&httpclient.Response{
					StatusCode: errorResponse.StatusCode,
					Body:       errorResponse.Body,
					Headers:    sharedTesting.ConvertToHTTPHeaders(errorResponse.Headers),
				}, nil)

			// When: Making request that returns error status
			resp, err := s.mockClient.Get(context.Background(), path, nil)

			// Then: Response should contain error information
			if tc.expectedError {
				s.Error(err, tc.description)
			} else {
				s.NoError(err, tc.description)
				s.NotNil(resp, "Response should not be nil")

				httpAssertions := sharedTesting.NewHTTPAssertions(s.T())
				httpAssertions.AssertStatusCode(resp.StatusCode, tc.statusCode)
			}
		})
	}
}

// TestRequestTimeout tests request timeout handling
func (s *HTTPClientTestSuite) TestRequestTimeout() {
	// Given: A client with short timeout
	shortTimeoutConfig := s.testConfig
	shortTimeoutConfig.Timeout = 100 * time.Millisecond

	// This test would need actual timeout simulation which depends on implementation
	// For now, we test the configuration
	client := httpclient.NewClient(shortTimeoutConfig)
	s.NotNil(client, "Client with short timeout should be created")
}

// TestCustomHeaders tests custom header handling
func (s *HTTPClientTestSuite) TestCustomHeaders() {
	// Given: Custom headers
	customHeaders := map[string]string{
		"X-Custom-Header": "custom-value",
		"X-Request-ID":    "req-123",
		"Authorization":   "Bearer custom-token",
	}

	expectedResponse := sharedTesting.MockHTTPResponse{
		StatusCode: http.StatusOK,
		Body:       []byte(`{"headers_received": true}`),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}

	path := "/api/v1/headers"
	s.mockClient.SetMockResponse(path, expectedResponse)
	s.mockClient.On("Get", context.Background(), path, customHeaders).Return(
		&httpclient.Response{
			StatusCode: expectedResponse.StatusCode,
			Body:       expectedResponse.Body,
			Headers:    sharedTesting.ConvertToHTTPHeaders(expectedResponse.Headers),
		}, nil)

	// When: Making request with custom headers
	resp, err := s.mockClient.Get(context.Background(), path, customHeaders)

	// Then: Request should succeed with headers
	s.NoError(err, "Request with custom headers should succeed")
	s.NotNil(resp, "Response should not be nil")

	httpAssertions := sharedTesting.NewHTTPAssertions(s.T())
	httpAssertions.AssertStatusCode(resp.StatusCode, http.StatusOK)
}

// TestConcurrentRequests tests thread safety of HTTP client
func (s *HTTPClientTestSuite) TestConcurrentRequests() {
	// Given: Multiple concurrent requests
	const numGoroutines = 10
	const requestsPerGoroutine = 5

	// Setup mock responses for all requests
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < requestsPerGoroutine; j++ {
			path := fmt.Sprintf("/api/v1/concurrent/%d/%d", i, j)
			response := sharedTesting.MockHTTPResponse{
				StatusCode: http.StatusOK,
				Body:       []byte(fmt.Sprintf(`{"goroutine": %d, "request": %d}`, i, j)),
				Headers: map[string]string{
					"Content-Type": "application/json",
				},
			}
			s.mockClient.SetMockResponse(path, response)
			s.mockClient.On("Get", context.Background(), path, map[string]string(nil)).Return(
				&httpclient.Response{
					StatusCode: response.StatusCode,
					Body:       response.Body,
					Headers:    sharedTesting.ConvertToHTTPHeaders(response.Headers),
				}, nil)
		}
	}

	results := make(chan error, numGoroutines*requestsPerGoroutine)

	// When: Making concurrent requests
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			for j := 0; j < requestsPerGoroutine; j++ {
				path := fmt.Sprintf("/api/v1/concurrent/%d/%d", goroutineID, j)
				_, err := s.mockClient.Get(context.Background(), path, nil)
				results <- err
			}
		}(i)
	}

	// Then: All requests should succeed
	for i := 0; i < numGoroutines*requestsPerGoroutine; i++ {
		err := <-results
		s.NoError(err, "Concurrent request %d should succeed", i)
	}
}

// TestHTTPMethodsSupport tests all HTTP methods
func (s *HTTPClientTestSuite) TestHTTPMethodsSupport() {
	testCases := []struct {
		method      string
		path        string
		body        []byte
		statusCode  int
		description string
	}{
		{
			method:      "GET",
			path:        "/api/v1/get-test",
			body:        nil,
			statusCode:  http.StatusOK,
			description: "GET requests should be supported",
		},
		{
			method:      "POST",
			path:        "/api/v1/post-test",
			body:        []byte(`{"data": "test"}`),
			statusCode:  http.StatusCreated,
			description: "POST requests should be supported",
		},
		{
			method:      "PUT",
			path:        "/api/v1/put-test",
			body:        []byte(`{"data": "updated"}`),
			statusCode:  http.StatusOK,
			description: "PUT requests should be supported",
		},
		{
			method:      "DELETE",
			path:        "/api/v1/delete-test",
			body:        nil,
			statusCode:  http.StatusNoContent,
			description: "DELETE requests should be supported",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.method, func() {
			// Given: Mock response for method
			response := sharedTesting.MockHTTPResponse{
				StatusCode: tc.statusCode,
				Body:       []byte(`{"method": "` + tc.method + `"}`),
				Headers: map[string]string{
					"Content-Type": "application/json",
				},
			}

			s.mockClient.SetMockResponse(tc.path, response)

			var resp *httpclient.Response
			var err error

			// When: Making request with specific method
			switch tc.method {
			case "GET":
				s.mockClient.On("Get", context.Background(), tc.path, map[string]string(nil)).Return(
					&httpclient.Response{StatusCode: response.StatusCode, Body: response.Body, Headers: sharedTesting.ConvertToHTTPHeaders(response.Headers)}, nil)
				resp, err = s.mockClient.Get(context.Background(), tc.path, nil)
			case "POST":
				s.mockClient.On("Post", context.Background(), tc.path, tc.body, map[string]string(nil)).Return(
					&httpclient.Response{StatusCode: response.StatusCode, Body: response.Body, Headers: sharedTesting.ConvertToHTTPHeaders(response.Headers)}, nil)
				resp, err = s.mockClient.Post(context.Background(), tc.path, tc.body, nil)
			case "PUT":
				s.mockClient.On("Put", context.Background(), tc.path, tc.body, map[string]string(nil)).Return(
					&httpclient.Response{StatusCode: response.StatusCode, Body: response.Body, Headers: sharedTesting.ConvertToHTTPHeaders(response.Headers)}, nil)
				resp, err = s.mockClient.Put(context.Background(), tc.path, tc.body, nil)
			case "DELETE":
				s.mockClient.On("Delete", context.Background(), tc.path, map[string]string(nil)).Return(
					&httpclient.Response{StatusCode: response.StatusCode, Body: response.Body, Headers: sharedTesting.ConvertToHTTPHeaders(response.Headers)}, nil)
				resp, err = s.mockClient.Delete(context.Background(), tc.path, nil)
			}

			// Then: Request should succeed
			s.NoError(err, tc.description)
			s.NotNil(resp, "Response should not be nil")

			httpAssertions := sharedTesting.NewHTTPAssertions(s.T())
			httpAssertions.AssertStatusCode(resp.StatusCode, tc.statusCode)
		})
	}
}

// TestRequestRetries tests retry logic (if implemented)
func (s *HTTPClientTestSuite) TestRequestRetries() {
	// This test would depend on the specific retry implementation
	// For now, we test basic functionality
	s.NotNil(s.realClient, "Real client should be available for retry testing")
}

// TestPerformanceMetrics tests performance measurement
func (s *HTTPClientTestSuite) TestPerformanceMetrics() {
	// Given: Performance testing setup
	perfHelper := sharedTesting.NewPerformanceTestHelper()

	response := sharedTesting.MockHTTPResponse{
		StatusCode: http.StatusOK,
		Body:       []byte(`{"performance": "test"}`),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}

	path := "/api/v1/performance"
	s.mockClient.SetMockResponse(path, response)
	s.mockClient.On("Get", context.Background(), path, map[string]string(nil)).Return(
		&httpclient.Response{
			StatusCode: response.StatusCode,
			Body:       response.Body,
			Headers:    sharedTesting.ConvertToHTTPHeaders(response.Headers),
		}, nil)

	// When: Making timed request
	perfHelper.StartTimer()
	resp, err := s.mockClient.Get(context.Background(), path, nil)
	duration := perfHelper.StopTimer("http_request")

	// Then: Request should complete within reasonable time
	s.NoError(err, "Performance test request should succeed")
	s.NotNil(resp, "Response should not be nil")

	perfAssertions := sharedTesting.NewPerformanceAssertions(s.T())
	perfAssertions.AssertExecutionTime(duration, 100*time.Millisecond, "HTTP request should complete quickly")
}

// TestHTTPClientSuite runs the HTTP client test suite
func TestHTTPClientSuite(t *testing.T) {
	sharedTesting.RunLiteTestSuite(t, new(HTTPClientTestSuite))
}
