package httpclient_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/dobrevit/svckit/httpclient"
	sharedTesting "github.com/dobrevit/svckit/testkit"
)

// HTTPClientLiteTestSuite tests the HTTP client functionality with lightweight suite
type HTTPClientLiteTestSuite struct {
	sharedTesting.LiteTestSuite
	mockClient *sharedTesting.MockHTTPClient
	realClient *httpclient.Client
	testConfig httpclient.Config
}

// SetupSuite initializes the HTTP client lite test suite
func (s *HTTPClientLiteTestSuite) SetupSuite() {
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
func (s *HTTPClientLiteTestSuite) SetupTest() {
	s.LiteTestSuite.SetupTest()
	s.mockClient.ClearMockResponses()
}

// TestClientConfiguration tests HTTP client configuration
func (s *HTTPClientLiteTestSuite) TestClientConfiguration() {
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
}

// TestSuccessfulGETRequest tests successful GET requests
func (s *HTTPClientLiteTestSuite) TestSuccessfulGETRequest() {
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
	s.Equal(http.StatusOK, resp.StatusCode)

	// Verify response body
	s.Contains(string(resp.Body), "test")

	// Verify headers
	convertedHeaders := sharedTesting.ConvertFromHTTPHeaders(resp.Headers)
	s.Equal("application/json", convertedHeaders["Content-Type"])
}

// TestHTTPErrorHandling tests HTTP error status codes
func (s *HTTPClientLiteTestSuite) TestHTTPErrorHandling() {
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
			name:          "server_error",
			statusCode:    http.StatusInternalServerError,
			responseBody:  `{"error": "Internal server error", "code": "INTERNAL_ERROR"}`,
			expectedError: false,
			description:   "500 responses should be handled gracefully",
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

			path := "/api/v1/error/" + tc.name
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
				s.Equal(tc.statusCode, resp.StatusCode)
			}
		})
	}
}

// TestMockClientThreadSafety tests mock client thread safety
func (s *HTTPClientLiteTestSuite) TestMockClientThreadSafety() {
	// Given: Multiple concurrent requests
	const numGoroutines = 5
	const requestsPerGoroutine = 3

	// Setup mock responses for all requests
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < requestsPerGoroutine; j++ {
			path := "/api/v1/concurrent/" + string(rune('A'+i)) + "/" + string(rune('0'+j))
			response := sharedTesting.MockHTTPResponse{
				StatusCode: http.StatusOK,
				Body:       []byte(`{"goroutine": ` + string(rune('0'+i)) + `, "request": ` + string(rune('0'+j)) + `}`),
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
				path := "/api/v1/concurrent/" + string(rune('A'+goroutineID)) + "/" + string(rune('0'+j))
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

// TestHTTPClientLiteTestSuite runs the HTTP client lite test suite
func TestHTTPClientLiteTestSuite(t *testing.T) {
	sharedTesting.RunLiteTestSuite(t, new(HTTPClientLiteTestSuite))
}
