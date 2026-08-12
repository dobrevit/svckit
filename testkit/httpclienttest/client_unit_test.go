package httpclient_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dobrevit/svckit/httpclient"
	sharedTesting "github.com/dobrevit/svckit/testkit"
)

// TestHTTPClientConfiguration tests HTTP client configuration without BaseTestSuite
func TestHTTPClientConfiguration(t *testing.T) {
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
	assert.NotNil(t, client, "Client should not be nil")
}

// TestMockHTTPClientBasic tests basic mock HTTP client functionality
func TestMockHTTPClientBasic(t *testing.T) {
	// Given: A mock HTTP client
	mockClient := sharedTesting.NewMockHTTPClient()
	require.NotNil(t, mockClient)

	// Given: A mock response
	expectedResponse := sharedTesting.MockHTTPResponse{
		StatusCode: http.StatusOK,
		Body:       []byte(`{"id": "123", "name": "test"}`),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}

	path := "/api/v1/test/123"
	mockClient.SetMockResponse(path, expectedResponse)
	mockClient.On("Get", context.Background(), path, map[string]string(nil)).Return(
		&httpclient.Response{
			StatusCode: expectedResponse.StatusCode,
			Body:       expectedResponse.Body,
			Headers:    sharedTesting.ConvertToHTTPHeaders(expectedResponse.Headers),
		}, nil)

	// When: Making a GET request
	resp, err := mockClient.Get(context.Background(), path, nil)

	// Then: Request should succeed
	assert.NoError(t, err, "GET request should succeed")
	assert.NotNil(t, resp, "Response should not be nil")
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify headers
	convertedHeaders := sharedTesting.ConvertFromHTTPHeaders(resp.Headers)
	assert.Equal(t, "application/json", convertedHeaders["Content-Type"])

	mockClient.AssertExpectations(t)
}

// TestMockHTTPErrorHandling tests error handling without BaseTestSuite
func TestMockHTTPErrorHandling(t *testing.T) {
	mockClient := sharedTesting.NewMockHTTPClient()

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
		t.Run(tc.name, func(t *testing.T) {
			// Given: Mock error response
			errorResponse := sharedTesting.MockHTTPResponse{
				StatusCode: tc.statusCode,
				Body:       []byte(tc.responseBody),
				Headers: map[string]string{
					"Content-Type": "application/json",
				},
			}

			path := fmt.Sprintf("/api/v1/error/%s", tc.name)
			mockClient.SetMockResponse(path, errorResponse)
			mockClient.On("Get", context.Background(), path, map[string]string(nil)).Return(
				&httpclient.Response{
					StatusCode: errorResponse.StatusCode,
					Body:       errorResponse.Body,
					Headers:    sharedTesting.ConvertToHTTPHeaders(errorResponse.Headers),
				}, nil)

			// When: Making request that returns error status
			resp, err := mockClient.Get(context.Background(), path, nil)

			// Then: Response should contain error information
			if tc.expectedError {
				assert.Error(t, err, tc.description)
			} else {
				assert.NoError(t, err, tc.description)
				assert.NotNil(t, resp, "Response should not be nil")
				assert.Equal(t, tc.statusCode, resp.StatusCode)
			}
		})
	}
}

// TestHTTPClientConcurrency tests concurrent access
func TestHTTPClientConcurrency(t *testing.T) {
	mockClient := sharedTesting.NewMockHTTPClient()
	const numGoroutines = 5

	// Setup mock responses
	for i := 0; i < numGoroutines; i++ {
		path := fmt.Sprintf("/api/v1/concurrent/%d", i)
		response := sharedTesting.MockHTTPResponse{
			StatusCode: http.StatusOK,
			Body:       []byte(fmt.Sprintf(`{"id": %d}`, i)),
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
		}
		mockClient.SetMockResponse(path, response)
		mockClient.On("Get", context.Background(), path, map[string]string(nil)).Return(
			&httpclient.Response{
				StatusCode: response.StatusCode,
				Body:       response.Body,
				Headers:    sharedTesting.ConvertToHTTPHeaders(response.Headers),
			}, nil)
	}

	results := make(chan error, numGoroutines)

	// When: Making concurrent requests
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			path := fmt.Sprintf("/api/v1/concurrent/%d", id)
			_, err := mockClient.Get(context.Background(), path, nil)
			results <- err
		}(i)
	}

	// Then: All requests should succeed
	for i := 0; i < numGoroutines; i++ {
		err := <-results
		assert.NoError(t, err, "Concurrent request %d should succeed", i)
	}
}

// TestHeaderConversion tests the header conversion utilities
func TestHeaderConversion(t *testing.T) {
	// Given: A simple header map
	simpleHeaders := map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer token123",
		"X-Request-ID":  "req-456",
	}

	// When: Converting to HTTP headers
	httpHeaders := sharedTesting.ConvertToHTTPHeaders(simpleHeaders)

	// Then: Conversion should be correct
	assert.Equal(t, "application/json", httpHeaders.Get("Content-Type"))
	assert.Equal(t, "Bearer token123", httpHeaders.Get("Authorization"))
	assert.Equal(t, "req-456", httpHeaders.Get("X-Request-ID"))

	// When: Converting back to simple headers
	convertedBack := sharedTesting.ConvertFromHTTPHeaders(httpHeaders)

	// Then: Round trip should preserve values; keys come back in canonical
	// MIME form, so "X-Request-ID" is stored as "X-Request-Id"
	assert.Equal(t, simpleHeaders["Content-Type"], convertedBack["Content-Type"])
	assert.Equal(t, simpleHeaders["Authorization"], convertedBack["Authorization"])
	assert.Equal(t, simpleHeaders["X-Request-ID"], convertedBack["X-Request-Id"])
}
