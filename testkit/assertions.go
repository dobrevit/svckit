package testkit

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type HTTPAssertions struct {
	t assert.TestingT
}

// NewHTTPAssertions creates new HTTP assertion helper
func NewHTTPAssertions(t assert.TestingT) *HTTPAssertions {
	return &HTTPAssertions{t: t}
}

// AssertStatusCode verifies HTTP status code
func (a *HTTPAssertions) AssertStatusCode(actual, expected int, msgAndArgs ...interface{}) bool {
	return assert.Equal(a.t, expected, actual, msgAndArgs...)
}

// AssertJSONResponse verifies JSON response structure
func (a *HTTPAssertions) AssertJSONResponse(body []byte, expectedFields map[string]interface{}, msgAndArgs ...interface{}) bool {
	var response map[string]interface{}

	if !assert.NoError(a.t, json.Unmarshal(body, &response), "Response should be valid JSON") {
		return false
	}

	for key, expectedValue := range expectedFields {
		actualValue, exists := response[key]
		if !assert.True(a.t, exists, "Response missing key: %s", key) {
			return false
		}

		if expectedValue != nil && !assert.Equal(a.t, expectedValue, actualValue, "Response value mismatch for key: %s", key) {
			return false
		}
	}

	return true
}

// AssertContainsHeaders verifies response contains expected headers
func (a *HTTPAssertions) AssertContainsHeaders(headers map[string]string, expectedHeaders map[string]string, msgAndArgs ...interface{}) bool {
	for expectedKey, expectedValue := range expectedHeaders {
		actualValue, exists := headers[expectedKey]
		if !assert.True(a.t, exists, "Response missing header: %s", expectedKey) {
			return false
		}
		if !assert.Equal(a.t, expectedValue, actualValue, "Header value mismatch for: %s", expectedKey) {
			return false
		}
	}
	return true
}

// DatabaseAssertions provides specialized assertions for database testing
type PerformanceAssertions struct {
	t assert.TestingT
}

// NewPerformanceAssertions creates new performance assertion helper
func NewPerformanceAssertions(t assert.TestingT) *PerformanceAssertions {
	return &PerformanceAssertions{t: t}
}

// AssertExecutionTime verifies execution time is within bounds
func (a *PerformanceAssertions) AssertExecutionTime(duration time.Duration, maxDuration time.Duration, msgAndArgs ...interface{}) bool {
	return assert.LessOrEqual(a.t, duration, maxDuration,
		fmt.Sprintf("Execution time %v exceeded maximum %v", duration, maxDuration))
}

// AssertMinimumThroughput verifies minimum operations per second
func (a *PerformanceAssertions) AssertMinimumThroughput(operations int, duration time.Duration, minimumOpsPerSecond float64, msgAndArgs ...interface{}) bool {
	actualThroughput := float64(operations) / duration.Seconds()
	return assert.GreaterOrEqual(a.t, actualThroughput, minimumOpsPerSecond,
		fmt.Sprintf("Throughput %.2f ops/sec is below minimum %.2f ops/sec", actualThroughput, minimumOpsPerSecond))
}

// ConcurrencyAssertions provides specialized assertions for concurrency testing
type ConcurrencyAssertions struct {
	t assert.TestingT
}

// NewConcurrencyAssertions creates new concurrency assertion helper
func NewConcurrencyAssertions(t assert.TestingT) *ConcurrencyAssertions {
	return &ConcurrencyAssertions{t: t}
}

// AssertNoRaceConditions verifies no race conditions occurred
func (a *ConcurrencyAssertions) AssertNoRaceConditions(expectedCount int, actualCount int, msgAndArgs ...interface{}) bool {
	return assert.Equal(a.t, expectedCount, actualCount, "Race condition detected: count mismatch")
}

// AssertGoroutineCleanup verifies goroutines were properly cleaned up
func (a *ConcurrencyAssertions) AssertGoroutineCleanup(initialCount int, finalCount int, tolerance int, msgAndArgs ...interface{}) bool {
	diff := finalCount - initialCount
	return assert.LessOrEqual(a.t, diff, tolerance,
		fmt.Sprintf("Goroutine leak detected: %d new goroutines (tolerance: %d)", diff, tolerance))
}

// BusinessLogicAssertions provides specialized assertions for business logic testing
func AssertDeepEqual(t assert.TestingT, expected, actual interface{}, msgAndArgs ...interface{}) bool {
	if reflect.DeepEqual(expected, actual) {
		return true
	}

	// Provide detailed comparison for debugging
	expectedJSON, _ := json.MarshalIndent(expected, "", "  ")
	actualJSON, _ := json.MarshalIndent(actual, "", "  ")

	message := fmt.Sprintf("Deep equality failed:\nExpected:\n%s\nActual:\n%s",
		string(expectedJSON), string(actualJSON))

	if len(msgAndArgs) > 0 {
		message = fmt.Sprintf("%s\n%s", fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...), message)
	}

	return assert.Fail(t, message)
}

// RequireEventually is like testify's Eventually but with better error messages
func RequireEventually(t require.TestingT, condition func() bool, waitFor time.Duration, tick time.Duration, msgAndArgs ...interface{}) {
	timer := time.NewTimer(waitFor)
	defer timer.Stop()

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-timer.C:
			require.True(t, false, "Condition never became true within %v", waitFor)
			return
		case <-ticker.C:
			if condition() {
				return
			}
		}
	}
}
