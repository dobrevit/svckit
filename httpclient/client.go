package httpclient

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dobrevit/svckit/buildinfo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// CircuitBreakerState represents the state of the circuit breaker
type CircuitBreakerState int

const (
	CircuitClosed CircuitBreakerState = iota
	CircuitOpen
	CircuitHalfOpen
)

// CircuitBreaker implements a simple circuit breaker pattern
type CircuitBreaker struct {
	maxFailures     int
	resetTimeout    time.Duration
	failureCount    int
	lastFailureTime time.Time
	state           CircuitBreakerState
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(maxFailures int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures:  maxFailures,
		resetTimeout: resetTimeout,
		state:        CircuitClosed,
	}
}

// CanExecute checks if the circuit breaker allows execution
func (cb *CircuitBreaker) CanExecute() bool {
	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if time.Since(cb.lastFailureTime) > cb.resetTimeout {
			cb.state = CircuitHalfOpen
			return true
		}
		return false
	case CircuitHalfOpen:
		return true
	}
	return false
}

// RecordSuccess records a successful execution
func (cb *CircuitBreaker) RecordSuccess() {
	cb.failureCount = 0
	cb.state = CircuitClosed
}

// RecordFailure records a failed execution
func (cb *CircuitBreaker) RecordFailure() {
	cb.failureCount++
	cb.lastFailureTime = time.Now()
	if cb.failureCount >= cb.maxFailures {
		cb.state = CircuitOpen
	}
}

// RetryConfig defines retry behavior
type RetryConfig struct {
	MaxRetries  int
	InitialWait time.Duration
	MaxWait     time.Duration
	Multiplier  float64
}

// DefaultRetryConfig returns a sensible default retry configuration
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:  3,
		InitialWait: 100 * time.Millisecond,
		MaxWait:     5 * time.Second,
		Multiplier:  2.0,
	}
}

// Prometheus metrics for HTTP client observability
var (
	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_client_request_duration_seconds",
			Help:    "Duration of HTTP requests from shared httpclient",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"service", "method", "status_code", "target_service"},
	)

	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_client_requests_total",
			Help: "Total number of HTTP requests from shared httpclient",
		},
		[]string{"service", "method", "status_code", "target_service"},
	)

	httpCircuitBreakerState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "http_client_circuit_breaker_state",
			Help: "Circuit breaker state (0=closed, 1=open, 2=half-open)",
		},
		[]string{"service", "target_service"},
	)

	httpRetryAttemptsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_client_retry_attempts_total",
			Help: "Total number of retry attempts from shared httpclient",
		},
		[]string{"service", "target_service", "reason"},
	)

	httpErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_client_errors_total",
			Help: "Total number of HTTP client errors",
		},
		[]string{"service", "target_service", "error_type"},
	)
)

// HTTPClientMetrics handles metrics collection for HTTP requests
type HTTPClientMetrics struct {
	serviceName   string
	targetService string
}

// NewHTTPClientMetrics creates a new metrics collector
func NewHTTPClientMetrics(serviceName, targetService string) *HTTPClientMetrics {
	return &HTTPClientMetrics{
		serviceName:   serviceName,
		targetService: targetService,
	}
}

// RecordRequest records HTTP request metrics
func (m *HTTPClientMetrics) RecordRequest(method string, statusCode int, duration time.Duration) {
	if m == nil {
		return
	}

	statusStr := strconv.Itoa(statusCode)

	httpRequestDuration.WithLabelValues(
		m.serviceName,
		method,
		statusStr,
		m.targetService,
	).Observe(duration.Seconds())

	httpRequestsTotal.WithLabelValues(
		m.serviceName,
		method,
		statusStr,
		m.targetService,
	).Inc()
}

// RecordCircuitBreakerState records circuit breaker state changes
func (m *HTTPClientMetrics) RecordCircuitBreakerState(state CircuitBreakerState) {
	if m == nil {
		return
	}

	httpCircuitBreakerState.WithLabelValues(
		m.serviceName,
		m.targetService,
	).Set(float64(state))
}

// RecordRetryAttempt records retry attempts
func (m *HTTPClientMetrics) RecordRetryAttempt(reason string) {
	if m == nil {
		return
	}

	httpRetryAttemptsTotal.WithLabelValues(
		m.serviceName,
		m.targetService,
		reason,
	).Inc()
}

// RecordError records HTTP client errors
func (m *HTTPClientMetrics) RecordError(errorType string) {
	if m == nil {
		return
	}

	httpErrorsTotal.WithLabelValues(
		m.serviceName,
		m.targetService,
		errorType,
	).Inc()
}

// Distributed tracing support
const (
	// Standard distributed tracing headers
	TraceIDHeader      = "X-Trace-ID"
	SpanIDHeader       = "X-Span-ID"
	ParentSpanIDHeader = "X-Parent-Span-ID"
	RequestIDHeader    = "X-Request-ID"

	// TraceparentHeader carries the same trace in the W3C Trace Context
	// format, so OpenTelemetry-instrumented peers can join the trace without
	// this package adopting an OTel dependency.
	TraceparentHeader = "traceparent"

	// Custom correlation headers
	CorrelationIDHeader = "X-Correlation-ID"
	UserIDHeader        = "X-User-ID"
	ServiceChainHeader  = "X-Service-Chain"
)

// traceparentVersion is the only W3C Trace Context version defined so far.
// Receivers must accept later versions whose format still parses, so parsing
// checks the field shapes rather than requiring this exact value.
const traceparentVersion = "00"

// sampledFlag marks a trace as recorded. Sampling is not implemented, so every
// trace this package starts is reported as sampled.
const sampledFlag = "01"

// TraceContext holds distributed tracing information
type TraceContext struct {
	TraceID       string
	SpanID        string
	ParentSpanID  string
	RequestID     string
	CorrelationID string
	UserID        string
	ServiceChain  []string
}

// NewTraceContext creates a new trace context or continues existing one
func NewTraceContext(ctx context.Context) *TraceContext {
	// Try to extract existing trace context from Go context
	if existingTrace := TraceFromContext(ctx); existingTrace != nil {
		// Continue existing trace with new span
		return &TraceContext{
			TraceID:       existingTrace.TraceID,
			SpanID:        generateSpanID(),
			ParentSpanID:  existingTrace.SpanID,
			RequestID:     existingTrace.RequestID,
			CorrelationID: existingTrace.CorrelationID,
			UserID:        existingTrace.UserID,
			ServiceChain:  existingTrace.ServiceChain,
		}
	}

	// Create new trace context
	return &TraceContext{
		TraceID:       generateID(),
		SpanID:        generateSpanID(),
		RequestID:     generateID(),
		CorrelationID: generateID(),
		ServiceChain:  []string{},
	}
}

// AddToServiceChain adds a service to the service call chain
func (tc *TraceContext) AddToServiceChain(serviceName string) {
	tc.ServiceChain = append(tc.ServiceChain, serviceName)
}

// ToHeaders converts trace context to HTTP headers
func (tc *TraceContext) ToHeaders() map[string]string {
	if tc == nil {
		return map[string]string{}
	}

	headers := map[string]string{
		TraceIDHeader:       tc.TraceID,
		SpanIDHeader:        tc.SpanID,
		RequestIDHeader:     tc.RequestID,
		CorrelationIDHeader: tc.CorrelationID,
	}

	if tp := tc.Traceparent(); tp != "" {
		headers[TraceparentHeader] = tp
	}

	if tc.ParentSpanID != "" {
		headers[ParentSpanIDHeader] = tc.ParentSpanID
	}

	if tc.UserID != "" {
		headers[UserIDHeader] = tc.UserID
	}

	if len(tc.ServiceChain) > 0 {
		headers[ServiceChainHeader] = strings.Join(tc.ServiceChain, "->")
	}

	return headers
}

// Traceparent renders the trace in the W3C Trace Context format, or returns
// the empty string when the identifiers do not fit that format — a trace-id
// must be 32 hex digits and a span-id 16, and neither may be all zeroes.
func (tc *TraceContext) Traceparent() string {
	if tc == nil || !isHex(tc.TraceID, 32) || !isHex(tc.SpanID, 16) {
		return ""
	}
	return traceparentVersion + "-" + tc.TraceID + "-" + tc.SpanID + "-" + sampledFlag
}

// parseTraceparent splits a W3C traceparent value into its trace-id and
// span-id. The span-id of an incoming header identifies the caller's span, so
// it becomes this request's parent span.
func parseTraceparent(value string) (traceID, parentSpanID string, ok bool) {
	parts := strings.Split(strings.TrimSpace(value), "-")
	if len(parts) < 4 || len(parts[0]) != 2 {
		return "", "", false
	}
	if !isHex(parts[1], 32) || !isHex(parts[2], 16) {
		return "", "", false
	}
	return parts[1], parts[2], true
}

// isHex reports whether s is exactly n lowercase-hex digits and not all zeroes,
// the validity rule W3C Trace Context places on trace-id and span-id.
func isHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	allZero := true
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
		default:
			return false
		}
		if c != '0' {
			allZero = false
		}
	}
	return !allZero
}

// FromHeaders creates trace context from HTTP headers
func TraceContextFromHeaders(headers http.Header) *TraceContext {
	tc := &TraceContext{
		TraceID:       headers.Get(TraceIDHeader),
		SpanID:        headers.Get(SpanIDHeader),
		ParentSpanID:  headers.Get(ParentSpanIDHeader),
		RequestID:     headers.Get(RequestIDHeader),
		CorrelationID: headers.Get(CorrelationIDHeader),
		UserID:        headers.Get(UserIDHeader),
	}

	// A W3C traceparent wins over the X- headers: it is the interoperable
	// format, so a peer sending both is most likely a gateway translating the
	// legacy headers it received.
	if traceID, parentSpanID, ok := parseTraceparent(headers.Get(TraceparentHeader)); ok {
		tc.TraceID = traceID
		tc.ParentSpanID = parentSpanID
		if tc.SpanID == "" {
			tc.SpanID = generateSpanID()
		}
	}

	// Parse service chain
	if chainStr := headers.Get(ServiceChainHeader); chainStr != "" {
		tc.ServiceChain = strings.Split(chainStr, "->")
	}

	return tc
}

// generateID generates a 16-byte random ID, the width W3C Trace Context
// requires of a trace-id.
func generateID() string { return randomHex(16) }

// generateSpanID generates an 8-byte random ID, the width W3C Trace Context
// requires of a span-id.
func generateSpanID() string { return randomHex(8) }

func randomHex(n int) string {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to timestamp-based ID
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(bytes)
}

// Context keys for trace propagation
type contextKey string

const traceContextKey = contextKey("traceContext")

// ContextWithTrace adds trace context to Go context
func ContextWithTrace(ctx context.Context, trace *TraceContext) context.Context {
	return context.WithValue(ctx, traceContextKey, trace)
}

// TraceFromContext extracts trace context from Go context
func TraceFromContext(ctx context.Context) *TraceContext {
	if trace, ok := ctx.Value(traceContextKey).(*TraceContext); ok {
		return trace
	}
	return nil
}

// Client represents a configured HTTP client for service-to-service communication
// DefaultAPIKeyPrefix is the service-API-key prefix assumed when Config
// leaves APIKeyPrefix empty.
const DefaultAPIKeyPrefix = "sk_"

type Client struct {
	httpClient     *http.Client
	baseURL        string
	serviceName    string
	authToken      string
	apiKeyPrefix   string
	userAgent      string
	defaultHeaders map[string]string
	circuitBreaker *CircuitBreaker
	retryConfig    RetryConfig
	metrics        *HTTPClientMetrics
}

// Config holds configuration for the HTTP client
type Config struct {
	BaseURL     string
	ServiceName string
	// TargetServiceName labels metrics for the called service. When empty it
	// is derived from the BaseURL hostname.
	TargetServiceName string
	AuthToken         string
	// APIKeyPrefix identifies an AuthToken that is a service API key rather
	// than a user session token: a token carrying this prefix is sent as
	// X-API-Key, anything else as an Authorization bearer token. Defaults to
	// DefaultAPIKeyPrefix.
	APIKeyPrefix   string
	Timeout        time.Duration
	Headers        map[string]string
	RetryConfig    *RetryConfig
	CircuitBreaker *CircuitBreaker
}

// NewClient creates a new HTTP client with proper User-Agent and authentication
func NewClient(config Config) *Client {
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.APIKeyPrefix == "" {
		config.APIKeyPrefix = DefaultAPIKeyPrefix
	}

	if config.Headers == nil {
		config.Headers = make(map[string]string)
	}

	// Set default retry config if not provided
	retryConfig := DefaultRetryConfig()
	if config.RetryConfig != nil {
		retryConfig = *config.RetryConfig
	}

	// Set default circuit breaker if not provided
	circuitBreaker := NewCircuitBreaker(5, 60*time.Second)
	if config.CircuitBreaker != nil {
		circuitBreaker = config.CircuitBreaker
	}

	// Label metrics with the configured target service, falling back to the
	// hostname derived from the base URL
	targetService := config.TargetServiceName
	if targetService == "" {
		targetService = hostFromURL(config.BaseURL)
	}

	client := &Client{
		httpClient: &http.Client{
			Timeout: config.Timeout,
		},
		baseURL:        config.BaseURL,
		serviceName:    config.ServiceName,
		authToken:      config.AuthToken,
		apiKeyPrefix:   config.APIKeyPrefix,
		userAgent:      buildinfo.GetUserAgent(config.ServiceName),
		defaultHeaders: config.Headers,
		circuitBreaker: circuitBreaker,
		retryConfig:    retryConfig,
		metrics:        NewHTTPClientMetrics(config.ServiceName, targetService),
	}

	// Initialize circuit breaker state metric
	client.metrics.RecordCircuitBreakerState(circuitBreaker.state)

	return client
}

// hostFromURL derives the metrics label for a called service from its base
// URL: the bare hostname, without protocol, port or path. In containerized
// deployments the hostname is the service name; callers with different
// topologies set Config.TargetServiceName explicitly.
func hostFromURL(baseURL string) string {
	url := strings.TrimPrefix(baseURL, "http://")
	url = strings.TrimPrefix(url, "https://")

	if idx := strings.Index(url, "/"); idx != -1 {
		url = url[:idx]
	}
	if idx := strings.Index(url, ":"); idx != -1 {
		url = url[:idx]
	}
	return url
}

// NewServiceClient creates a client configured for service-to-service communication
func NewServiceClient(baseURL, serviceName, authToken string) *Client {
	config := Config{
		BaseURL:     baseURL,
		ServiceName: serviceName,
		AuthToken:   authToken,
	}
	return NewClient(config)
}

// Request represents an HTTP request configuration
type Request struct {
	Method      string
	Path        string
	Body        interface{}
	Headers     map[string]string
	QueryParams map[string]string
}

// Response represents an HTTP response
type Response struct {
	StatusCode   int
	Body         []byte
	Headers      http.Header
	TraceContext *TraceContext
}

// Do performs an HTTP request with retry logic and circuit breaker
func (c *Client) Do(ctx context.Context, req Request) (*Response, error) {
	startTime := time.Now()

	// Check circuit breaker
	if !c.circuitBreaker.CanExecute() {
		c.metrics.RecordError("circuit_breaker_open")
		return nil, fmt.Errorf("circuit breaker is open")
	}

	var lastErr error
	waitTime := c.retryConfig.InitialWait

	for attempt := 0; attempt <= c.retryConfig.MaxRetries; attempt++ {
		if attempt > 0 {
			// Record retry attempt
			var reason string
			if lastErr != nil {
				reason = "network_error"
			} else {
				reason = "server_error"
			}
			c.metrics.RecordRetryAttempt(reason)

			// Wait before retry
			select {
			case <-time.After(waitTime):
			case <-ctx.Done():
				c.metrics.RecordError("context_cancelled")
				return nil, ctx.Err()
			}

			// Exponential backoff
			waitTime = time.Duration(float64(waitTime) * c.retryConfig.Multiplier)
			if waitTime > c.retryConfig.MaxWait {
				waitTime = c.retryConfig.MaxWait
			}
		}

		response, err := c.doSingleRequest(ctx, req)
		if err != nil {
			lastErr = err
			// Network errors are retryable
			continue
		}

		// Record request metrics
		duration := time.Since(startTime)
		c.metrics.RecordRequest(req.Method, response.StatusCode, duration)

		// Success case
		if response.IsSuccess() {
			c.circuitBreaker.RecordSuccess()
			c.metrics.RecordCircuitBreakerState(c.circuitBreaker.state)
			return response, nil
		}

		// Client errors (4xx) are generally not retryable
		if response.StatusCode >= 400 && response.StatusCode < 500 {
			c.circuitBreaker.RecordSuccess() // Not a service failure
			c.metrics.RecordCircuitBreakerState(c.circuitBreaker.state)
			return response, nil
		}

		// Server errors (5xx) are retryable
		if response.StatusCode >= 500 {
			lastErr = fmt.Errorf("server error: %d", response.StatusCode)
			continue
		}

		// Other status codes
		return response, nil
	}

	// All retries exhausted
	c.circuitBreaker.RecordFailure()
	c.metrics.RecordCircuitBreakerState(c.circuitBreaker.state)
	c.metrics.RecordError("max_retries_exceeded")

	if lastErr != nil {
		return nil, fmt.Errorf("request failed after %d retries: %w", c.retryConfig.MaxRetries, lastErr)
	}
	return nil, fmt.Errorf("request failed after %d retries", c.retryConfig.MaxRetries)
}

// doSingleRequest executes a single HTTP request without retry logic
func (c *Client) doSingleRequest(ctx context.Context, req Request) (*Response, error) {
	// Create or continue distributed trace
	traceCtx := NewTraceContext(ctx)
	if traceCtx != nil {
		traceCtx.AddToServiceChain(c.serviceName)
		ctx = ContextWithTrace(ctx, traceCtx)
	}

	// Build URL
	url := c.baseURL + req.Path
	if !strings.HasPrefix(req.Path, "/") {
		url = c.baseURL + "/" + req.Path
	}

	// Add query parameters
	if len(req.QueryParams) > 0 {
		url += "?"
		first := true
		for key, value := range req.QueryParams {
			if !first {
				url += "&"
			}
			url += fmt.Sprintf("%s=%s", key, value)
			first = false
		}
	}

	// Prepare request body
	var bodyReader io.Reader
	if req.Body != nil {
		bodyBytes, err := json.Marshal(req.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(bodyBytes)
	}

	// Create HTTP request
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set default headers
	httpReq.Header.Set("User-Agent", c.userAgent)
	if req.Body != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}

	// Set authentication header with proper service-to-service vs user authentication
	if c.authToken != "" {
		if strings.HasPrefix(c.authToken, c.apiKeyPrefix) {
			// Service API key - use X-API-Key header for proper S2S authentication
			httpReq.Header.Set("X-API-Key", c.authToken)
		} else {
			// User session token - use Authorization Bearer header
			httpReq.Header.Set("Authorization", "Bearer "+c.authToken)
		}
	}

	// Add distributed tracing headers
	if traceCtx != nil {
		for key, value := range traceCtx.ToHeaders() {
			httpReq.Header.Set(key, value)
		}
	}

	// Set default configured headers
	for key, value := range c.defaultHeaders {
		httpReq.Header.Set(key, value)
	}

	// Set request-specific headers (these override defaults)
	for key, value := range req.Headers {
		httpReq.Header.Set(key, value)
	}

	// Perform request
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to perform request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	response := &Response{
		StatusCode: resp.StatusCode,
		Body:       body,
		Headers:    resp.Header,
	}

	// Add trace context to response for potential extraction
	if traceCtx != nil {
		response.TraceContext = traceCtx
	}

	return response, nil
}

// Get performs a GET request
func (c *Client) Get(ctx context.Context, path string, queryParams map[string]string) (*Response, error) {
	return c.Do(ctx, Request{
		Method:      http.MethodGet,
		Path:        path,
		QueryParams: queryParams,
	})
}

// Post performs a POST request
func (c *Client) Post(ctx context.Context, path string, body interface{}) (*Response, error) {
	return c.Do(ctx, Request{
		Method: http.MethodPost,
		Path:   path,
		Body:   body,
	})
}

// Put performs a PUT request
func (c *Client) Put(ctx context.Context, path string, body interface{}) (*Response, error) {
	return c.Do(ctx, Request{
		Method: http.MethodPut,
		Path:   path,
		Body:   body,
	})
}

// Delete performs a DELETE request
func (c *Client) Delete(ctx context.Context, path string) (*Response, error) {
	return c.Do(ctx, Request{
		Method: http.MethodDelete,
		Path:   path,
	})
}

// HealthCheck performs a health check against the target service
func (c *Client) HealthCheck(ctx context.Context) error {
	resp, err := c.Get(ctx, "/health", nil)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check failed with status: %d", resp.StatusCode)
	}

	return nil
}

// UpdateAuthToken updates the authentication token
func (c *Client) UpdateAuthToken(token string) {
	c.authToken = token
}

// GetCircuitBreakerState returns the current circuit breaker state
func (c *Client) GetCircuitBreakerState() CircuitBreakerState {
	return c.circuitBreaker.state
}

// GetBaseURL returns the base URL of the client
func (c *Client) GetBaseURL() string {
	return c.baseURL
}

// GetServiceName returns the service name
func (c *Client) GetServiceName() string {
	return c.serviceName
}

// DecodeJSON decodes a JSON response into the provided interface
func (r *Response) DecodeJSON(v interface{}) error {
	if err := json.Unmarshal(r.Body, v); err != nil {
		return fmt.Errorf("failed to decode JSON response: %w", err)
	}
	return nil
}

// IsSuccess returns true if the response status code indicates success (2xx)
func (r *Response) IsSuccess() bool {
	return r.StatusCode >= 200 && r.StatusCode < 300
}

// GetError returns an error message from the response body if it's an error response
func (r *Response) GetError() error {
	if r.IsSuccess() {
		return nil
	}

	var errorResp struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}

	if err := r.DecodeJSON(&errorResp); err == nil {
		if errorResp.Error != "" {
			return fmt.Errorf("API error (status %d): %s", r.StatusCode, errorResp.Error)
		}
		if errorResp.Message != "" {
			return fmt.Errorf("API error (status %d): %s", r.StatusCode, errorResp.Message)
		}
	}

	return fmt.Errorf("HTTP error: status %d", r.StatusCode)
}
