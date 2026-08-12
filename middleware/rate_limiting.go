package middleware

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/dobrevit/svckit/httpx"
	"github.com/dobrevit/svckit/rediscluster"
)

// EndpointType represents different types of endpoints with different rate limits
type EndpointType string

const (
	EndpointTypeAuth     EndpointType = "auth"     // Authentication endpoints (strict)
	EndpointTypeData     EndpointType = "data"     // Data access endpoints (moderate)
	EndpointTypeUpload   EndpointType = "upload"   // File upload endpoints (strict)
	EndpointTypeAdmin    EndpointType = "admin"    // Admin operations (moderate)
	EndpointTypePublic   EndpointType = "public"   // Public endpoints (lenient)
	EndpointTypeInternal EndpointType = "internal" // Internal service calls (lenient)
	EndpointTypeHealthy  EndpointType = "health"   // Health checks (no limit)
)

// RateLimitConfig holds configuration for rate limiting
type RateLimitConfig struct {
	RequestsPerMinute int           `json:"requests_per_minute"`
	BurstAllowance    int           `json:"burst_allowance"`
	BlockDuration     time.Duration `json:"block_duration"`
}

// RateLimitConfigs holds rate limit configurations for different endpoint types
var RateLimitConfigs = map[EndpointType]RateLimitConfig{
	EndpointTypeAuth: {
		RequestsPerMinute: 10, // Strict for auth to prevent brute force
		BurstAllowance:    3,  // Allow small burst for legitimate retries
		BlockDuration:     5 * time.Minute,
	},
	EndpointTypeData: {
		RequestsPerMinute: 60, // Moderate for data access
		BurstAllowance:    10, // Allow reasonable burst for UI interactions
		BlockDuration:     1 * time.Minute,
	},
	EndpointTypeUpload: {
		RequestsPerMinute: 20, // Strict for uploads to prevent abuse
		BurstAllowance:    5,  // Small burst for legitimate file operations
		BlockDuration:     2 * time.Minute,
	},
	EndpointTypeAdmin: {
		RequestsPerMinute: 30, // Moderate for admin operations
		BurstAllowance:    5,  // Small burst for admin tasks
		BlockDuration:     1 * time.Minute,
	},
	EndpointTypePublic: {
		RequestsPerMinute: 100, // Lenient for public endpoints
		BurstAllowance:    20,  // Allow burst for legitimate traffic
		BlockDuration:     30 * time.Second,
	},
	EndpointTypeInternal: {
		RequestsPerMinute: 200, // Lenient for internal service calls
		BurstAllowance:    50,  // Allow burst for service communication
		BlockDuration:     30 * time.Second,
	},
	EndpointTypeHealthy: {
		RequestsPerMinute: 0, // No limit for health checks
		BurstAllowance:    0,
		BlockDuration:     0,
	},
}

// RateLimiter implements Redis-backed rate limiting
type RateLimiter struct {
	redisClient *rediscluster.ClusterClient
	serviceName string
}

// NewRateLimiter creates a new rate limiter instance
func NewRateLimiter(redisClient *rediscluster.ClusterClient, serviceName string) *RateLimiter {
	return &RateLimiter{
		redisClient: redisClient,
		serviceName: serviceName,
	}
}

// ClassifyEndpoint determines the endpoint type based on the request path and method
func ClassifyEndpoint(path, method string) EndpointType {
	path = strings.ToLower(path)
	method = strings.ToUpper(method)

	// Health check endpoints
	if strings.Contains(path, "/health") || strings.Contains(path, "/ready") || strings.Contains(path, "/metrics") {
		return EndpointTypeHealthy
	}

	// Internal service endpoints
	if strings.Contains(path, "/internal") || strings.Contains(path, "/service-auth") {
		return EndpointTypeInternal
	}

	// Authentication endpoints
	if strings.Contains(path, "/auth") || strings.Contains(path, "/login") || strings.Contains(path, "/logout") {
		return EndpointTypeAuth
	}

	// File upload endpoints
	if method == "POST" && (strings.Contains(path, "/upload") || strings.Contains(path, "/documents")) {
		return EndpointTypeUpload
	}

	// Admin endpoints
	if strings.Contains(path, "/admin") || strings.Contains(path, "/service-keys") {
		return EndpointTypeAdmin
	}

	// Public endpoints (no auth required)
	if method == "GET" && (strings.Contains(path, "/public") ||
		strings.Contains(path, "/verify-email") ||
		strings.Contains(path, "/status")) {
		return EndpointTypePublic
	}

	// Default to data access for other endpoints
	return EndpointTypeData
}

// ClientIdentifier returns the rate-limiting bucket for r: the authenticated
// user, else the calling service, else the client address. Buckets are
// prefixed by kind so a user ID can never collide with a service name.
func ClientIdentifier(r *http.Request) string {
	ctx := r.Context()
	if userID, ok := UserID(ctx); ok {
		return "user:" + userID
	}
	if serviceName, ok := ServiceName(ctx); ok {
		return "service:" + serviceName
	}

	clientIP := ClientIP(r)
	if clientIP == "" {
		clientIP = "unknown"
	}
	return "ip:" + clientIP
}

// IsAllowed checks if the request is allowed based on rate limits
func (rl *RateLimiter) IsAllowed(ctx context.Context, clientID string, endpointType EndpointType) (bool, int, error) {
	config := RateLimitConfigs[endpointType]

	// No rate limiting for health endpoints
	if config.RequestsPerMinute == 0 {
		return true, 0, nil
	}

	now := time.Now()
	windowStart := now.Truncate(time.Minute)

	// Redis keys
	countKey := fmt.Sprintf("rate_limit:%s:%s:%s:%d", rl.serviceName, clientID, endpointType, windowStart.Unix())
	blockKey := fmt.Sprintf("rate_limit_block:%s:%s:%s", rl.serviceName, clientID, endpointType)

	// Check if client is currently blocked
	blocked, err := rl.redisClient.Exists(ctx, blockKey).Result()
	if err != nil {
		return false, 0, fmt.Errorf("failed to check block status: %w", err)
	}

	if blocked > 0 {
		ttl, _ := rl.redisClient.TTL(ctx, blockKey).Result()
		return false, int(ttl.Seconds()), nil
	}

	// Get current request count
	count, err := rl.redisClient.Get(ctx, countKey).Result()
	if err != nil && err.Error() != "redis: nil" {
		return false, 0, fmt.Errorf("failed to get request count: %w", err)
	}

	currentCount := 0
	if count != "" {
		currentCount, _ = strconv.Atoi(count)
	}

	// Check if within limits
	limit := config.RequestsPerMinute + config.BurstAllowance
	if currentCount >= limit {
		// Block the client
		err = rl.redisClient.Set(ctx, blockKey, "1", config.BlockDuration).Err()
		if err != nil {
			return false, 0, fmt.Errorf("failed to set block: %w", err)
		}

		// Record rate limit exceeded metric
		RecordRateLimitExceeded(rl.serviceName, string(endpointType), clientID)

		return false, int(config.BlockDuration.Seconds()), nil
	}

	// Increment counter
	pipe := rl.redisClient.Pipeline()
	pipe.Incr(ctx, countKey)
	pipe.Expire(ctx, countKey, time.Minute)
	_, err = pipe.Exec(ctx)

	if err != nil {
		return false, 0, fmt.Errorf("failed to increment counter: %w", err)
	}

	// Record rate limit check metric
	RecordRateLimitCheck(rl.serviceName, string(endpointType), "allowed")

	return true, 0, nil
}

// RateLimit returns a middleware enforcing the per-endpoint-type limits in
// RateLimitConfigs, backed by redisClient.
//
// A rate limiter that cannot reach Redis lets the request through: an
// unreachable limiter should not take the service down with it. That choice
// means a Redis outage removes rate limiting rather than traffic.
func RateLimit(redisClient *rediscluster.ClusterClient, serviceName string) func(http.Handler) http.Handler {
	limiter := NewRateLimiter(redisClient, serviceName)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			endpointType := ClassifyEndpoint(r.URL.Path, r.Method)
			config := RateLimitConfigs[endpointType]

			allowed, retryAfter, err := limiter.IsAllowed(r.Context(), ClientIdentifier(r), endpointType)
			if err != nil {
				RecordRateLimitError(serviceName, string(endpointType), err.Error())
				next.ServeHTTP(w, r)
				return
			}

			if !allowed {
				w.Header().Set("X-RateLimit-Limit", strconv.Itoa(config.RequestsPerMinute))
				w.Header().Set("X-RateLimit-Reset", strconv.Itoa(retryAfter))
				w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
				RecordRateLimitCheck(serviceName, string(endpointType), "blocked")

				httpx.WriteJSON(w, http.StatusTooManyRequests, map[string]any{
					"error":         "Rate limit exceeded",
					"message":       fmt.Sprintf("Too many requests for %s endpoints", endpointType),
					"retry_after":   retryAfter,
					"endpoint_type": endpointType,
				})
				return
			}

			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(config.RequestsPerMinute))
			w.Header().Set("X-RateLimit-Type", string(endpointType))
			next.ServeHTTP(w, r)
		})
	}
}

// Rate limiting metrics
var (
	rateLimitChecks = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rate_limit_checks_total",
			Help: "Total number of rate limit checks performed",
		},
		[]string{"service", "endpoint_type", "result"},
	)

	rateLimitExceeded = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rate_limit_exceeded_total",
			Help: "Total number of rate limit violations",
		},
		[]string{"service", "endpoint_type", "client_type"},
	)

	rateLimitErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rate_limit_errors_total",
			Help: "Total number of rate limiting errors",
		},
		[]string{"service", "endpoint_type", "error"},
	)
)

// RecordRateLimitCheck records a rate limit check
func RecordRateLimitCheck(serviceName, endpointType, result string) {
	rateLimitChecks.WithLabelValues(serviceName, endpointType, result).Inc()
}

// RecordRateLimitExceeded records a rate limit violation
func RecordRateLimitExceeded(serviceName, endpointType, clientID string) {
	clientType := "unknown"
	if strings.HasPrefix(clientID, "user:") {
		clientType = "user"
	} else if strings.HasPrefix(clientID, "service:") {
		clientType = "service"
	} else if strings.HasPrefix(clientID, "ip:") {
		clientType = "ip"
	}

	rateLimitExceeded.WithLabelValues(serviceName, endpointType, clientType).Inc()
}

// RecordRateLimitError records a rate limiting error
func RecordRateLimitError(serviceName, endpointType, errorMsg string) {
	rateLimitErrors.WithLabelValues(serviceName, endpointType, errorMsg).Inc()
}
