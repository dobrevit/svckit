package middleware

import (
	"encoding/json"
	"net/http"
	"time"
)

// HealthCheckResponse represents the health check response
type HealthCheckResponse struct {
	Status    string                 `json:"status"`
	Service   string                 `json:"service"`
	Version   string                 `json:"version,omitempty"`
	Timestamp int64                  `json:"timestamp"`
	Uptime    int64                  `json:"uptime"`
	Checks    map[string]CheckResult `json:"checks,omitempty"`
}

// CheckResult represents the result of an individual health check
type CheckResult struct {
	Status      string `json:"status"`
	Message     string `json:"message,omitempty"`
	LastChecked int64  `json:"last_checked"`
	Duration    string `json:"duration,omitempty"`
}

// StatusHealthy and StatusUnhealthy are the values a HealthChecker reports.
const (
	StatusHealthy   = "healthy"
	StatusUnhealthy = "unhealthy"
)

var startTime = time.Now()

// HealthHandler serves a liveness report for serviceName: the outcome of
// every check, with 503 when any of them is unhealthy.
func HealthHandler(serviceName string, checks ...HealthChecker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		results, allHealthy := runChecks(now, checks)

		response := HealthCheckResponse{
			Status:    statusWord(allHealthy, "healthy", "unhealthy"),
			Service:   serviceName,
			Timestamp: now.Unix(),
			Uptime:    int64(now.Sub(startTime).Seconds()),
			Checks:    results,
		}

		writeJSON(w, healthStatusCode(allHealthy), response)
	})
}

// ReadinessHandler serves a readiness report for serviceName, in the shape
// Kubernetes readiness probes consume: 200 while every check passes, 503
// otherwise.
func ReadinessHandler(serviceName string, checks ...HealthChecker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		results, allReady := runChecks(now, checks)

		writeJSON(w, healthStatusCode(allReady), map[string]any{
			"status":    statusWord(allReady, "ready", "not_ready"),
			"service":   serviceName,
			"timestamp": now.Unix(),
			"checks":    results,
		})
	})
}

// runChecks runs every check, timing each one, and reports whether all passed.
func runChecks(now time.Time, checks []HealthChecker) (map[string]CheckResult, bool) {
	results := make(map[string]CheckResult, len(checks))
	allHealthy := true

	for _, check := range checks {
		start := time.Now()
		name, status, message := check.Check()

		results[name] = CheckResult{
			Status:      status,
			Message:     message,
			LastChecked: now.Unix(),
			Duration:    time.Since(start).String(),
		}
		if status != StatusHealthy {
			allHealthy = false
		}
	}

	return results, allHealthy
}

func healthStatusCode(ok bool) int {
	if ok {
		return http.StatusOK
	}
	return http.StatusServiceUnavailable
}

func statusWord(ok bool, whenTrue, whenFalse string) string {
	if ok {
		return whenTrue
	}
	return whenFalse
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// HealthChecker interface for health checks
type HealthChecker interface {
	Check() (name string, status string, message string)
}

// DatabaseHealthChecker checks database connection
type DatabaseHealthChecker struct {
	name string
	ping func() error
}

// NewDatabaseHealthChecker creates a new database health checker
func NewDatabaseHealthChecker(name string, ping func() error) *DatabaseHealthChecker {
	return &DatabaseHealthChecker{
		name: name,
		ping: ping,
	}
}

// Check implements HealthChecker
func (d *DatabaseHealthChecker) Check() (string, string, string) {
	if err := d.ping(); err != nil {
		return d.name, StatusUnhealthy, "Database connection failed: " + err.Error()
	}
	return d.name, StatusHealthy, "Database connection successful"
}

// EventBusHealthChecker checks event bus connection
type EventBusHealthChecker struct {
	name string
	ping func() error
}

// NewEventBusHealthChecker creates a new event bus health checker
func NewEventBusHealthChecker(name string, ping func() error) *EventBusHealthChecker {
	return &EventBusHealthChecker{
		name: name,
		ping: ping,
	}
}

// Check implements HealthChecker
func (e *EventBusHealthChecker) Check() (string, string, string) {
	if err := e.ping(); err != nil {
		return e.name, StatusUnhealthy, "Event bus connection failed: " + err.Error()
	}
	return e.name, StatusHealthy, "Event bus connection successful"
}

// ServiceHealthChecker checks external service connectivity
type ServiceHealthChecker struct {
	name    string
	url     string
	checker func() error
}

// NewServiceHealthChecker creates a new service health checker
func NewServiceHealthChecker(name, url string, checker func() error) *ServiceHealthChecker {
	return &ServiceHealthChecker{
		name:    name,
		url:     url,
		checker: checker,
	}
}

// Check implements HealthChecker
func (s *ServiceHealthChecker) Check() (string, string, string) {
	if err := s.checker(); err != nil {
		return s.name, StatusUnhealthy, "Service unavailable at " + s.url + ": " + err.Error()
	}
	return s.name, StatusHealthy, "Service available at " + s.url
}
