package health

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"runtime"
	"time"

	"github.com/dobrevit/svckit/amqpcluster"
	"github.com/dobrevit/svckit/buildinfo"
	"github.com/dobrevit/svckit/logging"
	"github.com/dobrevit/svckit/rediscluster"
	"github.com/redis/go-redis/v9"
)

// ServiceHealth represents the health status of a service
type ServiceHealth struct {
	Status       string          `json:"status"`
	Version      string          `json:"version"`
	Uptime       int64           `json:"uptime"`
	Timestamp    string          `json:"timestamp"`
	Service      string          `json:"service"`
	Dependencies map[string]bool `json:"dependencies"`
	Metrics      map[string]any  `json:"metrics"`
	Error        string          `json:"error,omitempty"`
}

// HealthChecker provides health check functionality
type HealthChecker struct {
	serviceName      string
	version          string
	startTime        time.Time
	db               *sql.DB
	redis            *redis.Client
	redisCluster     *rediscluster.ClusterAdapter
	rabbitPublisher  *amqpcluster.ClusterPublisher
	rabbitSubscriber *amqpcluster.ClusterSubscriber
}

// NewHealthChecker creates a new health checker instance
func NewHealthChecker(serviceName string) *HealthChecker {
	return &HealthChecker{
		serviceName: serviceName,
		version:     buildinfo.GetVersionString(),
		startTime:   time.Now(),
	}
}

// SetDatabase sets the database connection for health checks
func (h *HealthChecker) SetDatabase(db *sql.DB) {
	h.db = db
}

// SetRedis sets the Redis client for health checks
func (h *HealthChecker) SetRedis(redis *redis.Client) {
	h.redis = redis
}

// SetRedisCluster sets the Redis cluster adapter for health checks
func (h *HealthChecker) SetRedisCluster(cluster *rediscluster.ClusterAdapter) {
	h.redisCluster = cluster
}

// SetRabbitMQPublisher sets the RabbitMQ cluster publisher for health checks
func (h *HealthChecker) SetRabbitMQPublisher(publisher *amqpcluster.ClusterPublisher) {
	h.rabbitPublisher = publisher
}

// SetRabbitMQSubscriber sets the RabbitMQ cluster subscriber for health checks
func (h *HealthChecker) SetRabbitMQSubscriber(subscriber *amqpcluster.ClusterSubscriber) {
	h.rabbitSubscriber = subscriber
}

// CheckHealth performs a comprehensive health check
func (h *HealthChecker) CheckHealth(ctx context.Context) ServiceHealth {
	health := ServiceHealth{
		Status:       "healthy",
		Version:      h.version,
		Uptime:       int64(time.Since(h.startTime).Seconds()),
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
		Service:      h.serviceName,
		Dependencies: make(map[string]bool),
		Metrics:      make(map[string]any),
	}

	// Check PostgreSQL connection
	if h.db != nil {
		health.Dependencies["postgres"] = h.checkPostgres(ctx)
		if !health.Dependencies["postgres"] {
			health.Status = "degraded"
		}
	}

	// Check Redis connection (single instance)
	if h.redis != nil {
		health.Dependencies["redis"] = h.checkRedis(ctx)
		if !health.Dependencies["redis"] {
			health.Status = "degraded"
		}
	}

	// Check Redis cluster connection
	if h.redisCluster != nil {
		clusterHealth := h.redisCluster.GetClusterHealth()
		health.Dependencies["redis_cluster"] = clusterHealth.HealthyNodes > 0
		health.Metrics["redis_cluster_nodes"] = map[string]any{
			"total":   clusterHealth.TotalNodes,
			"healthy": clusterHealth.HealthyNodes,
		}
		if clusterHealth.HealthyNodes == 0 {
			health.Status = "unhealthy"
		} else if clusterHealth.HealthyNodes < clusterHealth.TotalNodes {
			health.Status = "degraded"
		}
	}

	// Check RabbitMQ publisher
	if h.rabbitPublisher != nil {
		rabbitHealthy := h.rabbitPublisher.TestConnection() == nil
		health.Dependencies["rabbitmq_publisher"] = rabbitHealthy
		if !rabbitHealthy {
			health.Status = "degraded"
		}

		// Add RabbitMQ publisher stats
		stats := h.rabbitPublisher.GetStats()
		health.Metrics["rabbitmq_publisher"] = map[string]any{
			"service_name":  stats.ServiceName,
			"exchange":      stats.Exchange,
			"total_nodes":   stats.TotalNodes,
			"healthy_nodes": stats.HealthyNodes,
		}
	}

	// Check RabbitMQ subscriber
	if h.rabbitSubscriber != nil {
		// Note: Subscriber doesn't have TestConnection, use stats instead
		stats := h.rabbitSubscriber.GetStats()
		subscriberHealthy := stats.HealthyNodes > 0
		health.Dependencies["rabbitmq_subscriber"] = subscriberHealthy
		if !subscriberHealthy {
			health.Status = "degraded"
		}

		// Add RabbitMQ subscriber stats
		health.Metrics["rabbitmq_subscriber"] = map[string]any{
			"service_name":  stats.ServiceName,
			"exchange":      stats.Exchange,
			"total_nodes":   stats.TotalNodes,
			"healthy_nodes": stats.HealthyNodes,
		}
	}

	// Add runtime metrics
	h.addRuntimeMetrics(&health)

	return health
}

// checkPostgres checks if PostgreSQL is accessible
func (h *HealthChecker) checkPostgres(ctx context.Context) bool {
	if h.db == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := h.db.PingContext(ctx); err != nil {
		return false
	}

	return true
}

// checkRedis checks if Redis is accessible
func (h *HealthChecker) checkRedis(ctx context.Context) bool {
	if h.redis == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := h.redis.Ping(ctx).Err(); err != nil {
		return false
	}

	return true
}

// addRuntimeMetrics adds runtime metrics to the health response
func (h *HealthChecker) addRuntimeMetrics(health *ServiceHealth) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	health.Metrics["goroutines"] = runtime.NumGoroutine()
	health.Metrics["memory_alloc_mb"] = m.Alloc / 1024 / 1024
	health.Metrics["memory_sys_mb"] = m.Sys / 1024 / 1024
	health.Metrics["gc_runs"] = m.NumGC
	health.Metrics["cpu_count"] = runtime.NumCPU()
}

// SimpleHealthHandler returns a simple health check handler (for k8s probes)
func SimpleHealthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]string{
			"status": "ok",
		}); err != nil {
			logging.Error("Failed to write health response: %v", err)
		}
	}
}

// DetailedHealthHandler returns a detailed health check handler (for monitoring)
func (h *HealthChecker) DetailedHealthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		health := h.CheckHealth(ctx)

		statusCode := http.StatusOK
		switch health.Status {
		case "degraded", "unhealthy":
			statusCode = http.StatusServiceUnavailable
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		if err := json.NewEncoder(w).Encode(health); err != nil {
			logging.Error("Failed to write health response: %v", err)
		}
	}
}
