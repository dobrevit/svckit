package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// HTTP request duration histogram
	httpDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"service", "method", "path", "status_code"},
	)

	// HTTP request counter
	httpRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"service", "method", "path", "status_code"},
	)

	// HTTP requests currently in flight
	httpInFlight = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "Number of HTTP requests currently being processed",
		},
		[]string{"service"},
	)
)

// MetricsPath is the endpoint serving the Prometheus exposition format. The
// middleware skips it so scrapes do not inflate a service's own request
// counts.
const MetricsPath = "/metrics"

// Metrics returns a middleware recording request count, duration and
// in-flight gauge for serviceName.
//
// The route label comes from Route, so it is the templated pattern when the
// router adapter recorded one. Without that, an ID-bearing path produces one
// label value per ID and the series count grows without bound.
func Metrics(serviceName string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == MetricsPath {
				next.ServeHTTP(w, r)
				return
			}

			done := TrackInFlight(serviceName)
			defer done()

			start := time.Now()
			rec := newRecorder(w)
			next.ServeHTTP(rec, r)

			ObserveRequest(serviceName, r.Method, Route(r), rec.status, time.Since(start))
		})
	}
}

// ObserveRequest records one completed HTTP request. It is exported so that
// router-specific adapters, which capture the status themselves, record
// through the same series as the middleware.
func ObserveRequest(serviceName, method, route string, status int, d time.Duration) {
	code := strconv.Itoa(status)
	httpDuration.WithLabelValues(serviceName, method, route, code).Observe(d.Seconds())
	httpRequests.WithLabelValues(serviceName, method, route, code).Inc()
}

// TrackInFlight marks a request as in flight for serviceName and returns the
// function that clears it.
func TrackInFlight(serviceName string) func() {
	httpInFlight.WithLabelValues(serviceName).Inc()
	return func() { httpInFlight.WithLabelValues(serviceName).Dec() }
}

// MetricsHandler serves the Prometheus exposition format.
func MetricsHandler() http.Handler { return promhttp.Handler() }

// DatabaseMetrics contains database-specific metrics
type DatabaseMetrics struct {
	ConnectionsActive prometheus.Gauge
	QueriesTotal      prometheus.CounterVec
	QueryDuration     prometheus.HistogramVec
}

// NewDatabaseMetrics creates database-specific metrics
func NewDatabaseMetrics(serviceName string) *DatabaseMetrics {
	return &DatabaseMetrics{
		ConnectionsActive: promauto.NewGauge(prometheus.GaugeOpts{
			Name:        "database_connections_active",
			Help:        "Number of active database connections",
			ConstLabels: prometheus.Labels{"service": serviceName},
		}),
		QueriesTotal: *promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name:        "database_queries_total",
				Help:        "Total number of database queries",
				ConstLabels: prometheus.Labels{"service": serviceName},
			},
			[]string{"operation", "table", "status"},
		),
		QueryDuration: *promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:        "database_query_duration_seconds",
				Help:        "Duration of database queries in seconds",
				Buckets:     []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
				ConstLabels: prometheus.Labels{"service": serviceName},
			},
			[]string{"operation", "table"},
		),
	}
}
