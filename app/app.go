// Package appkit is the service runtime: one constructor that wires the
// infrastructure a service would otherwise hand-roll in main() — logging,
// database cluster, event publisher/subscriber, audit client, event signing,
// health checks, the standard middleware stack and graceful shutdown.
//
// The runtime never builds a router. Handler decorates whatever http.Handler
// the service brings with the standard middleware and the operational
// endpoints, and Run serves it:
//
//	a, err := app.New("orders",
//		app.WithDatabase(),
//		app.WithOptionalEventPublisher(),
//	)
//	if err != nil {
//		return err
//	}
//	defer a.Close()
//
//	mux := http.NewServeMux()
//	// ... register service routes on mux ...
//	a.Run(a.Handler(mux))
//
// Schema migration is a service concern: WithMigration takes a
// func(*sql.DB) error, so goose, Atlas or an ORM's automigration all plug in
// without the runtime depending on any of them.
package app

import (
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"github.com/dobrevit/svckit/amqpcluster"
	"github.com/dobrevit/svckit/audit"
	"github.com/dobrevit/svckit/debug"
	"github.com/dobrevit/svckit/env"
	"github.com/dobrevit/svckit/eventbus"
	"github.com/dobrevit/svckit/health"
	"github.com/dobrevit/svckit/lifecycle"
	"github.com/dobrevit/svckit/logging"
	"github.com/dobrevit/svckit/middleware"
	"github.com/dobrevit/svckit/pgcluster"
)

type options struct {
	wantDB        bool
	migrate       func(*sql.DB) error
	wantPub       bool
	pubOptional   bool
	wantSub       bool
	subOptional   bool
	wantAudit     bool
	auditOptional bool
	extraChecks   []middleware.HealthChecker
}

// Option configures which infrastructure New wires up.
type Option func(*options)

// WithDatabase connects the database cluster and exposes the writer pool
// through SQLDB.
func WithDatabase() Option {
	return func(o *options) { o.wantDB = true }
}

// WithMigration connects the database cluster and runs migrate against the
// writer pool before New returns. New fails if the migration fails.
func WithMigration(migrate func(*sql.DB) error) Option {
	return func(o *options) { o.wantDB = true; o.migrate = migrate }
}

// WithEventPublisher wires a signed event publisher; New fails if the broker
// is unreachable.
func WithEventPublisher() Option {
	return func(o *options) { o.wantPub = true }
}

// WithOptionalEventPublisher wires the publisher if the broker is reachable
// and degrades to a nil EventPub with a warning otherwise.
func WithOptionalEventPublisher() Option {
	return func(o *options) { o.wantPub = true; o.pubOptional = true }
}

// WithEventSubscriber wires a signed event subscriber; New fails if the
// broker is unreachable.
func WithEventSubscriber() Option {
	return func(o *options) { o.wantSub = true }
}

// WithOptionalEventSubscriber wires the subscriber if the broker is reachable
// and degrades to a nil EventSub with a warning otherwise.
func WithOptionalEventSubscriber() Option {
	return func(o *options) { o.wantSub = true; o.subOptional = true }
}

// WithAudit wires the signed audit client; New fails if the broker is
// unreachable, audit being a compliance dependency.
func WithAudit() Option {
	return func(o *options) { o.wantAudit = true }
}

// WithOptionalAudit wires the audit client if the broker is reachable and
// degrades to a nil Audit with a warning otherwise.
func WithOptionalAudit() Option {
	return func(o *options) { o.wantAudit = true; o.auditOptional = true }
}

// WithHealthCheck adds a check to the /health report. It does not gate
// readiness; only the runtime's own hard dependencies do.
func WithHealthCheck(check middleware.HealthChecker) Option {
	return func(o *options) { o.extraChecks = append(o.extraChecks, check) }
}

// App holds the wired infrastructure for one service instance.
type App struct {
	Name        string
	Port        string
	Signing     *eventbus.SignatureConfig
	DBManager   *pgcluster.ClusterManager
	RabbitNodes []string
	EventPub    eventbus.PublisherInterface
	EventSub    *amqpcluster.ClusterSubscriber
	Audit       *audit.AuditClient
	Health      *health.HealthChecker
	Tomb        *lifecycle.Manager

	pubRequired bool
	extraChecks []middleware.HealthChecker
	sqlDB       *sql.DB
	closers     []func()
}

// New wires the requested infrastructure for the named service. On error the
// already-created resources are released; callers treat an error as fatal.
func New(name string, opts ...Option) (*App, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	logging.InitializeGlobalLogger(name)

	a := &App{
		Name:        name,
		Port:        env.String("PORT", env.String("SERVICE_PORT", "8080")),
		Health:      health.NewHealthChecker(name),
		Tomb:        lifecycle.NewManager(name),
		pubRequired: o.wantPub && !o.pubOptional,
		extraChecks: o.extraChecks,
	}

	signing, err := eventbus.LoadSignatureConfig(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load event signing key: %w", err)
	}
	a.Signing = signing

	if o.wantDB {
		if err := a.setupDatabase(name, o.migrate); err != nil {
			a.Close()
			return nil, err
		}
	}

	if o.wantPub || o.wantSub || o.wantAudit {
		if err := a.setupMessaging(name, o); err != nil {
			a.Close()
			return nil, err
		}
	}

	return a, nil
}

func (a *App) setupDatabase(name string, migrate func(*sql.DB) error) error {
	dbConfig, err := pgcluster.LoadConfigFromEnv(name)
	if err != nil {
		return fmt.Errorf("failed to load database cluster configuration: %w", err)
	}
	manager, err := pgcluster.NewClusterManager(*dbConfig)
	if err != nil {
		return fmt.Errorf("failed to create database cluster manager: %w", err)
	}
	a.DBManager = manager
	a.closers = append(a.closers, func() { manager.Close() })

	sqlDB, err := manager.Writer()
	if err != nil {
		return fmt.Errorf("failed to get writer database: %w", err)
	}
	a.sqlDB = sqlDB
	a.Health.SetDatabase(sqlDB)

	if migrate != nil {
		if err := migrate(sqlDB); err != nil {
			return fmt.Errorf("failed to migrate database: %w", err)
		}
	}

	return nil
}

func (a *App) setupMessaging(name string, o options) error {
	rabbitConfig, err := amqpcluster.LoadConfigFromEnv(name)
	if err != nil {
		if messagingAllOptional(o) {
			logging.Warn("Failed to load RabbitMQ cluster configuration (messaging disabled): %v", err)
			return nil
		}
		return fmt.Errorf("failed to load RabbitMQ cluster configuration: %w", err)
	}
	a.RabbitNodes = rabbitConfig.Nodes

	if o.wantPub {
		pub, err := amqpcluster.NewClusterPublisher(rabbitConfig.Nodes, name, a.Signing)
		if err != nil {
			if !o.pubOptional {
				return fmt.Errorf("failed to initialize cluster event publisher: %w", err)
			}
			logging.Warn("Failed to create cluster event publisher (event publishing disabled): %v", err)
		} else {
			a.EventPub = pub
			a.Health.SetRabbitMQPublisher(pub)
			a.closers = append(a.closers, func() { pub.Close() })
		}
	}

	if o.wantSub {
		sub, err := amqpcluster.NewClusterSubscriber(rabbitConfig.Nodes, name, a.Signing)
		if err != nil {
			if !o.subOptional {
				return fmt.Errorf("failed to initialize cluster event subscriber: %w", err)
			}
			logging.Warn("Failed to create cluster event subscriber (event consumption disabled): %v", err)
		} else {
			a.EventSub = sub
			a.closers = append(a.closers, func() { sub.Close() })
		}
	}

	if o.wantAudit {
		client, err := audit.NewClusterAuditClient(rabbitConfig.Nodes, name, a.Signing)
		if err != nil {
			if !o.auditOptional {
				return fmt.Errorf("failed to initialize audit client: %w", err)
			}
			logging.Warn("Failed to create audit client (audit logging disabled): %v", err)
		} else {
			a.Audit = client
		}
	}

	return nil
}

// messagingAllOptional reports whether every requested messaging component
// was optional, so an unreachable broker can be downgraded to a warning.
func messagingAllOptional(o options) bool {
	if o.wantPub && !o.pubOptional {
		return false
	}
	if o.wantSub && !o.subOptional {
		return false
	}
	return !o.wantAudit || o.auditOptional
}

// SQLDB returns the writer connection pool, or nil when the service runs
// without a database.
func (a *App) SQLDB() *sql.DB { return a.sqlDB }

// Handler decorates routes with the standard middleware stack in the
// canonical order — tracing, metrics, request logging — and serves the
// operational endpoints every service must expose: /health, /ready, /metrics,
// and the profiling endpoints when the debug gate is open. Anything else
// falls through to routes.
//
// CORS is deliberately absent: the allowed origins are a deployment policy,
// so a service that needs them wraps the result in middleware.CORS itself.
func (a *App) Handler(routes http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", routes)
	mux.Handle("GET /health", middleware.HealthHandler(a.Name, a.HealthCheckers()...))
	mux.Handle("GET /ready", middleware.ReadinessHandler(a.Name, a.ReadinessCheckers()...))
	mux.Handle("GET "+middleware.MetricsPath, middleware.MetricsHandler())
	debug.Register(mux)

	var handler http.Handler = mux
	handler = middleware.RequestLogging(a.Name)(handler)
	handler = middleware.Metrics(a.Name)(handler)
	handler = middleware.Tracing()(handler)
	return handler
}

// HealthCheckers reports on every dependency the service touches, including
// ones it can run degraded without.
func (a *App) HealthCheckers() []middleware.HealthChecker {
	var checks []middleware.HealthChecker
	if a.sqlDB != nil {
		checks = append(checks, middleware.NewDatabaseHealthChecker("postgresql", a.sqlDB.Ping))
	}
	if a.EventPub != nil {
		checks = append(checks, middleware.NewEventBusHealthChecker("rabbitmq", a.EventPub.TestConnection))
	}
	return append(checks, a.extraChecks...)
}

// ReadinessCheckers gates traffic on hard dependencies only: the database,
// and the event bus when the publisher was required rather than optional.
func (a *App) ReadinessCheckers() []middleware.HealthChecker {
	var checks []middleware.HealthChecker
	if a.sqlDB != nil {
		checks = append(checks, middleware.NewDatabaseHealthChecker("postgresql", a.sqlDB.Ping))
	}
	if a.pubRequired && a.EventPub != nil {
		checks = append(checks, middleware.NewEventBusHealthChecker("rabbitmq", a.EventPub.TestConnection))
	}
	return checks
}

// DetailedHealthHandler exposes the rich health report; services typically
// mount it inside their authenticated API surface.
func (a *App) DetailedHealthHandler() http.HandlerFunc {
	return a.Health.DetailedHealthHandler()
}

// Run serves handler on the configured port with tomb-managed graceful
// shutdown. It blocks until a shutdown signal has been handled; any cleanup
// functions run during shutdown, before the tracked resources are released.
func (a *App) Run(handler http.Handler, cleanup ...func() error) {
	server := &http.Server{
		Addr:         ":" + a.Port,
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	a.Tomb.StartHTTPServer(server, a.Port)
	logging.Info("🚀 %s starting on port %s", a.Name, a.Port)
	a.Tomb.WaitForShutdownSignal(server, cleanup...)
}

// Close releases everything New created, in reverse order. Safe to call after
// a failed New and idempotent enough to defer immediately after it.
func (a *App) Close() {
	for i := len(a.closers) - 1; i >= 0; i-- {
		a.closers[i]()
	}
	a.closers = nil
}
