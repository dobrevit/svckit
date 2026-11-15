package lifecycle

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dobrevit/svckit/logging"
	"gopkg.in/tomb.v2"
)

// Manager provides a centralized way to manage goroutines using tomb.Tomb
type Manager struct {
	tomb *tomb.Tomb
	name string

	// started records whether any goroutine was ever handed to the tomb.
	// See Wait for why that has to be tracked.
	started atomic.Bool

	drainTimeout   time.Duration
	shutdownBudget time.Duration
	shutdownOrder  func(ctx context.Context) error
}

const (
	// DefaultDrainTimeout is how long in-flight HTTP requests get to finish
	// at shutdown before they are cut.
	DefaultDrainTimeout = 5 * time.Second

	// DefaultShutdownBudget bounds the whole shutdown: the drain, the
	// background work and the cleanup.
	DefaultShutdownBudget = 30 * time.Second
)

// Option adjusts a Manager when it is created.
type Option func(*Manager)

// WithDrainTimeout sets how long in-flight HTTP requests get to finish at
// shutdown (default DefaultDrainTimeout); requests still running then are
// cut.
func WithDrainTimeout(d time.Duration) Option {
	return func(m *Manager) { m.drainTimeout = d }
}

// WithShutdownBudget bounds the whole shutdown (default
// DefaultShutdownBudget). Background work still running when the budget is
// spent is abandoned, so the process exits on time.
func WithShutdownBudget(d time.Duration) Option {
	return func(m *Manager) { m.shutdownBudget = d }
}

// WithShutdownOrder replaces the shutdown sequence with order, which runs
// with a context bounded by the shutdown budget. The server and cleanup
// functions given to WaitForShutdownSignal are then not used; order does
// all of it. For the order before v0.1.5, which cancelled background work
// before draining HTTP:
//
//	lifecycle.WithShutdownOrder(func(ctx context.Context) error {
//		m.Kill(nil)
//		_ = server.Shutdown(ctx)
//		// cleanup...
//		return m.Wait()
//	})
func WithShutdownOrder(order func(ctx context.Context) error) Option {
	return func(m *Manager) { m.shutdownOrder = order }
}

// NewManager creates a new tomb manager for a service
func NewManager(serviceName string, opts ...Option) *Manager {
	m := &Manager{
		tomb:           &tomb.Tomb{},
		name:           serviceName,
		drainTimeout:   DefaultDrainTimeout,
		shutdownBudget: DefaultShutdownBudget,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Go starts a new goroutine managed by the tomb
func (m *Manager) Go(f func() error) {
	m.started.Store(true)
	m.tomb.Go(f)
}

// Kill terminates all managed goroutines
func (m *Manager) Kill(reason error) {
	m.tomb.Kill(reason)
}

// Wait waits for all managed goroutines to finish.
//
// It returns immediately when none were ever started. tomb closes the channel
// Wait blocks on only from inside a finishing goroutine, so waiting on a tomb
// that never had one blocks forever — which would hang shutdown for a service
// that registered no background work.
func (m *Manager) Wait() error {
	if !m.started.Load() {
		return nil
	}
	return m.tomb.Wait()
}

// Dying returns a channel that is closed when the tomb is dying
func (m *Manager) Dying() <-chan struct{} {
	return m.tomb.Dying()
}

// StartHTTPServer starts an HTTP server managed by tomb
func (m *Manager) StartHTTPServer(server *http.Server, port string) {
	m.Go(func() error {
		logging.Info("%s started on port %s", m.name, port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})
}

// StartEventSubscriber starts an event subscriber managed by tomb
func (m *Manager) StartEventSubscriber(subscriberFunc func() error, description string) {
	m.Go(func() error {
		logging.Info("Starting %s event subscriber...", description)
		if err := subscriberFunc(); err != nil {
			logging.Error("Failed to start %s event subscriber: %v", description, err)
			return err
		}
		logging.Info("Successfully started %s event subscriber", description)
		<-m.Dying()
		return nil
	})
}

// StartPeriodicTask starts a periodic task managed by tomb
func (m *Manager) StartPeriodicTask(interval time.Duration, taskFunc func() error, description string) {
	m.Go(func() error {
		return m.runPeriodicTask(interval, taskFunc, description)
	})
}

// runPeriodicTask runs a task periodically until the tomb is dying
func (m *Manager) runPeriodicTask(interval time.Duration, taskFunc func() error, description string) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logging.Info("Starting %s periodic task (interval: %s)", description, interval)

	for {
		select {
		case <-m.Dying():
			logging.Info("%s periodic task shutting down...", description)
			return nil
		case <-ticker.C:
			if err := taskFunc(); err != nil {
				logging.Error("Error in %s periodic task: %v", description, err)
				// Don't return error to avoid killing the entire tomb
			}
		}
	}
}

// WaitForShutdownSignal waits for SIGINT or SIGTERM and then shuts down in
// the order the package doc describes:
//
//  1. the HTTP server stops accepting connections and in-flight requests get
//     the drain timeout (WithDrainTimeout) to finish; any still running then
//     are cut;
//  2. the background work is cancelled (Kill), and waited for within what is
//     left of the shutdown budget (WithShutdownBudget);
//  3. the cleanup functions run, in order; a failing one does not stop the
//     rest.
//
// Draining first means a request still running can rely on the background
// components it uses (a publisher, a queue consumer) until it finishes. The
// whole shutdown stays within the budget: background work that ignores its
// cancellation is abandoned, and logged. WithShutdownOrder replaces the
// sequence.
func (m *Manager) WaitForShutdownSignal(server *http.Server, additionalCleanup ...func() error) {
	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(quit)
	<-quit
	logging.Info("Shutting down %s...", m.name)

	ctx, cancel := context.WithTimeout(context.Background(), m.shutdownBudget)
	defer cancel()

	if m.shutdownOrder != nil {
		if err := m.shutdownOrder(ctx); err != nil {
			logging.Error("Shutdown of %s failed: %v", m.name, err)
		}
		logging.Info("%s exiting", m.name)
		return
	}

	// 1. Drain HTTP.
	if server != nil {
		drainCtx, cancelDrain := context.WithTimeout(ctx, m.drainTimeout)
		if err := server.Shutdown(drainCtx); err != nil {
			logging.Warn("Requests still running after the %v drain were cut: %v", m.drainTimeout, err)
			_ = server.Close()
		}
		cancelDrain()
	}

	// 2. Stop the background work, within the budget.
	m.Kill(nil)
	waited := make(chan error, 1)
	go func() { waited <- m.Wait() }()
	select {
	case err := <-waited:
		if err != nil {
			logging.Error("Error waiting for goroutines to finish: %v", err)
		}
	case <-ctx.Done():
		logging.Error("Background work of %s did not stop within the %v shutdown budget; abandoning it", m.name, m.shutdownBudget)
	}

	// 3. Run additional cleanup functions
	for _, cleanup := range additionalCleanup {
		if err := cleanup(); err != nil {
			logging.Error("Error during cleanup: %v", err)
		}
	}

	logging.Info("%s exiting", m.name)
}

// StartBotWithTomb starts a bot with tomb management
func (m *Manager) StartBotWithTomb(startFunc func(), stopFunc func()) {
	m.Go(func() error {
		logging.Info("%s bot starting...", m.name)
		startFunc()
		<-m.Dying()
		stopFunc()
		return nil
	})
}
