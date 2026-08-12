package lifecycle

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dobrevit/svckit/logging"
	"gopkg.in/tomb.v2"
)

// Manager provides a centralized way to manage goroutines using tomb.Tomb
type Manager struct {
	tomb *tomb.Tomb
	name string
}

// NewManager creates a new tomb manager for a service
func NewManager(serviceName string) *Manager {
	return &Manager{
		tomb: &tomb.Tomb{},
		name: serviceName,
	}
}

// Go starts a new goroutine managed by the tomb
func (m *Manager) Go(f func() error) {
	m.tomb.Go(f)
}

// Kill terminates all managed goroutines
func (m *Manager) Kill(reason error) {
	m.tomb.Kill(reason)
}

// Wait waits for all goroutines to finish
func (m *Manager) Wait() error {
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

// WaitForShutdownSignal waits for interrupt signal and gracefully shuts down
func (m *Manager) WaitForShutdownSignal(server *http.Server, additionalCleanup ...func() error) {
	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logging.Info("Shutting down %s...", m.name)

	// Kill tomb to stop all goroutines
	m.Kill(nil)

	// Shutdown HTTP server if provided
	if server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			logging.Error("Server forced to shutdown: %v", err)
		}
	}

	// Run additional cleanup functions
	for _, cleanup := range additionalCleanup {
		if err := cleanup(); err != nil {
			logging.Error("Error during cleanup: %v", err)
		}
	}

	// Wait for all goroutines to finish
	if err := m.Wait(); err != nil {
		logging.Error("Error waiting for goroutines to finish: %v", err)
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
