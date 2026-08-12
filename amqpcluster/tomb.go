package amqpcluster

import (
	"context"
	"sync"

	"github.com/dobrevit/svckit/logging"
)

// TombManager provides graceful background task management for RabbitMQ cluster components
// This is a simplified version that follows the same patterns as the platform's tomb manager
type TombManager struct {
	name   string
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewTombManager creates a new background task manager for RabbitMQ operations
func NewTombManager(serviceName string) *TombManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &TombManager{
		name:   serviceName,
		ctx:    ctx,
		cancel: cancel,
	}
}

// StartBackgroundTask starts a background task with proper lifecycle management
func (tm *TombManager) StartBackgroundTask(taskFn func(ctx context.Context), taskName string) {
	tm.wg.Add(1)
	go func() {
		defer tm.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				logging.Error("Background task %s panicked in %s: %v", taskName, tm.name, r)
			}
		}()

		logging.Info("Starting background task: %s for %s", taskName, tm.name)
		taskFn(tm.ctx)
		logging.Info("Background task completed: %s for %s", taskName, tm.name)
	}()
}

// StartEventSubscriber starts an event subscriber as a background task
func (tm *TombManager) StartEventSubscriber(subscriberFn func() error, subscriberName string) {
	tm.StartBackgroundTask(func(ctx context.Context) {
		// Retry loop for subscriber with context cancellation
		for {
			select {
			case <-ctx.Done():
				logging.Info("Event subscriber %s stopped due to context cancellation", subscriberName)
				return
			default:
				if err := subscriberFn(); err != nil {
					logging.Error("Event subscriber %s failed: %v, will retry", subscriberName, err)
					// Simple retry with backoff
					select {
					case <-ctx.Done():
						return
					case <-context.Background().Done():
						return
					}
				}
			}
		}
	}, subscriberName+" subscriber")
}

// Context returns the cancellation context for manual task management
func (tm *TombManager) Context() context.Context {
	return tm.ctx
}

// WaitGroup returns the wait group for manual task management
func (tm *TombManager) WaitGroup() *sync.WaitGroup {
	return &tm.wg
}

// Shutdown gracefully stops all background tasks
func (tm *TombManager) Shutdown() {
	logging.Info("Shutting down background tasks for %s", tm.name)

	// Cancel all background tasks
	tm.cancel()

	// Wait for all tasks to complete
	tm.wg.Wait()

	logging.Info("All background tasks stopped for %s", tm.name)
}

// IsShuttingDown returns true if the tomb manager is shutting down
func (tm *TombManager) IsShuttingDown() bool {
	select {
	case <-tm.ctx.Done():
		return true
	default:
		return false
	}
}
