package rediscluster

import (
	"context"
	"sync"

	"github.com/dobrevit/svckit/logging"
)

// TombManager provides graceful background task management for Redis cluster components
// This follows the same patterns as the platform's database and rabbitmq cluster implementations
type TombManager struct {
	name   string
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewTombManager creates a new background task manager for Redis operations
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

		logging.Debug("Starting Redis background task: %s for %s", taskName, tm.name)
		taskFn(tm.ctx)
		logging.Debug("Redis background task completed: %s for %s", taskName, tm.name)
	}()
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
	logging.Info("Shutting down Redis background tasks for %s", tm.name)

	// Cancel all background tasks
	tm.cancel()

	// Wait for all tasks to complete
	tm.wg.Wait()

	logging.Info("All Redis background tasks stopped for %s", tm.name)
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
