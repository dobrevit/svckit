// Package inflight tracks the message handlers a subscriber is running, so
// that Close can stop taking new messages, let the running ones finish
// within a deadline, and then cancel them.
package inflight

import (
	"context"
	"sync"
	"time"
)

// Grace is how long Drain waits, after cancelling the handlers' context,
// for them to return. A handler that honours its context returns well within
// it and has its message requeued; one that does not is abandoned, and the
// broker requeues its message when the channel closes.
const Grace = time.Second

// Tracker counts handlers in flight. The zero value is not usable; use New.
type Tracker struct {
	mu      sync.Mutex
	stopped bool
	wg      sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

// New returns a Tracker that admits handlers until Stop.
func New() *Tracker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Tracker{ctx: ctx, cancel: cancel}
}

// Begin admits one handler, and reports false once Stop has been called;
// the caller then gives the message back instead of handling it. Every
// admitted handler must call End.
func (t *Tracker) Begin() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return false
	}
	t.wg.Add(1)
	return true
}

// End records that an admitted handler has returned.
func (t *Tracker) End() { t.wg.Done() }

// Cancelled is done once Drain has given up waiting and cancelled the
// handlers. A handler's context is derived so that it ends with it.
func (t *Tracker) Cancelled() context.Context { return t.ctx }

// Stop admits no more handlers.
func (t *Tracker) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped = true
}

// Drain stops admitting handlers, waits up to timeout for the running ones,
// then cancels them and waits up to Grace more. It reports whether every
// handler returned. It always returns within timeout plus Grace.
func (t *Tracker) Drain(timeout time.Duration) bool {
	t.Stop()

	done := make(chan struct{})
	go func() {
		t.wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		t.cancel()
		return true
	case <-timer.C:
	}

	t.cancel()
	select {
	case <-done:
		return true
	case <-time.After(Grace):
		return false
	}
}

// HandlerContext is the context one handler call runs on: derived from
// parent (context.Background() when nil), cancelled when the tracker
// cancels its handlers, and bounded by timeout when it is positive. The
// returned stop releases it.
func (t *Tracker) HandlerContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	unhook := context.AfterFunc(t.ctx, cancel)
	if timeout > 0 {
		var cancelTimeout context.CancelFunc
		ctx, cancelTimeout = context.WithTimeout(ctx, timeout)
		return ctx, func() {
			cancelTimeout()
			unhook()
			cancel()
		}
	}
	return ctx, func() {
		unhook()
		cancel()
	}
}
