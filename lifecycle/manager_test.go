package lifecycle_test

import (
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/dobrevit/svckit/lifecycle"
)

// waitFor polls until cond holds, so a test never depends on a fixed sleep
// being long enough on a loaded machine.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestGoRunsTheGoroutineAndWaitCollectsIt(t *testing.T) {
	m := lifecycle.NewManager("test-service")

	var ran atomic.Bool
	m.Go(func() error {
		ran.Store(true)
		return nil
	})

	if err := m.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !ran.Load() {
		t.Error("the goroutine never ran")
	}
}

// The first goroutine to fail kills the group and its error is what Wait
// reports — that is the whole point of tying them together.
func TestAFailingGoroutineKillsTheGroup(t *testing.T) {
	m := lifecycle.NewManager("test-service")
	boom := errors.New("boom")

	var siblingSawDying atomic.Bool
	m.Go(func() error {
		<-m.Dying()
		siblingSawDying.Store(true)
		return nil
	})
	m.Go(func() error { return boom })

	if err := m.Wait(); !errors.Is(err, boom) {
		t.Errorf("Wait() = %v, want %v", err, boom)
	}
	if !siblingSawDying.Load() {
		t.Error("the sibling goroutine was never told the group was dying")
	}
}

func TestKillClosesDyingAndStopsWork(t *testing.T) {
	m := lifecycle.NewManager("test-service")

	var stopped atomic.Bool
	m.Go(func() error {
		<-m.Dying()
		stopped.Store(true)
		return nil
	})

	m.Kill(nil)

	if err := m.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !stopped.Load() {
		t.Error("Dying was never closed")
	}
}

func TestStartPeriodicTaskRunsUntilShutdown(t *testing.T) {
	m := lifecycle.NewManager("test-service")

	var runs atomic.Int64
	m.StartPeriodicTask(time.Millisecond, func() error {
		runs.Add(1)
		return nil
	}, "counter")

	waitFor(t, "the periodic task to run", func() bool { return runs.Load() >= 3 })

	m.Kill(nil)
	if err := m.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	// Nothing may run after the group has been collected.
	settled := runs.Load()
	time.Sleep(20 * time.Millisecond)
	if runs.Load() != settled {
		t.Errorf("the periodic task kept running after shutdown: %d then %d", settled, runs.Load())
	}
}

// A failing periodic task must not take the process down with it: one failed
// tick is not a reason to stop every other managed goroutine.
func TestAFailingPeriodicTaskKeepsTicking(t *testing.T) {
	m := lifecycle.NewManager("test-service")

	var runs atomic.Int64
	m.StartPeriodicTask(time.Millisecond, func() error {
		runs.Add(1)
		return errors.New("this tick failed")
	}, "flaky")

	waitFor(t, "the failing task to tick repeatedly", func() bool { return runs.Load() >= 3 })

	m.Kill(nil)
	if err := m.Wait(); err != nil {
		t.Errorf("a failing tick killed the group: %v", err)
	}
}

func TestStartEventSubscriberReportsAStartupFailure(t *testing.T) {
	m := lifecycle.NewManager("test-service")
	boom := errors.New("broker unreachable")

	m.StartEventSubscriber(func() error { return boom }, "orders")

	if err := m.Wait(); !errors.Is(err, boom) {
		t.Errorf("Wait() = %v, want %v", err, boom)
	}
}

// A subscriber that starts cleanly stays alive until shutdown; returning
// immediately would collect the group while the subscription is still live.
func TestAStartedEventSubscriberStaysUntilShutdown(t *testing.T) {
	m := lifecycle.NewManager("test-service")

	m.StartEventSubscriber(func() error { return nil }, "orders")

	done := make(chan struct{})
	go func() { _ = m.Wait(); close(done) }()

	select {
	case <-done:
		t.Fatal("the subscriber goroutine returned before shutdown")
	case <-time.After(50 * time.Millisecond):
	}

	m.Kill(nil)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the subscriber did not stop after Kill")
	}
}

func TestStartBotWithTombStopsTheBotOnShutdown(t *testing.T) {
	m := lifecycle.NewManager("test-service")

	var started, stopped atomic.Bool
	m.StartBotWithTomb(
		func() { started.Store(true) },
		func() { stopped.Store(true) },
	)

	waitFor(t, "the bot to start", started.Load)
	if stopped.Load() {
		t.Fatal("the bot was stopped before shutdown")
	}

	m.Kill(nil)
	if err := m.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !stopped.Load() {
		t.Error("the bot's stop function never ran")
	}
}

// A closed server is the normal outcome of shutdown, so http.ErrServerClosed
// must not be reported as a failure.
func TestStartHTTPServerTreatsAGracefulCloseAsSuccess(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}

	server := &http.Server{Handler: http.NotFoundHandler()}
	m := lifecycle.NewManager("test-service")

	// StartHTTPServer calls ListenAndServe, so hand the server its own
	// listener by serving it here and closing that instead.
	m.Go(func() error {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	if err := server.Close(); err != nil {
		t.Fatalf("closing the server: %v", err)
	}
	if err := m.Wait(); err != nil {
		t.Errorf("a graceful close was reported as an error: %v", err)
	}
}

// A port already in use must surface, not be swallowed as a normal close.
func TestStartHTTPServerReportsAListenFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	defer listener.Close()

	taken := listener.Addr().String()
	m := lifecycle.NewManager("test-service")
	m.StartHTTPServer(&http.Server{Addr: taken, Handler: http.NotFoundHandler()}, taken)

	if err := m.Wait(); err == nil {
		t.Error("binding an address already in use was reported as success")
	}
}

func TestWaitForShutdownSignalShutsTheServerDownAndRunsCleanup(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}

	server := &http.Server{Handler: http.NotFoundHandler()}
	m := lifecycle.NewManager("test-service")
	m.Go(func() error {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	var cleanupRan atomic.Bool
	returned := make(chan struct{})
	go func() {
		m.WaitForShutdownSignal(server, func() error {
			cleanupRan.Store(true)
			return nil
		})
		close(returned)
	}()

	// Give the handler time to register before raising the signal, otherwise
	// the default disposition terminates the test binary.
	time.Sleep(100 * time.Millisecond)
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signalling: %v", err)
	}

	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("WaitForShutdownSignal never returned")
	}

	if !cleanupRan.Load() {
		t.Error("the cleanup function never ran")
	}
	if _, err := net.DialTimeout("tcp", listener.Addr().String(), 200*time.Millisecond); err == nil {
		t.Error("the server is still accepting connections after shutdown")
	}
}

// A cleanup function that fails must not stop the ones after it, or prevent
// the shutdown from completing.
func TestShutdownContinuesWhenACleanupFails(t *testing.T) {
	m := lifecycle.NewManager("test-service")

	var secondRan atomic.Bool
	returned := make(chan struct{})
	go func() {
		m.WaitForShutdownSignal(nil,
			func() error { return errors.New("cleanup failed") },
			func() error { secondRan.Store(true); return nil },
		)
		close(returned)
	}()

	time.Sleep(100 * time.Millisecond)
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signalling: %v", err)
	}

	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("WaitForShutdownSignal never returned")
	}
	if !secondRan.Load() {
		t.Error("a failing cleanup function stopped the ones after it")
	}
}

// tomb closes the channel Wait blocks on only from inside a finishing
// goroutine, so a manager that never started one would wait forever — hanging
// shutdown for any service with no background work.
func TestWaitReturnsWhenNoGoroutineWasEverStarted(t *testing.T) {
	m := lifecycle.NewManager("test-service")
	m.Kill(nil)

	done := make(chan struct{})
	go func() { _ = m.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Wait blocked on a manager that never started a goroutine")
	}
}
