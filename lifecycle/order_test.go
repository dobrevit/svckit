package lifecycle_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dobrevit/svckit/lifecycle"
)

// serve starts server on a fresh local port under m and returns its URL.
func serve(t *testing.T, m *lifecycle.Manager, server *http.Server) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m.Go(func() error {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	return "http://" + listener.Addr().String()
}

// shutDown runs WaitForShutdownSignal, raises SIGTERM once it is listening,
// and reports when it returned.
func shutDown(t *testing.T, m *lifecycle.Manager, server *http.Server, cleanup ...func() error) (signalled, returned time.Time) {
	t.Helper()
	done := make(chan time.Time, 1)
	go func() {
		m.WaitForShutdownSignal(server, cleanup...)
		done <- time.Now()
	}()
	// The handler must be registered before the signal, or the default
	// disposition terminates the test binary.
	time.Sleep(100 * time.Millisecond)
	signalled = time.Now()
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case returned = <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("WaitForShutdownSignal never returned")
	}
	return signalled, returned
}

// The background work used to be cancelled first, so a request in flight
// at the signal lost the components it relied on while it was still
// running.
func TestARequestInFlightFinishesBeforeBackgroundWorkIsCancelled(t *testing.T) {
	m := lifecycle.NewManager("test-service")

	var mu sync.Mutex
	var requestDone, backgroundCancelled time.Time
	inRequest := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(inRequest)
		time.Sleep(300 * time.Millisecond)
		mu.Lock()
		requestDone = time.Now()
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})}
	url := serve(t, m, server)
	m.Go(func() error {
		<-m.Dying()
		mu.Lock()
		backgroundCancelled = time.Now()
		mu.Unlock()
		return nil
	})

	status := make(chan int, 1)
	go func() {
		resp, err := http.Get(url)
		if err != nil {
			status <- 0
			return
		}
		_ = resp.Body.Close()
		status <- resp.StatusCode
	}()
	<-inRequest

	shutDown(t, m, server)

	if code := <-status; code != http.StatusOK {
		t.Fatalf("the in-flight request got %d, want it to complete", code)
	}
	mu.Lock()
	defer mu.Unlock()
	if backgroundCancelled.Before(requestDone) {
		t.Errorf("background work was cancelled %v before the in-flight request finished", requestDone.Sub(backgroundCancelled))
	}
}

func TestARequestOutlastingTheDrainIsCut(t *testing.T) {
	m := lifecycle.NewManager("test-service", lifecycle.WithDrainTimeout(200*time.Millisecond))
	inRequest := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(inRequest)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})}
	url := serve(t, m, server)

	go func() {
		if resp, err := http.Get(url); err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-inRequest

	signalled, returned := shutDown(t, m, server)
	if took := returned.Sub(signalled); took > 2*time.Second {
		t.Errorf("shutdown took %v with a 200ms drain; the long request was not cut", took)
	}
}

func TestShutdownStaysWithinTheBudget(t *testing.T) {
	m := lifecycle.NewManager("test-service", lifecycle.WithShutdownBudget(500*time.Millisecond))
	stuck := make(chan struct{})
	defer close(stuck)
	m.Go(func() error {
		<-stuck // ignores Dying
		return nil
	})

	var cleaned bool
	signalled, returned := shutDown(t, m, nil, func() error { cleaned = true; return nil })
	if took := returned.Sub(signalled); took > 1500*time.Millisecond {
		t.Errorf("shutdown took %v with a 500ms budget", took)
	}
	if !cleaned {
		t.Error("the cleanup did not run after the budget ran out")
	}
}

func TestACustomOrderRunsAsGiven(t *testing.T) {
	var steps []string
	var m *lifecycle.Manager
	m = lifecycle.NewManager("test-service",
		lifecycle.WithShutdownBudget(time.Second),
		lifecycle.WithShutdownOrder(func(ctx context.Context) error {
			if _, ok := ctx.Deadline(); !ok {
				steps = append(steps, "no deadline")
			}
			steps = append(steps, "kill")
			m.Kill(nil)
			steps = append(steps, "wait")
			return m.Wait()
		}))
	m.Go(func() error { <-m.Dying(); return nil })

	var defaultCleanupRan bool
	shutDown(t, m, nil, func() error { defaultCleanupRan = true; return nil })

	if len(steps) != 2 || steps[0] != "kill" || steps[1] != "wait" {
		t.Errorf("steps = %v, want the custom order, with the budget as its deadline", steps)
	}
	if defaultCleanupRan {
		t.Error("the default sequence ran as well as the custom order")
	}
}
