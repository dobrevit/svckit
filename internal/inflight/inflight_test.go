package inflight

import (
	"context"
	"errors"
	"testing"
	"time"
)

// unset is the parent a subscriber passes when SubscribeOptions.Context was
// left out: nil, which HandlerContext treats as context.Background().
var unset context.Context

func TestDrainWaitsForAHandlerThatFinishes(t *testing.T) {
	tr := New()
	if !tr.Begin() {
		t.Fatal("Begin refused before Stop")
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		tr.End()
	}()

	if !tr.Drain(time.Second) {
		t.Error("Drain gave up on a handler that finished within the timeout")
	}
	if tr.Cancelled().Err() == nil {
		// Drained cleanly; cancelling afterwards is harmless and releases
		// the context.
		t.Error("the handlers' context was left open after Drain")
	}
}

func TestDrainCancelsAHandlerThatOutlastsTheTimeout(t *testing.T) {
	tr := New()
	tr.Begin()
	ctx, stop := tr.HandlerContext(unset, 0)
	defer stop()
	go func() {
		<-ctx.Done()
		tr.End()
	}()

	start := time.Now()
	if !tr.Drain(100 * time.Millisecond) {
		t.Error("a handler that honours its context did not return after the cancellation")
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond || elapsed > 100*time.Millisecond+Grace {
		t.Errorf("Drain took %v", elapsed)
	}
}

func TestDrainIsBoundedByAHandlerThatIgnoresItsContext(t *testing.T) {
	tr := New()
	tr.Begin()
	defer tr.End()

	start := time.Now()
	if tr.Drain(50 * time.Millisecond) {
		t.Error("Drain reported a clean stop with a handler still running")
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond+Grace+200*time.Millisecond {
		t.Errorf("Drain took %v, want at most the timeout plus Grace", elapsed)
	}
}

func TestNoHandlerIsAdmittedAfterStop(t *testing.T) {
	tr := New()
	tr.Stop()
	if tr.Begin() {
		t.Error("Begin admitted a handler after Stop")
	}
}

func TestHandlerContext(t *testing.T) {
	tr := New()

	ctx, stop := tr.HandlerContext(unset, 20*time.Millisecond)
	<-ctx.Done()
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Errorf("a handler past its timeout sees %v", ctx.Err())
	}
	stop()

	parent, cancelParent := context.WithCancel(context.Background())
	ctx, stop = tr.HandlerContext(parent, 0)
	defer stop()
	cancelParent()
	if ctx.Err() == nil {
		t.Error("cancelling the parent did not reach the handler")
	}
}
