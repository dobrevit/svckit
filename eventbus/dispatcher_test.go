package eventbus_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dobrevit/svckit/eventbus"
)

func TestDispatcherRoutesByEventType(t *testing.T) {
	var created, cancelled int

	d := eventbus.NewDispatcher("orders", "orders-queue").
		Register("order.created", func(context.Context, *eventbus.BaseEvent) error { created++; return nil }).
		Register("order.cancelled", func(context.Context, *eventbus.BaseEvent) error { cancelled++; return nil })

	ctx := t.Context()
	if err := d.Handle(ctx, eventbus.NewEvent("order.created", "orders", nil)); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := d.Handle(ctx, eventbus.NewEvent("order.cancelled", "orders", nil)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if created != 1 || cancelled != 1 {
		t.Errorf("created = %d, cancelled = %d, want 1 each", created, cancelled)
	}
}

// A handler's failure has to reach the caller, because that is what decides
// whether the message is acked or redelivered.
func TestDispatcherReturnsTheHandlersError(t *testing.T) {
	boom := errors.New("handler failed")

	d := eventbus.NewDispatcher("orders", "orders-queue").
		Register("order.created", func(context.Context, *eventbus.BaseEvent) error { return boom })

	err := d.Handle(t.Context(), eventbus.NewEvent("order.created", "orders", nil))
	if !errors.Is(err, boom) {
		t.Errorf("Handle() = %v, want %v", err, boom)
	}
}

// An event nobody registered for is not a failure: acking it is what stops it
// being redelivered forever.
func TestAnUnregisteredEventIsAcknowledged(t *testing.T) {
	d := eventbus.NewDispatcher("orders", "orders-queue").
		Register("order.created", func(context.Context, *eventbus.BaseEvent) error { return nil })

	if err := d.Handle(t.Context(), eventbus.NewEvent("something.else", "elsewhere", nil)); err != nil {
		t.Errorf("Handle() = %v, want nil for an unregistered type", err)
	}
}

func TestFallbackCatchesUnregisteredEvents(t *testing.T) {
	var seen []string

	d := eventbus.NewDispatcher("orders", "orders-queue").
		Register("order.created", func(context.Context, *eventbus.BaseEvent) error {
			seen = append(seen, "registered")
			return nil
		}).
		Fallback(func(_ context.Context, e *eventbus.BaseEvent) error {
			seen = append(seen, "fallback:"+e.Type)
			return nil
		})

	ctx := t.Context()
	_ = d.Handle(ctx, eventbus.NewEvent("order.created", "orders", nil))
	_ = d.Handle(ctx, eventbus.NewEvent("something.else", "elsewhere", nil))

	if len(seen) != 2 || seen[0] != "registered" || seen[1] != "fallback:something.else" {
		t.Errorf("routing = %v", seen)
	}
}

// A registered handler must win over the fallback, or registration would be
// pointless once a fallback exists.
func TestARegisteredHandlerWinsOverTheFallback(t *testing.T) {
	var fallbackRan bool

	d := eventbus.NewDispatcher("orders", "orders-queue").
		Register("order.created", func(context.Context, *eventbus.BaseEvent) error { return nil }).
		Fallback(func(context.Context, *eventbus.BaseEvent) error { fallbackRan = true; return nil })

	_ = d.Handle(t.Context(), eventbus.NewEvent("order.created", "orders", nil))

	if fallbackRan {
		t.Error("the fallback ran for an event that had a registered handler")
	}
}

func TestRegisteringTwiceReplacesTheHandler(t *testing.T) {
	var first, second bool

	d := eventbus.NewDispatcher("orders", "orders-queue").
		Register("order.created", func(context.Context, *eventbus.BaseEvent) error { first = true; return nil }).
		Register("order.created", func(context.Context, *eventbus.BaseEvent) error { second = true; return nil })

	_ = d.Handle(t.Context(), eventbus.NewEvent("order.created", "orders", nil))

	if first || !second {
		t.Errorf("first = %v, second = %v — the later registration should win", first, second)
	}
}

// The handler receives the event it was dispatched for, not a copy that has
// lost its payload.
func TestTheHandlerReceivesTheEvent(t *testing.T) {
	var got *eventbus.BaseEvent

	d := eventbus.NewDispatcher("orders", "orders-queue").
		Register("order.created", func(_ context.Context, e *eventbus.BaseEvent) error { got = e; return nil })

	sent := eventbus.NewEvent("order.created", "orders", map[string]any{"order_id": "o-1"})
	_ = d.Handle(t.Context(), sent)

	if got == nil {
		t.Fatal("the handler received no event")
	}
	if got.ID != sent.ID || got.Data["order_id"] != "o-1" {
		t.Errorf("handler received %+v, want %+v", got, sent)
	}
}

// The dispatcher's context must reach the handler, so cancellation and
// request-scoped values propagate.
func TestTheHandlerReceivesTheContext(t *testing.T) {
	var seen string

	d := eventbus.NewDispatcher("orders", "orders-queue").
		Register("order.created", func(ctx context.Context, _ *eventbus.BaseEvent) error {
			seen, _ = eventbus.UserID(ctx)
			return nil
		})

	ctx := eventbus.WithUserID(t.Context(), "u-1")
	_ = d.Handle(ctx, eventbus.NewEvent("order.created", "orders", nil))

	if seen != "u-1" {
		t.Errorf("the handler saw user %q, want u-1", seen)
	}
}

// WithMetrics is the migration path for switch-based handlers: it must route
// everything to the wrapped handler rather than swallowing anything.
func TestWithMetricsPassesEveryEventToTheWrappedHandler(t *testing.T) {
	var handled []string
	wrapped := eventbus.WithMetrics("orders", "orders-queue",
		handlerFunc(func(_ context.Context, e *eventbus.BaseEvent) error {
			handled = append(handled, e.Type)
			return nil
		}))

	ctx := t.Context()
	for _, eventType := range []string{"order.created", "order.cancelled", "anything.at.all"} {
		if err := wrapped.Handle(ctx, eventbus.NewEvent(eventType, "orders", nil)); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	}

	if len(handled) != 3 {
		t.Errorf("the wrapped handler saw %v, want all three events", handled)
	}
}

func TestWithMetricsPropagatesTheHandlersError(t *testing.T) {
	boom := errors.New("handler failed")
	wrapped := eventbus.WithMetrics("orders", "orders-queue",
		handlerFunc(func(context.Context, *eventbus.BaseEvent) error { return boom }))

	if err := wrapped.Handle(t.Context(), eventbus.NewEvent("order.created", "orders", nil)); !errors.Is(err, boom) {
		t.Errorf("Handle() = %v, want %v", err, boom)
	}
}

// handlerFunc adapts a function to the EventHandler interface.
type handlerFunc func(context.Context, *eventbus.BaseEvent) error

func (f handlerFunc) Handle(ctx context.Context, event *eventbus.BaseEvent) error {
	return f(ctx, event)
}
