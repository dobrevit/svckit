package eventbus

import (
	"context"
	"time"

	"github.com/dobrevit/svckit/logging"
)

// HandlerFunc handles a single event type.
type HandlerFunc func(ctx context.Context, event *BaseEvent) error

// Dispatcher routes events to per-type handlers and records processing
// metrics with the real outcome. It replaces the hand-rolled
// switch-plus-deferred-metrics prologue that every service copied — which
// hardcoded "success" and therefore reported a 100% success rate to
// Prometheus even when handlers returned errors.
//
// Dispatcher implements EventHandler, so it plugs directly into
// Subscriber.Subscribe:
//
//	d := events.NewDispatcher("skills-service", "skills-queue").
//		Register(events.EventUserRegistered, h.handleUserRegistered).
//		Register(events.EventSkillAdded, h.handleSkillAdded)
//	subscriber.Subscribe(events.EventUserRegistered, "skills-queue", d)
type Dispatcher struct {
	service  string
	queue    string
	handlers map[string]HandlerFunc
	fallback HandlerFunc
}

// NewDispatcher creates a dispatcher for the given service and queue; the
// names label the processing metrics.
func NewDispatcher(service, queue string) *Dispatcher {
	return &Dispatcher{
		service:  service,
		queue:    queue,
		handlers: make(map[string]HandlerFunc),
	}
}

// Register maps an event type to its handler and returns the dispatcher for chaining.
func (d *Dispatcher) Register(eventType string, fn HandlerFunc) *Dispatcher {
	d.handlers[eventType] = fn
	return d
}

// Fallback sets the handler for event types with no Register entry. Without
// one, unregistered events are logged, recorded with status "unhandled" and
// acknowledged (nil error).
func (d *Dispatcher) Fallback(fn HandlerFunc) *Dispatcher {
	d.fallback = fn
	return d
}

// WithMetrics wraps an existing EventHandler (typically a switch-based
// handler) so that processing duration and the real outcome are recorded,
// without restructuring the handler into per-type registrations. It is the
// minimal migration path away from the copy-pasted deferred-metrics prologue
// that always reported "success".
func WithMetrics(service, queue string, handler EventHandler) EventHandler {
	return NewDispatcher(service, queue).Fallback(handler.Handle)
}

// Handle implements EventHandler. It routes the event, then records the
// processing duration and outcome ("success", "error" or "unhandled").
func (d *Dispatcher) Handle(ctx context.Context, event *BaseEvent) error {
	start := time.Now()

	fn, ok := d.handlers[event.Type]
	if !ok {
		fn = d.fallback
	}
	if fn == nil {
		logging.Debug("%s: unhandled event type %s from source %s", d.service, event.Type, event.Source)
		RecordEventProcessed(event.Type, d.service, d.queue, "unhandled")
		return nil
	}

	err := fn(ctx, event)

	status := "success"
	if err != nil {
		status = "error"
	}
	RecordEventProcessed(event.Type, d.service, d.queue, status)
	EventProcessingDuration.WithLabelValues(event.Type, d.service, d.queue).Observe(time.Since(start).Seconds())

	return err
}
