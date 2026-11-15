package eventbus

import "context"

// ctxKey is the unexported key type for values this package attaches to a
// handler's context. A private type keeps the keys from colliding with values
// written by other packages — a bare string key cannot make that promise.
type ctxKey int

const (
	keyUserID ctxKey = iota
	keyBroadcast
	keyAttempt
)

// WithUserID returns a context carrying the user a message was published on
// behalf of, taken from the message's user_id header.
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, keyUserID, userID)
}

// UserID returns the user a message was published on behalf of, if the
// publisher set one.
func UserID(ctx context.Context) (string, bool) {
	userID, ok := ctx.Value(keyUserID).(string)
	return userID, ok && userID != ""
}

// WithAttempt returns a context carrying the attempt a handler call is: 1
// for a message's first delivery, 2 for its first retry, and so on. The
// subscribers set it; it is exported for tests and for other subscriber
// implementations.
func WithAttempt(ctx context.Context, attempt int) context.Context {
	return context.WithValue(ctx, keyAttempt, attempt)
}

// Attempt returns which attempt at handling the message this is: 1 for the
// first delivery, and with a RetryPolicy, 2 for the first retry and so on.
// A handler can use it to log, or to give up early with Permanent. It is 1
// when the subscriber set none.
func Attempt(ctx context.Context) int {
	if attempt, ok := ctx.Value(keyAttempt).(int); ok && attempt > 0 {
		return attempt
	}
	return 1
}

// WithBroadcast returns a context carrying the broadcast envelope — the
// originating exchange, publishing instance and headers — that delivered the
// event.
func WithBroadcast(ctx context.Context, event *BroadcastEvent) context.Context {
	return context.WithValue(ctx, keyBroadcast, event)
}

// Broadcast returns the broadcast envelope that delivered the event, if it
// arrived over a broadcast exchange rather than a direct subscription.
func Broadcast(ctx context.Context) (*BroadcastEvent, bool) {
	event, ok := ctx.Value(keyBroadcast).(*BroadcastEvent)
	return event, ok && event != nil
}
