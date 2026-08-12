package eventbus

import "context"

// ctxKey is the unexported key type for values this package attaches to a
// handler's context. A private type keeps the keys from colliding with values
// written by other packages — a bare string key cannot make that promise.
type ctxKey int

const (
	keyUserID ctxKey = iota
	keyBroadcast
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
