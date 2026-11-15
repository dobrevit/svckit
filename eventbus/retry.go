package eventbus

import "errors"

// Retryable marks err as worth retrying: the message is requeued (or, with a
// retry policy, retried later) instead of dead-lettered. Use it for failures
// that a later attempt can get past, such as a dependency that is down. The
// mark survives wrapping with fmt.Errorf("...: %w", err), and errors.Is and
// errors.As still see err through it. Retryable(nil) is nil.
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &markedError{err: err, retry: true}
}

// Permanent marks err as not worth retrying: the message is dead-lettered
// at once, whatever the error's text says. Use it for a message that can
// never succeed, such as one that fails validation. Permanent(nil) is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &markedError{err: err, retry: false}
}

// IsRetryable reports what a handler said about err with Retryable or
// Permanent. decided is false when err carries neither mark; the
// subscribers then fall back to matching words in the error's text, which
// is deprecated and will be removed in a later release. When marks are
// nested, the outermost one counts.
func IsRetryable(err error) (retry bool, decided bool) {
	var marked *markedError
	if errors.As(err, &marked) {
		return marked.retry, true
	}
	return false, false
}

type markedError struct {
	err   error
	retry bool
}

func (e *markedError) Error() string { return e.err.Error() }
func (e *markedError) Unwrap() error { return e.err }
