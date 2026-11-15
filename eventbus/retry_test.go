package eventbus

import (
	"errors"
	"fmt"
	"io/fs"
	"testing"
)

func TestAMarkOverridesTheErrorText(t *testing.T) {
	if !isRetryableError(Retryable(errors.New("invalid order: the stock service did not answer"))) {
		t.Error("a Retryable error whose text says \"invalid\" was not retried")
	}
	if isRetryableError(Permanent(errors.New("timeout field out of range"))) {
		t.Error("a Permanent error whose text says \"timeout\" was retried")
	}
}

func TestMarksSurviveWrapping(t *testing.T) {
	cause := fs.ErrNotExist
	err := fmt.Errorf("handle order: %w", fmt.Errorf("load: %w", Retryable(cause)))

	if retry, decided := IsRetryable(err); !retry || !decided {
		t.Errorf("IsRetryable through two %%w wraps = %v, %v; want true, true", retry, decided)
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Error("the mark hides the cause from errors.Is")
	}
	var pathErr *fs.PathError
	if errors.As(Permanent(&fs.PathError{Op: "open", Path: "x", Err: cause}), &pathErr) == false {
		t.Error("the mark hides the cause from errors.As")
	}
	if Retryable(nil) != nil || Permanent(nil) != nil {
		t.Error("marking nil is not nil")
	}
}

func TestTheOutermostMarkCounts(t *testing.T) {
	err := Permanent(fmt.Errorf("gave up: %w", Retryable(errors.New("connection refused"))))
	if retry, _ := IsRetryable(err); retry {
		t.Error("an inner Retryable overrode the outer Permanent")
	}
}

func TestAnUnmarkedErrorIsUndecided(t *testing.T) {
	if _, decided := IsRetryable(errors.New("connection refused")); decided {
		t.Error("an unmarked error counts as decided")
	}
}

// Today's word lists, kept as they are for unmarked errors until the
// fallback is removed.
func TestUnmarkedErrorsFollowTheOldWordList(t *testing.T) {
	cases := map[string]bool{
		"connection refused":         true,
		"read TIMEOUT":               true,
		"network unreachable":        true,
		"dial tcp 10.0.0.1:5432":     true,
		"invalid order id":           false,
		"missing field customer":     false,
		"malformed payload":          false,
		"something else went wrong":  false,
		"invalid connection setting": true, // "connection" is checked first
	}
	for text, want := range cases {
		if got := isRetryableError(errors.New(text)); got != want {
			t.Errorf("isRetryableError(%q) = %v, want %v", text, got, want)
		}
	}
}
