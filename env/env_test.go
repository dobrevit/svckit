package env_test

import (
	"testing"
	"time"

	"github.com/dobrevit/svckit/env"
)

func TestString(t *testing.T) {
	t.Setenv("ENV_TEST_STRING", "value")
	if got := env.String("ENV_TEST_STRING", "def"); got != "value" {
		t.Errorf("String set = %q, want %q", got, "value")
	}
	if got := env.String("ENV_TEST_STRING_UNSET", "def"); got != "def" {
		t.Errorf("String unset = %q, want %q", got, "def")
	}
	t.Setenv("ENV_TEST_STRING_EMPTY", "")
	if got := env.String("ENV_TEST_STRING_EMPTY", "def"); got != "def" {
		t.Errorf("String empty = %q, want %q", got, "def")
	}
}

func TestInt(t *testing.T) {
	t.Setenv("ENV_TEST_INT", "42")
	if got := env.Int("ENV_TEST_INT", 7); got != 42 {
		t.Errorf("Int set = %d, want 42", got)
	}
	if got := env.Int("ENV_TEST_INT_UNSET", 7); got != 7 {
		t.Errorf("Int unset = %d, want 7", got)
	}
	t.Setenv("ENV_TEST_INT_BAD", "not-a-number")
	if got := env.Int("ENV_TEST_INT_BAD", 7); got != 7 {
		t.Errorf("Int unparsable = %d, want 7", got)
	}
}

func TestInt64(t *testing.T) {
	t.Setenv("ENV_TEST_INT64", "9223372036854775807")
	if got := env.Int64("ENV_TEST_INT64", 1); got != 9223372036854775807 {
		t.Errorf("Int64 set = %d", got)
	}
	if got := env.Int64("ENV_TEST_INT64_UNSET", -5); got != -5 {
		t.Errorf("Int64 unset = %d, want -5", got)
	}
}

func TestBool(t *testing.T) {
	t.Setenv("ENV_TEST_BOOL", "true")
	if !env.Bool("ENV_TEST_BOOL", false) {
		t.Error("Bool true = false, want true")
	}
	t.Setenv("ENV_TEST_BOOL_ZERO", "0")
	if env.Bool("ENV_TEST_BOOL_ZERO", true) {
		t.Error("Bool 0 = true, want false")
	}
	if !env.Bool("ENV_TEST_BOOL_UNSET", true) {
		t.Error("Bool unset = false, want default true")
	}
	t.Setenv("ENV_TEST_BOOL_BAD", "yes-ish")
	if env.Bool("ENV_TEST_BOOL_BAD", false) {
		t.Error("Bool unparsable = true, want default false")
	}
}

func TestDuration(t *testing.T) {
	t.Setenv("ENV_TEST_DUR", "90s")
	if got := env.Duration("ENV_TEST_DUR", time.Minute); got != 90*time.Second {
		t.Errorf("Duration set = %v, want 90s", got)
	}
	if got := env.Duration("ENV_TEST_DUR_UNSET", time.Minute); got != time.Minute {
		t.Errorf("Duration unset = %v, want 1m", got)
	}
	t.Setenv("ENV_TEST_DUR_BAD", "ninety")
	if got := env.Duration("ENV_TEST_DUR_BAD", time.Minute); got != time.Minute {
		t.Errorf("Duration unparsable = %v, want 1m", got)
	}
}

func TestSlice(t *testing.T) {
	t.Setenv("ENV_TEST_SLICE", "a, b ,,c")
	got := env.Slice("ENV_TEST_SLICE", nil)
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("Slice = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Slice[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	def := []string{"x"}
	if got := env.Slice("ENV_TEST_SLICE_UNSET", def); len(got) != 1 || got[0] != "x" {
		t.Errorf("Slice unset = %v, want [x]", got)
	}
}
