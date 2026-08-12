package debug_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dobrevit/svckit/debug"
)

func TestHandlerIsNilUnlessDebugLevel(t *testing.T) {
	for _, level := range []string{"", "info", "warn", "DEBUGGING"} {
		t.Setenv("LOG_LEVEL", level)
		if debug.Enabled() {
			t.Errorf("LOG_LEVEL=%q: Enabled() = true, want false", level)
		}
		if debug.Handler() != nil {
			t.Errorf("LOG_LEVEL=%q: Handler() is non-nil, want nil", level)
		}
	}
}

func TestDebugLevelIsCaseInsensitive(t *testing.T) {
	for _, level := range []string{"debug", "DEBUG", "Debug"} {
		t.Setenv("LOG_LEVEL", level)
		if !debug.Enabled() {
			t.Errorf("LOG_LEVEL=%q: Enabled() = false, want true", level)
		}
	}
}

func TestRegisterServesProfilesOnlyWhenEnabled(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")
	mux := http.NewServeMux()
	if !debug.Register(mux) {
		t.Fatal("Register reported the endpoints were not mounted")
	}

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, debug.Prefix+"goroutine?debug=1", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("X-Debug-Mode"); got != "enabled" {
		t.Errorf("X-Debug-Mode = %q, want %q", got, "enabled")
	}
}

func TestRegisterIsANoopWhenDisabled(t *testing.T) {
	t.Setenv("LOG_LEVEL", "info")
	mux := http.NewServeMux()
	if debug.Register(mux) {
		t.Fatal("Register mounted the endpoints while profiling is disabled")
	}

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, debug.Prefix, nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}
