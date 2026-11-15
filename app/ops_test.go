package app_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dobrevit/svckit/app"
	"github.com/dobrevit/svckit/debug"
)

func get(h http.Handler, path string) int {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w.Code
}

// A service whose routes are exposed beyond the cluster exposed /metrics and,
// at debug level, pprof with them.
func TestWithAnOpsListenerMetricsAndPprofLeaveTheMainHandler(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")
	a := &app.App{Name: "test-service", OpsAddr: ":9090"}
	main := a.Handler(http.NotFoundHandler())
	ops := a.OpsHandler()

	for _, path := range []string{"/metrics", debug.Prefix} {
		if code := get(main, path); code != http.StatusNotFound {
			t.Errorf("main %s = %d, want 404", path, code)
		}
		if code := get(ops, path); code != http.StatusOK {
			t.Errorf("ops %s = %d, want 200", path, code)
		}
	}
	// Orchestrators probe the main listener.
	for _, path := range []string{"/health", "/ready"} {
		if code := get(main, path); code != http.StatusOK {
			t.Errorf("main %s = %d, want 200", path, code)
		}
	}
}

func TestPprofIsOnlyOnTheOpsListenerAtDebugLevel(t *testing.T) {
	t.Setenv("LOG_LEVEL", "info")
	a := &app.App{Name: "test-service", OpsAddr: ":9090"}
	if code := get(a.OpsHandler(), debug.Prefix); code != http.StatusNotFound {
		t.Errorf("ops %s at info level = %d, want 404", debug.Prefix, code)
	}
}

func TestWithoutAnOpsListenerNothingMoves(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")
	a := &app.App{Name: "test-service"}
	main := a.Handler(http.NotFoundHandler())
	for _, path := range []string{"/metrics", debug.Prefix} {
		if code := get(main, path); code != http.StatusOK {
			t.Errorf("main %s = %d, want 200 as before", path, code)
		}
	}
}

func TestNewRefusesAnOpsListenerOnTheMainPort(t *testing.T) {
	t.Setenv("PORT", "8080")
	t.Setenv("OPS_ADDR", "")
	_, err := app.New("test-service", app.WithOpsListener(":8080"))
	if err == nil || !strings.Contains(err.Error(), "own port") {
		t.Errorf("New with the ops listener on the main port: %v", err)
	}
}

func TestNewTakesTheOpsListenerFromTheEnvironment(t *testing.T) {
	t.Setenv("PORT", "8080")
	t.Setenv("OPS_ADDR", "127.0.0.1:9090")
	a, err := app.New("test-service")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer a.Close()
	if a.OpsAddr != "127.0.0.1:9090" {
		t.Errorf("OpsAddr = %q, want OPS_ADDR", a.OpsAddr)
	}

	a, err = app.New("test-service", app.WithOpsListener(":9191"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer a.Close()
	if a.OpsAddr != ":9191" {
		t.Errorf("OpsAddr = %q, want the option over OPS_ADDR", a.OpsAddr)
	}
}

// A service with no key configured used to start on a key published in
// svckit's source.
func TestWithStrictSigningRefusesToStartWithoutAKey(t *testing.T) {
	t.Setenv("OPS_ADDR", "")
	t.Setenv("EVENT_SIGNING_KEY_REQUIRED", "")
	t.Setenv("EVENT_SIGNING_KEY", "")

	if _, err := app.New("test-service", app.WithStrictSigning()); err == nil {
		t.Error("New with WithStrictSigning and no key succeeded")
	}

	a, err := app.New("test-service")
	if err != nil {
		t.Fatalf("New without the option, and no key: %v (it should fall back as before)", err)
	}
	a.Close()

	t.Setenv("EVENT_SIGNING_KEY", "a-real-key-0123456789abcdef")
	a, err = app.New("test-service", app.WithStrictSigning())
	if err != nil {
		t.Fatalf("New with WithStrictSigning and a key: %v", err)
	}
	a.Close()
}
