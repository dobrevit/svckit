package app_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dobrevit/svckit/app"
	"github.com/dobrevit/svckit/middleware"
)

// serviceRoutes stands in for whatever handler a service brings.
func serviceRoutes(reached *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*reached = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("service"))
	})
}

func TestHandlerServesTheOperationalEndpoints(t *testing.T) {
	a := &app.App{Name: "test-service"}

	var reached bool
	handler := a.Handler(serviceRoutes(&reached))

	for _, path := range []string{"/health", "/ready", "/metrics"} {
		t.Run(path, func(t *testing.T) {
			reached = false
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))

			if w.Code != http.StatusOK {
				t.Errorf("status = %d, want 200", w.Code)
			}
			if reached {
				t.Errorf("%s fell through to the service handler", path)
			}
		})
	}
}

func TestHandlerPassesEverythingElseToTheService(t *testing.T) {
	a := &app.App{Name: "test-service"}

	var reached bool
	w := httptest.NewRecorder()
	a.Handler(serviceRoutes(&reached)).
		ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/things", nil))

	if !reached {
		t.Error("a service route did not reach the service handler")
	}
	if w.Body.String() != "service" {
		t.Errorf("body = %q, want the service's response", w.Body.String())
	}
}

func TestHandlerAppliesTracing(t *testing.T) {
	a := &app.App{Name: "test-service"}

	var reached bool
	w := httptest.NewRecorder()
	a.Handler(serviceRoutes(&reached)).
		ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/things", nil))

	if w.Header().Get("X-Trace-ID") == "" {
		t.Error("no trace ID on the response — the tracing middleware did not run")
	}
}

// An unhealthy dependency must make /health fail, since that is what an
// operator and an orchestrator both read.
func TestHealthReportsAFailingExtraCheck(t *testing.T) {
	sick := middleware.NewServiceHealthChecker("dependency", "http://127.0.0.1:1",
		func() error { return errors.New("unreachable") })

	a := &app.App{Name: "test-service"}
	handler := a.Handler(http.NotFoundHandler())

	// Without the check the report is healthy.
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("baseline /health = %d, want 200", w.Code)
	}

	withCheck := middleware.HealthHandler("test-service", sick)
	w = httptest.NewRecorder()
	withCheck.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("/health with a failing dependency = %d, want 503", w.Code)
	}
}

// Readiness must not be gated on soft dependencies: a service with no
// database and an optional publisher is ready as soon as it serves.
func TestReadinessIgnoresSoftDependencies(t *testing.T) {
	a := &app.App{Name: "test-service"}

	if got := len(a.ReadinessCheckers()); got != 0 {
		t.Errorf("readiness checkers = %d, want none for a service with no hard dependencies", got)
	}
}
