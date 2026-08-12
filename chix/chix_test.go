package chix_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dobrevit/svckit/chix"
	"github.com/dobrevit/svckit/logging"
	"github.com/dobrevit/svckit/middleware"
	"github.com/go-chi/chi/v5"
)

// The label has to be the template. One value per ID is how a metrics bill
// grows with traffic instead of with the number of routes.
func TestRouteLabelsByTemplateNotByPath(t *testing.T) {
	var seen string

	r := chi.NewRouter()
	r.Use(chix.Route())
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req)
			seen = middleware.Route(req) // read after routing, as metrics do
		})
	})
	r.Get("/orders/{id}/items/{itemID}", func(w http.ResponseWriter, r *http.Request) {})

	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/orders/42/items/7", nil))

	if seen != "/orders/{id}/items/{itemID}" {
		t.Errorf("route label = %q, want the template", seen)
	}
}

// Two different IDs must produce one label, not two.
func TestDifferentIDsShareOneLabel(t *testing.T) {
	labels := map[string]bool{}

	r := chi.NewRouter()
	r.Use(chix.Route())
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req)
			labels[middleware.Route(req)] = true
		})
	})
	r.Get("/orders/{id}", func(w http.ResponseWriter, r *http.Request) {})

	for _, path := range []string{"/orders/1", "/orders/2", "/orders/3"} {
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}

	if len(labels) != 1 {
		t.Errorf("labels = %v, want exactly one", labels)
	}
}

// An unmatched request has no template; falling back to the path is right,
// and panicking would be wrong.
func TestUnmatchedRequestsFallBackToThePath(t *testing.T) {
	var seen string

	r := chi.NewRouter()
	r.Use(chix.Route())
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req)
			seen = middleware.Route(req)
		})
	})
	r.Get("/orders/{id}", func(w http.ResponseWriter, r *http.Request) {})

	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/nothing/here", nil))

	if seen != "/nothing/here" {
		t.Errorf("route = %q, want the path for an unmatched request", seen)
	}
}

// Used outside a chi router there is no route context; the request must still
// be served.
func TestSurvivesOutsideAChiRouter(t *testing.T) {
	var reached bool
	h := chix.Route()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))

	if !reached || w.Code != http.StatusOK {
		t.Errorf("reached = %v, status = %d", reached, w.Code)
	}
}

// The point of the package: svckit's middleware runs unmodified under chi.
func TestSvckitMiddlewareComposesWithChi(t *testing.T) {
	var buf strings.Builder
	prev := slog.Default()
	slog.SetDefault(slog.New(logging.NewHandler(logging.Options{
		Service: "orders", Writer: &buf, Level: slog.LevelDebug,
	})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	r := chi.NewRouter()
	r.Use(chix.Route())
	r.Use(middleware.Tracing())
	r.Use(middleware.Metrics("orders"))
	r.Use(middleware.RequestLogging("orders"))
	r.Get("/orders/{id}", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/orders/42", nil))

	if w.Header().Get("traceparent") == "" {
		t.Error("tracing did not run")
	}
	if got := buf.String(); !strings.Contains(got, "route=/orders/{id}") {
		t.Errorf("access log did not carry the templated route: %q", got)
	}
}
