package middleware_test

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dobrevit/svckit/middleware"
)

// okHandler reports that it ran, so short-circuiting middleware is detectable.
func okHandler(ran *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*ran = true
		w.WriteHeader(http.StatusOK)
	})
}

func TestClassifyEndpoint(t *testing.T) {
	cases := []struct {
		path, method string
		want         middleware.EndpointType
	}{
		{"/health", "GET", middleware.EndpointTypeHealthy},
		{"/api/v1/iam/login", "POST", middleware.EndpointTypeAuth},
	}
	for _, tc := range cases {
		if got := middleware.ClassifyEndpoint(tc.path, tc.method); got != tc.want {
			t.Errorf("ClassifyEndpoint(%q, %q) = %q, want %q", tc.path, tc.method, got, tc.want)
		}
	}
}

func TestCORSWithOrigins(t *testing.T) {
	var ran bool
	h := middleware.CORSWithOrigins([]string{"http://allowed.example"})(okHandler(&ran))

	t.Run("allowed origin is echoed back", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/x", nil)
		r.Header.Set("Origin", "http://allowed.example")
		h.ServeHTTP(w, r)

		if got := w.Header().Get("Access-Control-Allow-Origin"); got != "http://allowed.example" {
			t.Errorf("Access-Control-Allow-Origin = %q", got)
		}
		if got := w.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
			t.Errorf("Access-Control-Allow-Credentials = %q, want true", got)
		}
	})

	t.Run("preflight is answered without reaching the handler", func(t *testing.T) {
		ran = false
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodOptions, "/x", nil)
		r.Header.Set("Origin", "http://allowed.example")
		h.ServeHTTP(w, r)

		if w.Code != http.StatusNoContent {
			t.Errorf("status = %d, want 204", w.Code)
		}
		if ran {
			t.Error("preflight reached the wrapped handler")
		}
	})

	t.Run("unknown origin falls back to the first configured origin", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/x", nil)
		r.Header.Set("Origin", "http://evil.example")
		h.ServeHTTP(w, r)

		if got := w.Header().Get("Access-Control-Allow-Origin"); got != "http://allowed.example" {
			t.Errorf("Access-Control-Allow-Origin = %q, want the configured origin", got)
		}
	})
}

func TestHealthHandler(t *testing.T) {
	healthy := middleware.NewDatabaseHealthChecker("postgresql", func() error { return nil })
	sick := middleware.NewDatabaseHealthChecker("postgresql", func() error { return errors.New("down") })

	t.Run("all checks passing reports 200", func(t *testing.T) {
		w := httptest.NewRecorder()
		middleware.HealthHandler("test-service", healthy).ServeHTTP(w, httptest.NewRequest("GET", "/health", nil))

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want 200", w.Code)
		}
		var body middleware.HealthCheckResponse
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("decoding body: %v", err)
		}
		if body.Service != "test-service" || body.Status != "healthy" {
			t.Errorf("body = %+v, want service test-service and status healthy", body)
		}
		if body.Checks["postgresql"].Status != middleware.StatusHealthy {
			t.Errorf("postgresql check = %+v", body.Checks["postgresql"])
		}
	})

	t.Run("a failing check reports 503", func(t *testing.T) {
		w := httptest.NewRecorder()
		middleware.HealthHandler("test-service", sick).ServeHTTP(w, httptest.NewRequest("GET", "/health", nil))

		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", w.Code)
		}
		if !strings.Contains(w.Body.String(), "down") {
			t.Errorf("body %q does not carry the check's message", w.Body.String())
		}
	})
}

func TestReadinessHandler(t *testing.T) {
	sick := middleware.NewEventBusHealthChecker("rabbitmq", func() error { return errors.New("no connection") })

	w := httptest.NewRecorder()
	middleware.ReadinessHandler("test-service", sick).ServeHTTP(w, httptest.NewRequest("GET", "/ready", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding body: %v", err)
	}
	if body["status"] != "not_ready" {
		t.Errorf("status field = %v, want not_ready", body["status"])
	}
}

// stubValidator accepts exactly one key.
type stubValidator struct {
	key  string
	info *middleware.ServiceKeyInfo
}

func (s stubValidator) ValidateServiceKey(apiKey string) (*middleware.ServiceKeyInfo, error) {
	if apiKey != s.key {
		return nil, errors.New("unknown key")
	}
	return s.info, nil
}

func TestServiceAuthRequired(t *testing.T) {
	validator := stubValidator{
		key:  "good-key",
		info: &middleware.ServiceKeyInfo{ServiceName: "caller", Scopes: []string{"read"}},
	}

	cases := []struct {
		name       string
		header     [2]string
		wantStatus int
		wantRan    bool
	}{
		{"no key is rejected", [2]string{"", ""}, http.StatusUnauthorized, false},
		{"bad key is rejected", [2]string{"X-API-Key", "wrong"}, http.StatusUnauthorized, false},
		{"good key passes", [2]string{"X-API-Key", "good-key"}, http.StatusOK, true},
		{"bearer token is accepted", [2]string{"Authorization", "Bearer good-key"}, http.StatusOK, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ran bool
			h := middleware.ServiceAuthRequired(validator)(okHandler(&ran))

			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/x", nil)
			if tc.header[0] != "" {
				r.Header.Set(tc.header[0], tc.header[1])
			}
			h.ServeHTTP(w, r)

			if w.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", w.Code, tc.wantStatus)
			}
			if ran != tc.wantRan {
				t.Errorf("handler ran = %v, want %v", ran, tc.wantRan)
			}
		})
	}
}

func TestServiceAuthPublishesIdentityOnContext(t *testing.T) {
	validator := stubValidator{
		key:  "good-key",
		info: &middleware.ServiceKeyInfo{ServiceName: "caller", Scopes: []string{"read"}},
	}

	var (
		gotName    string
		gotRead    bool
		gotWrite   bool
		gotService bool
	)
	h := middleware.ServiceAuthRequired(validator)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotName, _ = middleware.ServiceName(r.Context())
		gotRead = middleware.HasScope(r.Context(), "read")
		gotWrite = middleware.HasScope(r.Context(), "write")
		gotService = middleware.IsServiceAuthenticated(r.Context())
	}))

	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	r.Header.Set("X-API-Key", "good-key")
	h.ServeHTTP(httptest.NewRecorder(), r)

	if gotName != "caller" {
		t.Errorf("ServiceName = %q, want caller", gotName)
	}
	if !gotRead {
		t.Error("HasScope(read) = false, want true")
	}
	if gotWrite {
		t.Error("HasScope(write) = true, want false")
	}
	if !gotService {
		t.Error("IsServiceAuthenticated = false, want true")
	}
}

func TestWildcardScopeSatisfiesEveryCheck(t *testing.T) {
	validator := stubValidator{
		key:  "k",
		info: &middleware.ServiceKeyInfo{ServiceName: "caller", Scopes: []string{middleware.ScopeAll}},
	}

	var ran bool
	h := middleware.ServiceAuthRequired(validator)(middleware.RequireServiceScope("anything")(okHandler(&ran)))

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	r.Header.Set("X-API-Key", "k")
	h.ServeHTTP(w, r)

	if !ran {
		t.Errorf("wildcard scope was rejected: status %d, body %s", w.Code, w.Body)
	}
}

func TestRequireServiceScopeRejectsMissingScope(t *testing.T) {
	validator := stubValidator{
		key:  "k",
		info: &middleware.ServiceKeyInfo{ServiceName: "caller", Scopes: []string{"read"}},
	}

	var ran bool
	h := middleware.ServiceAuthRequired(validator)(middleware.RequireServiceScope("write")(okHandler(&ran)))

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	r.Header.Set("X-API-Key", "k")
	h.ServeHTTP(w, r)

	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", w.Code)
	}
	if ran {
		t.Error("handler ran despite the missing scope")
	}
}

func TestOptionalServiceAuthLetsAnonymousThrough(t *testing.T) {
	validator := stubValidator{key: "k", info: &middleware.ServiceKeyInfo{ServiceName: "caller"}}

	var ran bool
	h := middleware.OptionalServiceAuth(validator)(okHandler(&ran))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))

	if !ran {
		t.Error("anonymous request was blocked by OptionalServiceAuth")
	}
}

func TestOptionalServiceAuthStillRejectsABadKey(t *testing.T) {
	validator := stubValidator{key: "k", info: &middleware.ServiceKeyInfo{ServiceName: "caller"}}

	var ran bool
	h := middleware.OptionalServiceAuth(validator)(okHandler(&ran))

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	r.Header.Set("X-API-Key", "wrong")
	h.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
	if ran {
		t.Error("handler ran despite an invalid key")
	}
}

func TestClientIP(t *testing.T) {
	cases := []struct {
		name    string
		headers map[string]string
		remote  string
		want    string
	}{
		{"remote address when no headers", nil, "10.0.0.5:1234", "10.0.0.5"},
		{"X-Real-IP wins over remote", map[string]string{"X-Real-IP": "203.0.113.7"}, "10.0.0.5:1234", "203.0.113.7"},
		{"left-most X-Forwarded-For wins", map[string]string{"X-Forwarded-For": "203.0.113.9, 10.0.0.1"}, "10.0.0.5:1234", "203.0.113.9"},
		{"single-entry X-Forwarded-For", map[string]string{"X-Forwarded-For": "203.0.113.9"}, "10.0.0.5:1234", "203.0.113.9"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/x", nil)
			r.RemoteAddr = tc.remote
			for k, v := range tc.headers {
				r.Header.Set(k, v)
			}
			if got := middleware.ClientIP(r); got != tc.want {
				t.Errorf("ClientIP = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestClientIdentifierPrefersUserThenService(t *testing.T) {
	base := httptest.NewRequest(http.MethodGet, "/x", nil)
	base.RemoteAddr = "10.0.0.5:1234"

	if got := middleware.ClientIdentifier(base); got != "ip:10.0.0.5" {
		t.Errorf("anonymous identifier = %q", got)
	}

	withService := base.WithContext(middleware.WithServiceIdentity(base.Context(),
		&middleware.ServiceKeyInfo{ServiceName: "caller"}))
	if got := middleware.ClientIdentifier(withService); got != "service:caller" {
		t.Errorf("service identifier = %q", got)
	}

	withUser := withService.WithContext(middleware.WithUserID(withService.Context(), "u-1"))
	if got := middleware.ClientIdentifier(withUser); got != "user:u-1" {
		t.Errorf("user identifier = %q", got)
	}
}

func TestRouteFallsBackToPathWithoutARouterPattern(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/api/v1/users/42", nil)
	if got := middleware.Route(r); got != "/api/v1/users/42" {
		t.Errorf("Route = %q, want the request path", got)
	}

	withPattern := r.WithContext(middleware.WithRoute(r.Context(), "/api/v1/users/{id}"))
	if got := middleware.Route(withPattern); got != "/api/v1/users/{id}" {
		t.Errorf("Route = %q, want the recorded pattern", got)
	}
}

func TestMetricsSkipsItsOwnEndpoint(t *testing.T) {
	var ran bool
	h := middleware.Metrics("test-service")(okHandler(&ran))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, middleware.MetricsPath, nil))

	if !ran {
		t.Error("the metrics endpoint was not passed through")
	}
}

func TestRecorderCapturesStatusForMetrics(t *testing.T) {
	h := middleware.Metrics("test-service")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))

	if w.Code != http.StatusTeapot {
		t.Errorf("status = %d, want %d — the recorder swallowed the handler's status", w.Code, http.StatusTeapot)
	}
}

// logAt captures what the request logger emits at the given level.
func logAt(t *testing.T, level slog.Level, mw func(http.Handler) http.Handler, path string, status int) string {
	t.Helper()
	var buf strings.Builder
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: level})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	return buf.String()
}

// Under an orchestrator, probes are the bulk of a service's log volume and
// none of its information.
func TestSuccessfulProbesAreQuietAtInfo(t *testing.T) {
	mw := middleware.RequestLogging("orders")

	for _, path := range []string{"/health", "/ready", "/metrics", "/debug/pprof/heap"} {
		t.Run(path, func(t *testing.T) {
			if got := logAt(t, slog.LevelInfo, mw, path, http.StatusOK); got != "" {
				t.Errorf("a successful probe was logged at info: %q", got)
			}
		})
	}
}

// Nothing is lost — the lines come back when someone asks for them.
func TestQuietedProbesStillAppearAtDebug(t *testing.T) {
	mw := middleware.RequestLogging("orders")

	if got := logAt(t, slog.LevelDebug, mw, "/health", http.StatusOK); !strings.Contains(got, "/health") {
		t.Errorf("a probe did not appear at debug level: %q", got)
	}
}

// A readiness check that starts flapping is the worst possible thing to have
// silenced.
func TestFailingProbesStayAtInfo(t *testing.T) {
	mw := middleware.RequestLogging("orders")

	got := logAt(t, slog.LevelInfo, mw, "/ready", http.StatusServiceUnavailable)
	if !strings.Contains(got, "/ready") {
		t.Errorf("a failing readiness probe was silenced: %q", got)
	}
}

func TestOrdinaryRequestsAreUnaffected(t *testing.T) {
	mw := middleware.RequestLogging("orders")

	if got := logAt(t, slog.LevelInfo, mw, "/api/v1/orders", http.StatusOK); !strings.Contains(got, "/api/v1/orders") {
		t.Errorf("an ordinary request was not logged at info: %q", got)
	}
}

// The default is an opinion, so it has to be overridable.
func TestQuietCanBeDisabled(t *testing.T) {
	mw := middleware.RequestLoggingWith(middleware.LoggingOptions{
		ServiceName: "orders",
		Quiet:       func(*http.Request, int) bool { return false },
	})

	if got := logAt(t, slog.LevelInfo, mw, "/health", http.StatusOK); !strings.Contains(got, "/health") {
		t.Errorf("Quiet override was ignored: %q", got)
	}
}

func TestIsOperationalPath(t *testing.T) {
	for _, path := range []string{"/health", "/healthz", "/ready", "/readyz", "/live", "/livez", "/metrics", "/debug/pprof/", "/debug/pprof/goroutine"} {
		if !middleware.IsOperationalPath(path) {
			t.Errorf("IsOperationalPath(%q) = false, want true", path)
		}
	}
	for _, path := range []string{"/", "/api/v1/orders", "/healthcheck-report", "/readyverse"} {
		if middleware.IsOperationalPath(path) {
			t.Errorf("IsOperationalPath(%q) = true, want false", path)
		}
	}
}
