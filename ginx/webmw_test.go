package ginx_test

import (
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/dobrevit/svckit/ginx"
	"github.com/dobrevit/svckit/logging"
	webmw "github.com/dobrevit/svckit/middleware"
)

func TestUseRunsStdlibMiddlewareInAGinChain(t *testing.T) {
	var order []string

	mw := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			order = append(order, "before")
			next.ServeHTTP(w, r)
			order = append(order, "after")
		})
	}

	router := gin.New()
	router.Use(ginx.Use(mw))
	router.GET("/x", func(c *gin.Context) {
		order = append(order, "handler")
		c.String(http.StatusOK, "ok")
	})

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))

	if w.Code != http.StatusOK || w.Body.String() != "ok" {
		t.Fatalf("status = %d, body = %q", w.Code, w.Body.String())
	}
	want := []string{"before", "handler", "after"}
	for i := range want {
		if i >= len(order) || order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

func TestUseAbortsTheGinChainWhenTheMiddlewareShortCircuits(t *testing.T) {
	blocking := func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
		})
	}

	var handlerRan bool
	router := gin.New()
	router.Use(ginx.Use(blocking))
	router.GET("/x", func(c *gin.Context) { handlerRan = true })

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))

	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", w.Code)
	}
	if handlerRan {
		t.Error("the Gin handler ran even though the middleware short-circuited")
	}
}

func TestUsePropagatesContextValuesToGinHandlers(t *testing.T) {
	inject := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(webmw.WithUserID(r.Context(), "u-7")))
		})
	}

	var seen string
	router := gin.New()
	router.Use(ginx.Use(inject))
	router.GET("/x", func(c *gin.Context) {
		seen, _ = webmw.UserID(c.Request.Context())
	})

	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))

	if seen != "u-7" {
		t.Errorf("UserID seen by the handler = %q, want u-7", seen)
	}
}

func TestRouteRecordsGinsPatternForMetricLabels(t *testing.T) {
	var seen string
	router := gin.New()
	router.Use(ginx.Route())
	router.GET("/api/v1/users/:id", func(c *gin.Context) {
		seen = webmw.Route(c.Request)
	})

	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/users/42", nil))

	if seen != "/api/v1/users/:id" {
		t.Errorf("route label = %q, want the pattern rather than the concrete path", seen)
	}
}

type stubValidator struct{ info *webmw.ServiceKeyInfo }

func (s stubValidator) ValidateServiceKey(apiKey string) (*webmw.ServiceKeyInfo, error) {
	if apiKey != "good-key" {
		return nil, errors.New("unknown key")
	}
	return s.info, nil
}

func TestServiceAuthMirrorsIdentityOntoGinContext(t *testing.T) {
	validator := stubValidator{info: &webmw.ServiceKeyInfo{ServiceName: "caller", Scopes: []string{"read"}}}

	var (
		ginName  any
		helper   string
		hasScope bool
	)
	router := gin.New()
	router.Use(ginx.ServiceAuthRequired(validator))
	router.GET("/x", func(c *gin.Context) {
		ginName, _ = c.Get("service_name")
		helper, _ = ginx.GetAuthenticatedService(c)
		hasScope = ginx.HasServiceScope(c, "read")
	})

	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	r.Header.Set("X-API-Key", "good-key")
	router.ServeHTTP(httptest.NewRecorder(), r)

	if ginName != "caller" {
		t.Errorf(`c.Get("service_name") = %v, want caller`, ginName)
	}
	if helper != "caller" {
		t.Errorf("GetAuthenticatedService = %q, want caller", helper)
	}
	if !hasScope {
		t.Error("HasServiceScope(read) = false, want true")
	}
}

func TestServiceAuthRejectionAbortsTheGinChain(t *testing.T) {
	validator := stubValidator{info: &webmw.ServiceKeyInfo{ServiceName: "caller"}}

	var handlerRan bool
	router := gin.New()
	router.Use(ginx.ServiceAuthRequired(validator))
	router.GET("/x", func(c *gin.Context) { handlerRan = true })

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	r.Header.Set("X-API-Key", "wrong")
	router.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
	if handlerRan {
		t.Error("the handler ran despite an invalid service key")
	}
}

func TestTracingSetsResponseHeadersAndGinContext(t *testing.T) {
	router := gin.New()
	router.Use(ginx.TracingMiddleware())
	router.GET("/x", func(c *gin.Context) {
		if ginx.GetTraceContext(c) == nil {
			t.Error("no trace context reached the handler")
		}
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))

	if w.Header().Get("X-Trace-ID") == "" {
		t.Error("X-Trace-ID response header is empty")
	}
	if w.Header().Get("traceparent") == "" {
		t.Error("traceparent response header is empty")
	}
}

func TestHealthCheckVeneer(t *testing.T) {
	sick := webmw.NewDatabaseHealthChecker("postgresql", func() error { return errors.New("down") })

	router := gin.New()
	router.GET("/health", ginx.HealthCheck("test-service", sick))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestMain(m *testing.M) {
	gin.SetMode(gin.TestMode)
	m.Run()
}

// Not every middleware that authenticates a service goes through webmw:
// shared/auth's CombinedAuthMiddleware records the identity on gin.Context
// only. The helpers must still answer for those requests.
func TestServiceHelpersReadIdentityRecordedOnlyOnGinContext(t *testing.T) {
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("auth_type", "service")
		c.Set("service_name", "legacy-caller")
		c.Set("service_scopes", []string{"read"})
		c.Next()
	})

	var (
		name      string
		found     bool
		hasRead   bool
		hasWrite  bool
		isService bool
	)
	router.GET("/x", func(c *gin.Context) {
		name, found = ginx.GetAuthenticatedService(c)
		hasRead = ginx.HasServiceScope(c, "read")
		hasWrite = ginx.HasServiceScope(c, "write")
		isService = ginx.IsServiceAuthenticated(c)
	})

	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))

	if !found || name != "legacy-caller" {
		t.Errorf("GetAuthenticatedService = (%q, %v), want (legacy-caller, true)", name, found)
	}
	if !hasRead {
		t.Error("HasServiceScope(read) = false, want true")
	}
	if hasWrite {
		t.Error("HasServiceScope(write) = true, want false")
	}
	if !isService {
		t.Error("IsServiceAuthenticated = false, want true")
	}
}

// serveLogged runs one request through the Gin logging middleware and returns
// what was logged at the given level.
func serveLogged(t *testing.T, level slog.Level, path string, status int) string {
	t.Helper()
	var buf strings.Builder
	prev := slog.Default()
	slog.SetDefault(slog.New(logging.NewHandler(logging.Options{
		Service: "orders", Writer: &buf, Level: level,
	})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	router := gin.New()
	router.Use(ginx.LoggingMiddleware())
	router.GET(path, func(c *gin.Context) { c.Status(status) })
	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	return buf.String()
}

// Probes are the bulk of a deployment's log volume and none of its
// information, so they must not reach info level.
func TestSuccessfulProbesAreQuiet(t *testing.T) {
	for _, path := range []string{"/health", "/ready", "/metrics"} {
		t.Run(path, func(t *testing.T) {
			if got := serveLogged(t, slog.LevelInfo, path, http.StatusOK); got != "" {
				t.Errorf("logged at info: %q", got)
			}
		})
	}
}

// A readiness check that starts flapping is the worst possible thing to
// silence, so a failing probe stays at info.
func TestFailingProbesAreNotQuiet(t *testing.T) {
	got := serveLogged(t, slog.LevelInfo, "/ready", http.StatusServiceUnavailable)
	if !strings.Contains(got, "/ready") {
		t.Errorf("a failing readiness probe was silenced: %q", got)
	}
}

func TestOrdinaryRequestsAreStillLogged(t *testing.T) {
	got := serveLogged(t, slog.LevelInfo, "/api/v1/orders", http.StatusOK)
	if !strings.Contains(got, "/api/v1/orders") {
		t.Errorf("an ordinary request was silenced: %q", got)
	}
}

func TestQuietedProbesReturnAtDebug(t *testing.T) {
	got := serveLogged(t, slog.LevelDebug, "/health", http.StatusOK)
	if !strings.Contains(got, "/health") {
		t.Errorf("a probe did not come back at debug: %q", got)
	}
}

// Gin reports -1 when the handler wrote no body.
func TestEmptyResponseReportsZeroBytes(t *testing.T) {
	got := serveLogged(t, slog.LevelInfo, "/api/v1/orders", http.StatusOK)
	if strings.Contains(got, "bytes=-1") {
		t.Errorf("negative byte count in %q", got)
	}
}
