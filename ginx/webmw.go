package ginx

import (
	"net/http"
	"slices"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/dobrevit/svckit/httpclient"
	webmw "github.com/dobrevit/svckit/middleware"
	rediscluster "github.com/dobrevit/svckit/rediscluster"
)

// This file is the Gin expression of svckit/middleware. The logic lives there
// against net/http; what Gin needs on top is the route pattern
// (c.FullPath, which the stdlib request does not carry), the status code
// (which Gin records on its own writer), and mirroring of context values into
// gin.Context for handlers that still read them from there.

// Route records Gin's matched route pattern on the request context so that
// webmw's metrics and logging label by route template rather than by concrete
// path. Register it before the middlewares that read it.
func Route() gin.HandlerFunc {
	return func(c *gin.Context) {
		if pattern := c.FullPath(); pattern != "" {
			c.Request = c.Request.WithContext(webmw.WithRoute(c.Request.Context(), pattern))
		}
		c.Next()
	}
}

// CORSWithOrigins is the Gin form of webmw.CORSWithOrigins.
func CORSWithOrigins(allowedOrigins []string) gin.HandlerFunc {
	return Use(webmw.CORSWithOrigins(allowedOrigins))
}

// TracingMiddleware is the Gin form of webmw.Tracing. It additionally mirrors
// the trace onto gin.Context under "trace_context" for handlers using
// GetTraceContext.
func TracingMiddleware() gin.HandlerFunc {
	tracing := webmw.Tracing()
	return UseThen(tracing, func(c *gin.Context) {
		if trace := httpclient.TraceFromContext(c.Request.Context()); trace != nil {
			c.Set("trace_context", trace)
		}
	})
}

// GetTraceContext returns the trace context attached to c, or nil.
func GetTraceContext(c *gin.Context) *httpclient.TraceContext {
	return httpclient.TraceFromContext(c.Request.Context())
}

// SetUserID records the authenticated user on the request's trace context and
// on the request context, so rate limiting and outbound calls both see it.
func SetUserID(c *gin.Context, userID string) {
	if trace := GetTraceContext(c); trace != nil {
		trace.UserID = userID
	}
	c.Request = c.Request.WithContext(webmw.WithUserID(c.Request.Context(), userID))
}

// PrometheusMiddleware is the Gin form of webmw.Metrics. It reads the status
// from Gin's writer rather than wrapping it, and records through webmw's
// exported recorders so both forms share one set of series.
func PrometheusMiddleware(serviceName string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.URL.Path == webmw.MetricsPath {
			c.Next()
			return
		}

		done := webmw.TrackInFlight(serviceName)
		defer done()

		start := time.Now()
		c.Next()

		route := c.FullPath()
		if route == "" {
			route = c.Request.URL.Path
		}
		webmw.ObserveRequest(serviceName, c.Request.Method, route, c.Writer.Status(), time.Since(start))
	}
}

// MetricsHandler serves the Prometheus exposition format.
func MetricsHandler() gin.HandlerFunc {
	return gin.WrapH(webmw.MetricsHandler())
}

// RequestLoggingMiddleware logs one line per completed request, reading the
// status from Gin's writer.
func RequestLoggingMiddleware(serviceName string) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		logRequest(serviceName, c, time.Since(start))
	}
}

// HealthCheck is the Gin form of webmw.HealthHandler.
func HealthCheck(serviceName string, checks ...webmw.HealthChecker) gin.HandlerFunc {
	return gin.WrapH(webmw.HealthHandler(serviceName, checks...))
}

// ReadinessCheck is the Gin form of webmw.ReadinessHandler.
func ReadinessCheck(serviceName string, checks ...webmw.HealthChecker) gin.HandlerFunc {
	return gin.WrapH(webmw.ReadinessHandler(serviceName, checks...))
}

// RateLimitMiddleware is the Gin form of webmw.RateLimit.
func RateLimitMiddleware(redisClient *rediscluster.ClusterClient, serviceName string) gin.HandlerFunc {
	return Use(webmw.RateLimit(redisClient, serviceName))
}

// ServiceAuthRequired is the Gin form of webmw.ServiceAuthRequired. It mirrors
// the validated identity onto gin.Context for handlers reading it from there.
func ServiceAuthRequired(validator webmw.ServiceKeyValidator) gin.HandlerFunc {
	return serviceAuth(webmw.ServiceAuthRequired(validator))
}

// OptionalServiceAuth is the Gin form of webmw.OptionalServiceAuth.
func OptionalServiceAuth(validator webmw.ServiceKeyValidator) gin.HandlerFunc {
	return serviceAuth(webmw.OptionalServiceAuth(validator))
}

// RequireServiceScope is the Gin form of webmw.RequireServiceScope.
func RequireServiceScope(requiredScope string) gin.HandlerFunc {
	return Use(webmw.RequireServiceScope(requiredScope))
}

// ServiceAuthOrUserAuth accepts either a service API key or, failing that,
// whatever userAuthMiddleware accepts.
func ServiceAuthOrUserAuth(validator webmw.ServiceKeyValidator, userAuthMiddleware gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		if webmw.ServiceKeyFromRequest(c.Request) == "" {
			c.Set("auth_type", string(webmw.AuthTypeUser))
			userAuthMiddleware(c)
			return
		}
		serviceAuth(webmw.ServiceAuthRequired(validator))(c)
	}
}

// serviceAuth runs a webmw service-auth middleware and mirrors the resulting
// identity onto gin.Context.
func serviceAuth(mw func(http.Handler) http.Handler) gin.HandlerFunc {
	return UseThen(mw, func(c *gin.Context) {
		if info, ok := webmw.ServiceIdentity(c.Request.Context()); ok {
			c.Set("service_name", info.ServiceName)
			c.Set("service_scopes", info.Scopes)
			c.Set("service_info", info)
			c.Set("auth_type", string(webmw.AuthTypeService))
		}
	})
}

// UseThen runs a stdlib middleware in a Gin chain and calls sync once the
// middleware has annotated the request but before the rest of the chain runs.
// It is how a Gin veneer mirrors context values onto gin.Context for handlers
// that still read them from there.
//
// The mirroring has to happen at that point: Use resumes the whole Gin chain
// from inside the middleware, so anything written to gin.Context after Use
// returns lands after the route handler has already read it.
func UseThen(mw func(http.Handler) http.Handler, sync func(*gin.Context)) gin.HandlerFunc {
	return func(c *gin.Context) {
		Use(func(next http.Handler) http.Handler {
			return mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c.Request = r
				sync(c)
				next.ServeHTTP(w, r)
			}))
		})(c)
	}
}

// The three helpers below read the request context first and fall back to
// gin.Context. The fallback matters because not every middleware that
// authenticates a service goes through svckit/middleware — a platform's own
// CombinedAuthMiddleware, for one, records the identity on gin.Context
// directly — and these helpers must answer for both.

// GetAuthenticatedService returns the calling service's name, if the request
// was authenticated as a service.
func GetAuthenticatedService(c *gin.Context) (string, bool) {
	if name, ok := webmw.ServiceName(c.Request.Context()); ok {
		return name, true
	}
	name, ok := c.Get("service_name")
	if !ok {
		return "", false
	}
	s, ok := name.(string)
	return s, ok && s != ""
}

// HasServiceScope reports whether the calling service holds scope. The
// wildcard scope satisfies any check.
func HasServiceScope(c *gin.Context, scope string) bool {
	if webmw.HasScope(c.Request.Context(), scope) {
		return true
	}
	raw, ok := c.Get("service_scopes")
	if !ok {
		return false
	}
	scopes, ok := raw.([]string)
	if !ok {
		return false
	}
	return slices.Contains(scopes, scope) || slices.Contains(scopes, webmw.ScopeAll)
}

// IsServiceAuthenticated reports whether the request was authenticated as a
// service rather than as a user.
func IsServiceAuthenticated(c *gin.Context) bool {
	if webmw.IsServiceAuthenticated(c.Request.Context()) {
		return true
	}
	if authType, ok := c.Get("auth_type"); ok {
		return authType == string(webmw.AuthTypeService)
	}
	_, ok := c.Get("service_name")
	return ok
}

// GetClientIdentifier returns the rate-limiting bucket for the request.
func GetClientIdentifier(c *gin.Context) string {
	return webmw.ClientIdentifier(c.Request)
}
