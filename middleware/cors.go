package middleware

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// DefaultCORSHeaders is the request-header allowlist applied when CORSOptions
// leaves AllowedHeaders empty.
var DefaultCORSHeaders = []string{
	"Content-Type", "Content-Length", "Accept-Encoding", "X-CSRF-Token",
	"Authorization", "accept", "origin", "Cache-Control", "X-Requested-With",
	"X-Trace-ID", "X-User-ID", "traceparent",
}

// DefaultCORSMethods is the method allowlist applied when CORSOptions leaves
// AllowedMethods empty.
var DefaultCORSMethods = []string{
	http.MethodGet, http.MethodPost, http.MethodPut,
	http.MethodDelete, http.MethodOptions,
}

// CORSOptions configures the CORS middleware.
type CORSOptions struct {
	// AllowedOrigins lists the origins permitted to make credentialed
	// requests. The single entry "*" allows any origin.
	AllowedOrigins []string
	// AllowedMethods and AllowedHeaders default to DefaultCORSMethods and
	// DefaultCORSHeaders when empty.
	AllowedMethods []string
	AllowedHeaders []string
	// AllowCredentials sets Access-Control-Allow-Credentials.
	AllowCredentials bool
	// MaxAge caps how long a preflight result may be cached. Defaults to 12h.
	MaxAge time.Duration
}

// CORS returns a middleware applying opts to every request and answering
// preflight requests with 204.
func CORS(opts CORSOptions) func(http.Handler) http.Handler {
	methods := opts.AllowedMethods
	if len(methods) == 0 {
		methods = DefaultCORSMethods
	}
	headers := opts.AllowedHeaders
	if len(headers) == 0 {
		headers = DefaultCORSHeaders
	}
	maxAge := opts.MaxAge
	if maxAge == 0 {
		maxAge = 12 * time.Hour
	}

	allowMethods := strings.Join(methods, ", ")
	allowHeaders := strings.Join(headers, ", ")
	maxAgeSeconds := strconv.Itoa(int(maxAge.Seconds()))

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if origin := allowedOrigin(opts.AllowedOrigins, r.Header.Get("Origin")); origin != "" {
				w.Header().Set("Access-Control-Allow-Origin", origin)
			}
			if opts.AllowCredentials {
				w.Header().Set("Access-Control-Allow-Credentials", "true")
			}
			w.Header().Set("Access-Control-Allow-Headers", allowHeaders)
			w.Header().Set("Access-Control-Allow-Methods", allowMethods)
			w.Header().Set("Access-Control-Max-Age", maxAgeSeconds)

			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// CORSWithOrigins returns a credentialed CORS middleware restricted to
// allowedOrigins, using the default method and header allowlists.
func CORSWithOrigins(allowedOrigins []string) func(http.Handler) http.Handler {
	return CORS(CORSOptions{AllowedOrigins: allowedOrigins, AllowCredentials: true})
}

// allowedOrigin picks the value for Access-Control-Allow-Origin. An origin on
// the allowlist is echoed back; anything else falls back to the first
// configured origin, so a browser on an unlisted origin sees a mismatch and
// blocks the response rather than receiving no header at all.
func allowedOrigin(allowed []string, origin string) string {
	if len(allowed) == 0 {
		return ""
	}
	if slices.Contains(allowed, "*") || slices.Contains(allowed, origin) {
		if origin != "" {
			return origin
		}
	}
	return allowed[0]
}
