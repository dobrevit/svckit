package ginx

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/dobrevit/svckit/debug"
)

// Use adapts a standard net/http middleware — func(http.Handler) http.Handler,
// the form the toolkit packages are written in — into gin.HandlerFunc, so Gin
// routers can run framework-neutral middleware unchanged.
//
// The adapter runs mw around a handler that resumes the Gin chain, so
// middleware that short-circuits (writing a response without calling the next
// handler) correctly aborts the remaining Gin handlers, and context values it
// attaches reach downstream handlers.
//
// One kind of middleware does not survive the adaptation: one that substitutes
// the http.ResponseWriter to observe the response, since Gin handlers write
// through c.Writer and would bypass the substitute. Middleware that needs the
// response status — metrics and request logging — therefore has a dedicated
// Gin form in this package that reads the status from Gin's own writer.
func Use(mw func(http.Handler) http.Handler) gin.HandlerFunc {
	return func(c *gin.Context) {
		resumed := false
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resumed = true
			// Adopt the request the middleware passed down, so downstream
			// handlers see any context values it attached.
			c.Request = r
			c.Next()
		})

		mw(next).ServeHTTP(c.Writer, c.Request)

		if !resumed {
			c.Abort()
		}
	}
}

// EnablePprofIfDebug mounts the profiling endpoints on router when the debug
// gate is open. It is the Gin expression of debug.Register.
func EnablePprofIfDebug(router *gin.Engine) {
	if h := debug.Handler(); h != nil {
		router.Any(debug.Prefix+"*any", gin.WrapH(h))
	}
}
