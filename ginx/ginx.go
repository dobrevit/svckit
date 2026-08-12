// Package ginx adapts svckit to the Gin framework.
//
// Gin is the reason this package needs to exist. Where chi and the standard
// library's own mux consume svckit's middleware directly -- it is already
// func(http.Handler) http.Handler -- Gin has its own handler type, its own
// context and its own response writer, so each piece needs a translation:
//
//	router := gin.New()
//	router.Use(ginx.Route())               // label metrics by route template
//	router.Use(ginx.TracingMiddleware())
//	router.Use(ginx.PrometheusMiddleware("orders"))
//	router.Use(ginx.LoggingMiddleware())
//
// It also carries the Gin expression of svckit/httpx -- the same response
// envelope, decoding and pagination semantics, written against gin.Context --
// so handlers do not hand-roll a second set.
//
// Use adapts any func(http.Handler) http.Handler into a gin.HandlerFunc, which
// covers svckit middleware this package does not wrap explicitly.
//
// It lives in its own module so that the main svckit module depends on no web
// framework: importing svckit does not pull in Gin, and a project on a
// different Gin version is unaffected by what this module requires.
package ginx

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/dobrevit/svckit/httpx"
	"github.com/dobrevit/svckit/logging"
)

// OK responds 200 with v as JSON.
func OK(c *gin.Context, v any) {
	c.JSON(http.StatusOK, v)
}

// Created responds 201 with v as JSON.
func Created(c *gin.Context, v any) {
	c.JSON(http.StatusCreated, v)
}

// Error responds with the standard error envelope. The message goes to the
// client verbatim — public-safe descriptions only, never err.Error() from an
// internal error (use InternalError for those).
func Error(c *gin.Context, status int, message string) {
	c.JSON(status, httpx.ErrorBody{Error: message})
}

// ErrorCode responds with the standard error envelope plus a machine-readable code.
func ErrorCode(c *gin.Context, status int, message, code string) {
	c.JSON(status, httpx.ErrorBody{Error: message, Code: code})
}

// BadRequest responds 400 with the given public-safe message.
func BadRequest(c *gin.Context, message string) {
	Error(c, http.StatusBadRequest, message)
}

// NotFound responds 404 with the given public-safe message.
func NotFound(c *gin.Context, message string) {
	Error(c, http.StatusNotFound, message)
}

// InternalError logs err with the request path for operators and responds
// with the canonical 500 — the client never sees the internal error string.
func InternalError(c *gin.Context, err error) {
	logging.Error("internal error handling %s %s: %v", c.Request.Method, c.Request.URL.Path, err)
	Error(c, http.StatusInternalServerError, "Internal server error")
}

// Bind decodes the JSON request body into dst. On failure it writes the
// standard 400 response and returns false — callers just early-return.
func Bind(c *gin.Context, dst any) bool {
	if err := c.ShouldBindJSON(dst); err != nil {
		BadRequest(c, "Invalid request body")
		return false
	}
	return true
}

// Pagination parses limit/offset query parameters with httpx's defaults and clamping.
func Pagination(c *gin.Context) httpx.Page {
	return httpx.Pagination(c.Request)
}

// PaginationWith parses pagination with a custom default and maximum limit.
func PaginationWith(c *gin.Context, defaultLimit, maxLimit int) httpx.Page {
	return httpx.PaginationWith(c.Request, defaultLimit, maxLimit)
}
