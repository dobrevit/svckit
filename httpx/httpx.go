// Package httpx provides small, framework-neutral helpers for JSON HTTP
// handlers: a standard error envelope, safe error responses that never echo
// internal error strings to clients, bounded JSON decoding, and clamped
// pagination parsing. Everything operates on net/http types; framework
// veneers (e.g. Gin) wrap these.
package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
)

// ErrorBody is the standard JSON error envelope.
type ErrorBody struct {
	Error   string `json:"error"`
	Code    string `json:"code,omitempty"`
	Details any    `json:"details,omitempty"`
}

// WriteJSON writes v as JSON with the given status code.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError writes the standard error envelope. The message is sent to the
// client verbatim, so it must be a public-safe description — never pass
// err.Error() from an internal error here; use WriteInternalError instead.
func WriteError(w http.ResponseWriter, status int, message string) {
	WriteJSON(w, status, ErrorBody{Error: message})
}

// WriteErrorCode writes the standard error envelope with a machine-readable code.
func WriteErrorCode(w http.ResponseWriter, status int, message, code string) {
	WriteJSON(w, status, ErrorBody{Error: message, Code: code})
}

// WriteInternalError writes the canonical 500 response without exposing any
// internal detail. Log the underlying error at the call site.
func WriteInternalError(w http.ResponseWriter) {
	WriteError(w, http.StatusInternalServerError, "Internal server error")
}

// MaxJSONBody is the default request-body cap for DecodeJSON.
const MaxJSONBody = 1 << 20 // 1 MiB

// DecodeJSON decodes the request body into dst, capping the body at
// MaxJSONBody and rejecting trailing garbage. The returned error is safe to
// show to clients.
func DecodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, MaxJSONBody)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return fmt.Errorf("request body exceeds %d bytes", maxErr.Limit)
		}
		return errors.New("invalid request body")
	}
	if dec.More() {
		return errors.New("request body must contain a single JSON object")
	}
	return nil
}

// Pagination defaults; override per call with PaginationWith.
const (
	DefaultPageLimit = 20
	MaxPageLimit     = 100
)

// Page is a parsed, clamped limit/offset pair.
type Page struct {
	Limit  int
	Offset int
}

// Pagination parses the "limit" and "offset" query parameters with the
// package defaults: limit defaults to DefaultPageLimit and is clamped to
// [1, MaxPageLimit]; offset defaults to 0 and is clamped to >= 0.
func Pagination(r *http.Request) Page {
	return PaginationWith(r, DefaultPageLimit, MaxPageLimit)
}

// PaginationWith parses pagination with a custom default and maximum limit.
func PaginationWith(r *http.Request, defaultLimit, maxLimit int) Page {
	limit := queryInt(r, "limit", defaultLimit)
	if limit < 1 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	offset := max(queryInt(r, "offset", 0), 0)
	return Page{Limit: limit, Offset: offset}
}

func queryInt(r *http.Request, key string, def int) int {
	value := r.URL.Query().Get(key)
	if value == "" {
		return def
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return def
	}
	return n
}
