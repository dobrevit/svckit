package middleware

import (
	"bufio"
	"errors"
	"net"
	"net/http"
)

// recorder wraps an http.ResponseWriter to remember the status code and the
// number of bytes written, which logging and metrics both need after the
// handler has returned.
type recorder struct {
	http.ResponseWriter
	status  int
	written int64
}

func newRecorder(w http.ResponseWriter) *recorder {
	// A handler that writes a body without calling WriteHeader implies 200.
	return &recorder{ResponseWriter: w, status: http.StatusOK}
}

func (r *recorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *recorder) Write(p []byte) (int, error) {
	n, err := r.ResponseWriter.Write(p)
	r.written += int64(n)
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer, so
// deadline, flush and hijack support survive the wrapping.
func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// Flush forwards to the underlying writer when it supports flushing, so
// streaming responses are not buffered by the wrapper.
func (r *recorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack forwards to the underlying writer, keeping WebSocket upgrades and
// other connection takeovers working through the middleware chain.
func (r *recorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := r.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, errors.New("webmw: underlying ResponseWriter does not support hijacking")
}
