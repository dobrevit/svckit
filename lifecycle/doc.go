// Package lifecycle manages the goroutines and servers a process owns, so
// shutdown is orderly rather than abrupt.
//
// A Manager tracks background work and the HTTP server together: on a
// termination signal it stops accepting new requests, lets in-flight ones
// finish within a deadline, then cancels the background work. It is built on
// gopkg.in/tomb.v2.
package lifecycle
