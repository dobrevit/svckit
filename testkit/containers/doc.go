// Package containers starts real PostgreSQL, Redis and RabbitMQ instances in
// Docker for the integration tier, using testcontainers-go.
//
// Tests that use it must be built with the "integration" tag, so an ordinary
// go test run never tries to reach a Docker daemon.
package containers
