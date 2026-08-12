// Package rediscluster connects to a set of Redis nodes with health checking
// and load balancing across the healthy ones.
//
// go-redis is exposed rather than wrapped: the client type it returns is
// go-redis's own, so every command is available without this package
// mirroring the API.
package rediscluster
