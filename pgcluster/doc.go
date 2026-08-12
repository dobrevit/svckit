// Package pgcluster connects to a PostgreSQL cluster over database/sql.
//
// It keeps a pool per node and decides which one a query should use: the
// writer is discovered at runtime (by DNS, by pg_is_in_recovery(), or by
// probing for a writable transaction) so a failover does not need a restart,
// and reads are balanced across the healthy nodes. Failing nodes are dropped
// and retried, and a circuit breaker gives up quickly once the whole cluster
// is unreachable.
//
// It returns *sql.DB, so an ORM binds to it in a few lines rather than being
// a dependency of this package.
package pgcluster
