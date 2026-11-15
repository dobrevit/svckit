package pgcluster

import "errors"

// SQLStateUniqueViolation is the SQLSTATE of a unique or primary key
// violation.
const SQLStateUniqueViolation = "23505"

// SQLState returns the SQLSTATE code of a PostgreSQL error, through any
// wrapping, or "" if err carries none. It works with either driver: pgx's
// *pgconn.PgError and lib/pq's *pq.Error (since lib/pq 1.10) both have a
// SQLState method, so code that checks a constraint violation keeps
// working when DB_DRIVER changes, and svckit need not import lib/pq.
func SQLState(err error) string {
	var coded interface{ SQLState() string }
	if errors.As(err, &coded) {
		return coded.SQLState()
	}
	return ""
}

// IsUniqueViolation reports whether err is a unique or primary key
// violation (SQLSTATE 23505), from either driver.
func IsUniqueViolation(err error) bool {
	return SQLState(err) == SQLStateUniqueViolation
}
