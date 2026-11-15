// Package outbox stores a message in the same database transaction as the
// change it announces, so that the two commit or roll back together: a crash
// between changing state and publishing can neither lose the message nor
// publish one for a change that rolled back. A Relay (in this package)
// publishes what was stored.
//
// The table is the consumer's to create and migrate; svckit ships no
// migration runner. Schema returns its DDL for a table name, and
// outbox/schema.sql is the same for DefaultTable:
//
//	CREATE TABLE svckit_outbox (
//	    id              text        PRIMARY KEY,
//	    exchange        text        NOT NULL,
//	    routing_key     text        NOT NULL,
//	    headers         jsonb       NOT NULL DEFAULT '{}',
//	    content_type    text        NOT NULL DEFAULT 'application/json',
//	    body            bytea       NOT NULL,
//	    subject_key     text,
//	    created_at      timestamptz NOT NULL DEFAULT now(),
//	    expires_at      timestamptz,
//	    attempts        integer     NOT NULL DEFAULT 0,
//	    next_attempt_at timestamptz NOT NULL DEFAULT now(),
//	    last_error      text,
//	    published_at    timestamptz,
//	    expired_at      timestamptz
//	);
//
// plus indexes for the relay's claim, for erasure by subject, and for the
// retention purge.
package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// DefaultTable is the outbox table's name unless another is given.
const DefaultTable = "svckit_outbox"

// Execer runs a statement. *sql.Tx and *sql.DB satisfy it, and so does an
// Ent transaction generated with the sql/execquery feature, so svckit
// depends on no ORM. Pass the caller's transaction, so the message commits
// with the change it announces.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Message is one stored message, published by the relay as it is stored.
type Message struct {
	// ID is the message id: the event id, which is the AMQP MessageId and
	// what consumers deduplicate on. It is the table's primary key.
	ID string

	Exchange   string
	RoutingKey string

	// Headers are the AMQP headers, stored as JSON: strings, booleans and
	// numbers survive; numbers come back as float64.
	Headers map[string]any

	// ContentType defaults to application/json.
	ContentType string

	// Body is published byte for byte: the event already signed, and sealed
	// if it carries personal data.
	Body []byte

	// ExpiresAt, when set, is when the message stops being worth
	// delivering: the relay publishes it with the time left as its AMQP
	// expiration, and does not publish it at all once it has passed.
	ExpiresAt time.Time

	// SubjectKey, when set, names whom the message is about (a person id,
	// say), so that an erasure can find the messages about one subject
	// (DeleteUnpublished). It is opaque to svckit and indexed.
	SubjectKey string
}

// Writer writes messages to one outbox table.
type Writer struct {
	table string
}

// NewWriter returns a Writer for table (DefaultTable when empty), which
// must be a plain or schema-qualified SQL identifier.
func NewWriter(table string) (*Writer, error) {
	table, err := checkTable(table)
	if err != nil {
		return nil, err
	}
	return &Writer{table: table}, nil
}

// Write inserts msg through ex, which should be the caller's transaction.
func (w *Writer) Write(ctx context.Context, ex Execer, msg Message) error {
	if msg.ID == "" || (msg.Exchange == "" && msg.RoutingKey == "") {
		return errors.New("outbox: a message needs an id and somewhere to go")
	}
	headers := msg.Headers
	if headers == nil {
		headers = map[string]any{}
	}
	rawHeaders, err := json.Marshal(headers)
	if err != nil {
		return fmt.Errorf("outbox: encode headers: %w", err)
	}
	contentType := msg.ContentType
	if contentType == "" {
		contentType = "application/json"
	}
	var expiresAt sql.NullTime
	if !msg.ExpiresAt.IsZero() {
		expiresAt = sql.NullTime{Time: msg.ExpiresAt, Valid: true}
	}
	subject := sql.NullString{String: msg.SubjectKey, Valid: msg.SubjectKey != ""}

	_, err = ex.ExecContext(ctx, `INSERT INTO `+w.table+
		` (id, exchange, routing_key, headers, content_type, body, subject_key, expires_at)
		 VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8)`,
		msg.ID, msg.Exchange, msg.RoutingKey, string(rawHeaders), contentType, msg.Body, subject, expiresAt)
	if err != nil {
		return fmt.Errorf("outbox: write %s: %w", msg.ID, err)
	}
	return nil
}

// DeleteUnpublished removes the messages about subjectKey that have not been
// published yet, through ex (the caller's erasure transaction), and returns
// how many. Published rows are kept: what was sent is the consumers' to
// erase, and the retention purge removes the rows.
func (w *Writer) DeleteUnpublished(ctx context.Context, ex Execer, subjectKey string) (int64, error) {
	if subjectKey == "" {
		return 0, errors.New("outbox: an empty subject key matches nothing")
	}
	res, err := ex.ExecContext(ctx, `DELETE FROM `+w.table+
		` WHERE subject_key = $1 AND published_at IS NULL`, subjectKey)
	if err != nil {
		return 0, fmt.Errorf("outbox: delete unpublished messages: %w", err)
	}
	return res.RowsAffected()
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)

// checkTable defaults and validates a table name: it is written into SQL,
// so only a plain or schema-qualified identifier is accepted.
func checkTable(table string) (string, error) {
	if table == "" {
		return DefaultTable, nil
	}
	if !identifier.MatchString(table) {
		return "", fmt.Errorf("outbox: %q is not a plain or schema-qualified table name", table)
	}
	return table, nil
}

// Schema returns the DDL that creates the outbox table named table
// (DefaultTable when empty) and its indexes. It panics on a name that is
// not a plain or schema-qualified identifier.
func Schema(table string) string {
	table, err := checkTable(table)
	if err != nil {
		panic(err)
	}
	prefix := strings.ReplaceAll(table, ".", "_")
	if i := strings.LastIndex(table, "."); i >= 0 {
		prefix = table[i+1:]
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s (
    id              text        PRIMARY KEY,
    exchange        text        NOT NULL,
    routing_key     text        NOT NULL,
    headers         jsonb       NOT NULL DEFAULT '{}',
    content_type    text        NOT NULL DEFAULT 'application/json',
    body            bytea       NOT NULL,
    subject_key     text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    expires_at      timestamptz,
    attempts        integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error      text,
    published_at    timestamptz,
    expired_at      timestamptz
);

-- The relay claims pending rows in creation order.
CREATE INDEX IF NOT EXISTS %[2]s_pending_idx
    ON %[1]s (next_attempt_at, created_at)
    WHERE published_at IS NULL AND expired_at IS NULL;

-- An erasure finds the messages about one subject.
CREATE INDEX IF NOT EXISTS %[2]s_subject_idx
    ON %[1]s (subject_key)
    WHERE subject_key IS NOT NULL;

-- The retention purge removes old published and expired rows.
CREATE INDEX IF NOT EXISTS %[2]s_published_idx
    ON %[1]s (published_at)
    WHERE published_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS %[2]s_expired_idx
    ON %[1]s (expired_at)
    WHERE expired_at IS NOT NULL;
`, table, prefix)
}

// schemaHeader opens schema.sql.
const schemaHeader = `-- The outbox table, as outbox.Schema(outbox.DefaultTable) returns it.
-- Apply it with your own migrations; svckit ships no migration runner.
`
