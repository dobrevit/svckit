// Package inbox makes handling a message idempotent. Delivery is at least
// once, so a consumer can receive one message twice; claiming its id as the
// first write of the transaction that handles it means the second delivery
// finds the claim and skips the work, and the claim commits or rolls back
// with the work itself.
//
//	claimed, err := inbox.Claim(ctx, tx, event.ID, "billing")
//	if err != nil { return err }       // retry: nothing was done
//	if !claimed { return tx.Commit() } // a duplicate: already handled
//	// ... the handling, in tx ...
//	return tx.Commit()
//
// The table is the consumer's to create; Schema returns its DDL, and
// inbox/schema.sql is the same for DefaultTable:
//
//	CREATE TABLE svckit_inbox (
//	    message_id text        NOT NULL,
//	    consumer   text        NOT NULL,
//	    claimed_at timestamptz NOT NULL DEFAULT now(),
//	    PRIMARY KEY (message_id, consumer)
//	);
package inbox

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/dobrevit/svckit/outbox"
)

// DefaultTable is the inbox table's name unless another is given.
const DefaultTable = "svckit_inbox"

// Inbox claims message ids in one table.
type Inbox struct {
	table string
}

// New returns an Inbox on table (DefaultTable when empty), which must be a
// plain or schema-qualified SQL identifier.
func New(table string) (*Inbox, error) {
	table, err := checkTable(table)
	if err != nil {
		return nil, err
	}
	return &Inbox{table: table}, nil
}

// Claim records that consumer is handling messageID, through ex, which
// should be the transaction that handles the message, as its first write.
// It reports false when the message was claimed already: a duplicate.
//
// It is an INSERT ... ON CONFLICT DO NOTHING, so it needs no SQLSTATE
// parsing and works with either driver. A concurrent claim of the same id
// waits for the first transaction: it gets false once that commits, and
// true if it rolls back.
func (i *Inbox) Claim(ctx context.Context, ex outbox.Execer, messageID, consumer string) (bool, error) {
	if messageID == "" || consumer == "" {
		return false, errors.New("inbox: a claim needs a message id and a consumer")
	}
	res, err := ex.ExecContext(ctx, `INSERT INTO `+i.table+` (message_id, consumer) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		messageID, consumer)
	if err != nil {
		return false, fmt.Errorf("inbox: claim %s: %w", messageID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inbox: claim %s: %w", messageID, err)
	}
	return n == 1, nil
}

// Purge deletes claims older than olderThan and returns how many. Keep
// claims longer than a message can be redelivered: a duplicate that
// arrives after its claim was purged is handled again.
func (i *Inbox) Purge(ctx context.Context, ex outbox.Execer, olderThan time.Duration) (int64, error) {
	res, err := ex.ExecContext(ctx, `DELETE FROM `+i.table+` WHERE claimed_at < now() - $1 * interval '1 millisecond'`,
		olderThan.Milliseconds())
	if err != nil {
		return 0, fmt.Errorf("inbox: purge: %w", err)
	}
	return res.RowsAffected()
}

var defaultInbox = &Inbox{table: DefaultTable}

// Claim is Inbox.Claim on DefaultTable.
func Claim(ctx context.Context, ex outbox.Execer, messageID, consumer string) (bool, error) {
	return defaultInbox.Claim(ctx, ex, messageID, consumer)
}

// Purge is Inbox.Purge on DefaultTable.
func Purge(ctx context.Context, ex outbox.Execer, olderThan time.Duration) (int64, error) {
	return defaultInbox.Purge(ctx, ex, olderThan)
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)

func checkTable(table string) (string, error) {
	if table == "" {
		return DefaultTable, nil
	}
	if !identifier.MatchString(table) {
		return "", fmt.Errorf("inbox: %q is not a plain or schema-qualified table name", table)
	}
	return table, nil
}

// Schema returns the DDL that creates the inbox table named table
// (DefaultTable when empty). It panics on a name that is not a plain or
// schema-qualified identifier.
func Schema(table string) string {
	table, err := checkTable(table)
	if err != nil {
		panic(err)
	}
	index := table
	for j := len(table) - 1; j >= 0; j-- {
		if table[j] == '.' {
			index = table[j+1:]
			break
		}
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s (
    message_id text        NOT NULL,
    consumer   text        NOT NULL,
    claimed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (message_id, consumer)
);

-- The purge removes old claims.
CREATE INDEX IF NOT EXISTS %[2]s_claimed_idx ON %[1]s (claimed_at);
`, table, index)
}

// schemaHeader opens schema.sql.
const schemaHeader = `-- The inbox table, as inbox.Schema(inbox.DefaultTable) returns it.
-- Apply it with your own migrations; svckit ships no migration runner.
`
