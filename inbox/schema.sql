-- The inbox table, as inbox.Schema(inbox.DefaultTable) returns it.
-- Apply it with your own migrations; svckit ships no migration runner.
CREATE TABLE IF NOT EXISTS svckit_inbox (
    message_id text        NOT NULL,
    consumer   text        NOT NULL,
    claimed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (message_id, consumer)
);

-- The purge removes old claims.
CREATE INDEX IF NOT EXISTS svckit_inbox_claimed_idx ON svckit_inbox (claimed_at);
