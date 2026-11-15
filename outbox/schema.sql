-- The outbox table, as outbox.Schema(outbox.DefaultTable) returns it.
-- Apply it with your own migrations; svckit ships no migration runner.
CREATE TABLE IF NOT EXISTS svckit_outbox (
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
CREATE INDEX IF NOT EXISTS svckit_outbox_pending_idx
    ON svckit_outbox (next_attempt_at, created_at)
    WHERE published_at IS NULL AND expired_at IS NULL;

-- An erasure finds the messages about one subject.
CREATE INDEX IF NOT EXISTS svckit_outbox_subject_idx
    ON svckit_outbox (subject_key)
    WHERE subject_key IS NOT NULL;

-- The retention purge removes old published and expired rows.
CREATE INDEX IF NOT EXISTS svckit_outbox_published_idx
    ON svckit_outbox (published_at)
    WHERE published_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS svckit_outbox_expired_idx
    ON svckit_outbox (expired_at)
    WHERE expired_at IS NOT NULL;
