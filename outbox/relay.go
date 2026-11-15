package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dobrevit/svckit/eventbus"
	"github.com/dobrevit/svckit/logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher publishes one message and reports what the broker did with it.
// It must wait for the broker's confirmation, so that a nil error means the
// message is stored: amqpcluster.ClusterAdapter with
// ClusterConfig.PublisherConfirms is one. A publisher without confirms
// would make the relay mark rows published that the broker never took.
// With amqpcluster, a confirm timeout degrades the node until a health
// check finds it quiet again, so after a broker outage the relay resumes
// within about two of the adapter's HealthCheckInterval.
type Publisher interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
}

// RelayConfig configures a Relay. Zero values take the defaults given.
type RelayConfig struct {
	Table        string        // DefaultTable
	BatchSize    int           // 100 rows claimed at a time
	PollInterval time.Duration // 1s between polls when the table is idle

	// BackoffBase and BackoffMax bound the wait before a failed message is
	// tried again: BackoffBase doubled per attempt, up to BackoffMax (1s,
	// 5m).
	BackoffBase time.Duration
	BackoffMax  time.Duration

	// Retention, when positive, purges published and expired rows older
	// than it, every PurgeInterval (1h). Unpublished rows are never purged.
	Retention     time.Duration
	PurgeInterval time.Duration

	// StopTimeout bounds how long Run, once its context is cancelled, keeps
	// publishing the batch it has claimed (10s). What was published is
	// committed; the rest of the batch stays pending.
	StopTimeout time.Duration

	// OnExpired is told of every message that passed its ExpiresAt before it
	// could be published: it is marked expired and never sent, since a
	// one-time code or a reminder delivered late is worse than none.
	OnExpired func(ctx context.Context, msg Message)
}

// Relay publishes what Writers store.
type Relay struct {
	db     *sql.DB
	pub    Publisher
	config RelayConfig
}

// NewRelay returns a Relay over db and pub.
func NewRelay(db *sql.DB, pub Publisher, config RelayConfig) (*Relay, error) {
	table, err := checkTable(config.Table)
	if err != nil {
		return nil, err
	}
	config.Table = table
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.PollInterval <= 0 {
		config.PollInterval = time.Second
	}
	if config.BackoffBase <= 0 {
		config.BackoffBase = time.Second
	}
	if config.BackoffMax <= 0 {
		config.BackoffMax = 5 * time.Minute
	}
	if config.PurgeInterval <= 0 {
		config.PurgeInterval = time.Hour
	}
	if config.StopTimeout <= 0 {
		config.StopTimeout = 10 * time.Second
	}
	return &Relay{db: db, pub: pub, config: config}, nil
}

// Run relays until ctx is cancelled, then finishes or abandons the batch in
// hand within StopTimeout and returns ctx's error.
func (r *Relay) Run(ctx context.Context) error {
	// Publishing goes on for StopTimeout after ctx ends, so a batch is not
	// cut off mid-way by a shutdown that would leave every message in it
	// to be sent again.
	publishCtx, cancelPublish := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelPublish()
	stop := context.AfterFunc(ctx, func() { time.AfterFunc(r.config.StopTimeout, cancelPublish) })
	defer stop()

	nextPurge := time.Now()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n, err := r.relayBatch(publishCtx)
		if err != nil {
			logging.Error("outbox relay on %s: %v", r.config.Table, err)
		}
		r.observeBacklog(publishCtx)

		if r.config.Retention > 0 && !time.Now().Before(nextPurge) {
			if purged, err := r.Purge(publishCtx); err != nil {
				logging.Error("outbox purge on %s: %v", r.config.Table, err)
			} else if purged > 0 {
				logging.Info("outbox purge on %s removed %d rows", r.config.Table, purged)
			}
			nextPurge = time.Now().Add(r.config.PurgeInterval)
		}

		// A full batch means more may be waiting: go again at once.
		if n == r.config.BatchSize && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.config.PollInterval):
		}
	}
}

// claimed is a row taken for publishing.
type claimed struct {
	msg      Message
	attempts int
}

// relayBatch claims up to BatchSize pending rows, publishes them, and
// records each outcome in the same transaction. It returns how many rows it
// claimed.
func (r *Relay) relayBatch(ctx context.Context) (int, error) {
	// The transaction outlives a cancelled ctx long enough to commit what
	// was published.
	txCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.config.StopTimeout+30*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(txCtx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }() // after Commit, a no-op

	batch, err := r.claim(txCtx, tx)
	if err != nil || len(batch) == 0 {
		return 0, err
	}

	var expired []Message
	for _, c := range batch {
		if ctx.Err() != nil {
			break // the stop timeout ran out: leave the rest pending
		}
		now := time.Now()
		if !c.msg.ExpiresAt.IsZero() && !c.msg.ExpiresAt.After(now) {
			if _, err := tx.ExecContext(txCtx, `UPDATE `+r.config.Table+` SET expired_at = now() WHERE id = $1`, c.msg.ID); err != nil {
				return len(batch), err
			}
			relayed.WithLabelValues(r.config.Table, "expired").Inc()
			expired = append(expired, c.msg)
			continue
		}

		err := r.pub.PublishWithContext(ctx, c.msg.Exchange, c.msg.RoutingKey, true, false, publishing(c.msg, now))
		if err == nil {
			if _, err := tx.ExecContext(txCtx, `UPDATE `+r.config.Table+` SET published_at = now() WHERE id = $1`, c.msg.ID); err != nil {
				return len(batch), err
			}
			relayed.WithLabelValues(r.config.Table, "published").Inc()
			continue
		}

		label := failureLabel(err)
		relayed.WithLabelValues(r.config.Table, label).Inc()
		logging.Warn("outbox: message %s (%s) not published: %s", c.msg.ID, c.msg.RoutingKey, label)
		if _, err := tx.ExecContext(txCtx, `UPDATE `+r.config.Table+`
			SET attempts = attempts + 1,
			    next_attempt_at = now() + $2 * interval '1 millisecond',
			    last_error = $3
			WHERE id = $1`,
			c.msg.ID, r.backoff(c.attempts+1).Milliseconds(), label); err != nil {
			return len(batch), err
		}
	}

	if err := tx.Commit(); err != nil {
		return len(batch), fmt.Errorf("commit the batch: %w", err)
	}
	if r.config.OnExpired != nil {
		for _, msg := range expired {
			r.config.OnExpired(ctx, msg)
		}
	}
	return len(batch), nil
}

func (r *Relay) claim(ctx context.Context, tx *sql.Tx) ([]claimed, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id, exchange, routing_key, headers::text, content_type, body,
		       coalesce(subject_key, ''), expires_at, attempts
		  FROM `+r.config.Table+`
		 WHERE published_at IS NULL AND expired_at IS NULL AND next_attempt_at <= now()
		 ORDER BY created_at
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`, r.config.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var batch []claimed
	for rows.Next() {
		var c claimed
		var headers string
		var expires sql.NullTime
		if err := rows.Scan(&c.msg.ID, &c.msg.Exchange, &c.msg.RoutingKey, &headers, &c.msg.ContentType,
			&c.msg.Body, &c.msg.SubjectKey, &expires, &c.attempts); err != nil {
			return nil, fmt.Errorf("claim: %w", err)
		}
		if err := json.Unmarshal([]byte(headers), &c.msg.Headers); err != nil {
			return nil, fmt.Errorf("claim %s: headers: %w", c.msg.ID, err)
		}
		if expires.Valid {
			c.msg.ExpiresAt = expires.Time
		}
		batch = append(batch, c)
	}
	return batch, rows.Err()
}

// publishing is a stored message as it goes to the broker: persistent, with
// its id as MessageId, and with the time it has left as its expiration.
func publishing(msg Message, now time.Time) amqp.Publishing {
	p := amqp.Publishing{
		Headers:      table(msg.Headers),
		ContentType:  msg.ContentType,
		DeliveryMode: amqp.Persistent,
		MessageId:    msg.ID,
		Timestamp:    now,
		Body:         msg.Body,
	}
	if !msg.ExpiresAt.IsZero() {
		left := msg.ExpiresAt.Sub(now).Milliseconds()
		p.Expiration = strconv.FormatInt(max(left, 1), 10)
	}
	return p
}

// table converts decoded JSON headers to an amqp.Table, nested objects
// included, which the client requires.
func table(m map[string]any) amqp.Table {
	t := amqp.Table{}
	for k, v := range m {
		if nested, ok := v.(map[string]any); ok {
			t[k] = table(nested)
		} else {
			t[k] = v
		}
	}
	return t
}

// failureLabel is the short error stored in last_error and used as the
// metric outcome: never the message.
func failureLabel(err error) string {
	switch {
	case errors.Is(err, eventbus.ErrUnroutable):
		return "returned"
	case errors.Is(err, eventbus.ErrNacked):
		return "nacked"
	case errors.Is(err, eventbus.ErrConfirmTimeout), errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "failed"
	}
}

func (r *Relay) backoff(attempt int) time.Duration {
	d := r.config.BackoffBase
	for i := 1; i < attempt && d < r.config.BackoffMax; i++ {
		d *= 2
	}
	return min(d, r.config.BackoffMax)
}

// Purge deletes published and expired rows older than the retention, and
// returns how many. Unpublished rows are never touched.
func (r *Relay) Purge(ctx context.Context) (int64, error) {
	if r.config.Retention <= 0 {
		return 0, nil
	}
	res, err := r.db.ExecContext(ctx, `DELETE FROM `+r.config.Table+`
		WHERE (published_at IS NOT NULL AND published_at < now() - $1 * interval '1 millisecond')
		   OR (expired_at IS NOT NULL AND expired_at < now() - $1 * interval '1 millisecond')`,
		r.config.Retention.Milliseconds())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// observeBacklog sets the backlog and oldest-age gauges.
func (r *Relay) observeBacklog(ctx context.Context) {
	var n int64
	var oldest float64
	err := r.db.QueryRowContext(ctx, `SELECT count(*), coalesce(extract(epoch FROM now() - min(created_at)), 0)::float8
		  FROM `+r.config.Table+` WHERE published_at IS NULL AND expired_at IS NULL`).Scan(&n, &oldest)
	if err != nil {
		logging.Warn("outbox: measure the backlog of %s: %v", r.config.Table, err)
		return
	}
	backlog.WithLabelValues(r.config.Table).Set(float64(n))
	oldestAge.WithLabelValues(r.config.Table).Set(oldest)
}
