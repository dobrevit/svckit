//go:build integration

package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dobrevit/svckit/amqpcluster"
	"github.com/prometheus/client_golang/prometheus/testutil"
	amqp "github.com/rabbitmq/amqp091-go"
)

func rabbitURL() string {
	if url := os.Getenv("RABBITMQ_URL"); url != "" {
		return url
	}
	return "amqp://test_user:test_password@localhost:5673/"
}

// confirming is a publisher that waits for the broker's confirmation. Its
// health interval is short: a confirm timeout degrades the node, and the
// outage test waits for it to come back.
func confirming(t *testing.T, timeout time.Duration) *amqpcluster.ClusterAdapter {
	t.Helper()
	ca, err := amqpcluster.NewClusterAdapter(amqpcluster.ClusterConfig{
		Nodes:               []string{rabbitURL()},
		ServiceName:         "outbox-it",
		PublisherConfirms:   true,
		ConfirmTimeout:      timeout,
		HealthCheckInterval: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ca.Close() })
	return ca
}

// queue declares a queue for routing key key on the default exchange's
// queue-name routing, deleted when the test ends: messages published to ""
// with the queue's name as key land in it.
func queue(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("outbox.it.%d", time.Now().UnixNano())
	withChannel(t, func(ch *amqp.Channel) {
		if _, err := ch.QueueDeclare(name, false, false, false, false, nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Cleanup(func() { withChannel(t, func(ch *amqp.Channel) { _, _ = ch.QueueDelete(name, false, false, false) }) })
	return name
}

func withChannel(t *testing.T, f func(*amqp.Channel)) {
	t.Helper()
	conn, err := amqp.Dial(rabbitURL())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	f(ch)
}

func drain(t *testing.T, q string) []amqp.Delivery {
	t.Helper()
	var got []amqp.Delivery
	withChannel(t, func(ch *amqp.Channel) {
		for {
			d, ok, err := ch.Get(q, true)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				return
			}
			got = append(got, d)
		}
	})
	return got
}

func write(t *testing.T, db *sql.DB, table string, msgs ...Message) {
	t.Helper()
	w, err := NewWriter(table)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range msgs {
		if err := w.Write(context.Background(), db, m); err != nil {
			t.Fatal(err)
		}
	}
}

func relayFor(t *testing.T, db *sql.DB, pub Publisher, table string, config RelayConfig) *Relay {
	t.Helper()
	config.Table = table
	if config.BackoffBase == 0 {
		config.BackoffBase = 10 * time.Millisecond
	}
	r, err := NewRelay(db, pub, config)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestACommittedMessageIsDeliveredOnceAndMarked(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db := testDB(t)
	table := freshTable(t, db)
	q := queue(t)
	write(t, db, table, Message{ID: "m-1", RoutingKey: q, Body: []byte(`{"n":1}`), Headers: map[string]any{"signed": false}})
	r := relayFor(t, db, confirming(t, 5*time.Second), table, RelayConfig{})

	for range 3 {
		if _, err := r.relayBatch(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	got := drain(t, q)
	if len(got) != 1 || got[0].MessageId != "m-1" || string(got[0].Body) != `{"n":1}` || got[0].Headers["signed"] != false {
		t.Fatalf("delivered %d messages: %+v", len(got), got)
	}
	if n := count(t, db, table, "id = 'm-1' AND published_at IS NOT NULL"); n != 1 {
		t.Error("the delivered message was not marked published")
	}
}

func TestAnUnroutableMessageStaysPendingWithItsError(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db := testDB(t)
	table := freshTable(t, db)
	write(t, db, table, Message{ID: "lost", RoutingKey: fmt.Sprintf("nobody.%d", time.Now().UnixNano()), Body: []byte("x")})
	r := relayFor(t, db, confirming(t, 5*time.Second), table, RelayConfig{BackoffBase: time.Minute})

	if _, err := r.relayBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	var attempts int
	var lastError string
	var later bool
	if err := db.QueryRow("SELECT attempts, last_error, next_attempt_at > now() FROM "+table+" WHERE id = 'lost' AND published_at IS NULL").
		Scan(&attempts, &lastError, &later); err != nil {
		t.Fatalf("the unroutable row: %v", err)
	}
	if attempts != 1 || lastError != "returned" || !later {
		t.Errorf("attempts %d, last_error %q, retried later %v", attempts, lastError, later)
	}
}

func pauseBroker(t *testing.T) (resume func()) {
	t.Helper()
	if out, err := exec.Command("docker", "compose", "pause", "rabbitmq-test").CombinedOutput(); err != nil {
		t.Fatalf("pause: %v\n%s", err, out)
	}
	resumed := false
	resume = func() {
		if !resumed {
			resumed = true
			_ = exec.Command("docker", "compose", "unpause", "rabbitmq-test").Run()
		}
	}
	t.Cleanup(resume)
	return resume
}

// With the broker frozen nothing is confirmed, so the rows stay pending;
// they go once it is back.
func TestRowsWaitOutABrokerOutage(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db := testDB(t)
	table := freshTable(t, db)
	q := queue(t)
	pub := confirming(t, time.Second)
	r := relayFor(t, db, pub, table, RelayConfig{})
	write(t, db, table, Message{ID: "during-outage", RoutingKey: q, Body: []byte("x")})

	resume := pauseBroker(t)
	if _, err := r.relayBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	var lastError string
	_ = db.QueryRow("SELECT coalesce(last_error, '') FROM " + table + " WHERE id = 'during-outage' AND published_at IS NULL").Scan(&lastError)
	if lastError != "timeout" {
		t.Errorf("during the outage last_error = %q, want timeout and the row pending", lastError)
	}

	resume()
	deadline := time.Now().Add(30 * time.Second)
	for count(t, db, table, "published_at IS NOT NULL") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the row was not published after the broker came back")
		}
		_, _ = r.relayBatch(context.Background())
		time.Sleep(100 * time.Millisecond)
	}
}

// A one-time code delivered late is worse than none.
func TestAMessageThatExpiredWhileTheBrokerWasDownIsNeverPublished(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db := testDB(t)
	table := freshTable(t, db)
	q := queue(t)

	var mu sync.Mutex
	var expired []Message
	r := relayFor(t, db, confirming(t, time.Second), table, RelayConfig{OnExpired: func(_ context.Context, m Message) {
		mu.Lock()
		expired = append(expired, m)
		mu.Unlock()
	}})
	write(t, db, table, Message{ID: "code", RoutingKey: q, Body: []byte("123456"), ExpiresAt: time.Now().Add(500 * time.Millisecond)})

	resume := pauseBroker(t)
	time.Sleep(time.Second) // the code expires while the broker is down
	resume()

	if _, err := r.relayBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := drain(t, q); len(got) != 0 {
		t.Errorf("an expired message was delivered: %+v", got)
	}
	if n := count(t, db, table, "id = 'code' AND expired_at IS NOT NULL AND published_at IS NULL"); n != 1 {
		t.Error("the expired message was not marked expired")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(expired) != 1 || expired[0].ID != "code" {
		t.Errorf("OnExpired got %+v", expired)
	}
}

func TestAMessageWithTimeLeftCarriesItAsItsExpiration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db := testDB(t)
	table := freshTable(t, db)
	q := queue(t)
	write(t, db, table, Message{ID: "soon", RoutingKey: q, Body: []byte("x"), ExpiresAt: time.Now().Add(2 * time.Second)})
	r := relayFor(t, db, confirming(t, 5*time.Second), table, RelayConfig{})

	if _, err := r.relayBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := drain(t, q)
	if len(got) != 1 {
		t.Fatalf("%d messages delivered", len(got))
	}
	if ms, err := strconv.Atoi(got[0].Expiration); err != nil || ms <= 0 || ms > 2000 {
		t.Errorf("expiration = %q, want at most 2000 ms", got[0].Expiration)
	}
}

// SKIP LOCKED: two relays on one table each take rows the other has not.
func TestTwoRelaysPublishEachRowOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db := testDB(t)
	table := freshTable(t, db)
	q := queue(t)
	const n = 60
	for i := range n {
		write(t, db, table, Message{ID: fmt.Sprintf("m-%02d", i), RoutingKey: q, Body: []byte("x")})
	}

	var wg sync.WaitGroup
	for range 2 {
		r := relayFor(t, db, confirming(t, 5*time.Second), table, RelayConfig{BatchSize: 7})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				claimed, err := r.relayBatch(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				if claimed == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	seen := map[string]int{}
	for _, d := range drain(t, q) {
		seen[d.MessageId]++
	}
	if len(seen) != n {
		t.Errorf("%d distinct messages delivered, want %d", len(seen), n)
	}
	for id, times := range seen {
		if times != 1 {
			t.Errorf("%s delivered %d times", id, times)
		}
	}
}

func TestThePurgeKeepsEveryUnpublishedRow(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db := testDB(t)
	table := freshTable(t, db)
	write(t, db, table,
		Message{ID: "old-published", RoutingKey: "q", Body: []byte("x")},
		Message{ID: "old-expired", RoutingKey: "q", Body: []byte("x")},
		Message{ID: "new-published", RoutingKey: "q", Body: []byte("x")},
		Message{ID: "old-pending", RoutingKey: "q", Body: []byte("x")},
	)
	for _, stmt := range []string{
		"UPDATE " + table + " SET published_at = now() - interval '2 hours' WHERE id = 'old-published'",
		"UPDATE " + table + " SET expired_at = now() - interval '2 hours' WHERE id = 'old-expired'",
		"UPDATE " + table + " SET published_at = now() WHERE id = 'new-published'",
		"UPDATE " + table + " SET created_at = now() - interval '30 days' WHERE id = 'old-pending'",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}

	r := relayFor(t, db, nil, table, RelayConfig{Retention: time.Hour})
	purged, err := r.Purge(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if purged != 2 {
		t.Errorf("purged %d rows, want the old published and old expired ones", purged)
	}
	for _, id := range []string{"new-published", "old-pending"} {
		if count(t, db, table, "id = $1", id) != 1 {
			t.Errorf("%s was purged", id)
		}
	}
}

func TestRunStopsWithinTheStopTimeoutAndReportsTheBacklog(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db := testDB(t)
	table := freshTable(t, db)
	for i := range 3 {
		write(t, db, table, Message{ID: fmt.Sprintf("nowhere-%d", i), RoutingKey: fmt.Sprintf("nobody.%d", time.Now().UnixNano()), Body: []byte("x")})
	}
	r := relayFor(t, db, confirming(t, 5*time.Second), table, RelayConfig{PollInterval: 50 * time.Millisecond, BackoffBase: time.Minute, StopTimeout: 500 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	time.Sleep(300 * time.Millisecond)

	if got := testutil.ToFloat64(backlog.WithLabelValues(table)); got != 3 {
		t.Errorf("backlog gauge = %v, want the 3 unroutable rows", got)
	}
	if got := testutil.ToFloat64(oldestAge.WithLabelValues(table)); got <= 0 {
		t.Errorf("oldest unpublished age = %v, want positive", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within its 500ms stop timeout")
	}
}
