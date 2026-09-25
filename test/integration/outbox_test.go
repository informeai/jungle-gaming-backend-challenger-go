//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/config"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/infra/postgres"
	isqs "github.com/informeai/jungle-gaming-backend-challenger-go/internal/infra/sqs"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/observability"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/worker"
)

var quietLog = slog.New(slog.NewTextHandler(io.Discard, nil))

// isolateOutbox marks events left unpublished by earlier tests as published,
// so relays in this test only see the events it creates.
func isolateOutbox(t *testing.T) {
	t.Helper()
	if _, err := adminPool.Exec(context.Background(), `UPDATE outbox_events SET published_at = now(), locked_by = NULL WHERE published_at IS NULL`); err != nil {
		t.Fatal(err)
	}
}

func outboxOf(t *testing.T, walletIDs []string) (total, published int) {
	t.Helper()
	err := adminPool.QueryRow(context.Background(), `SELECT count(*), count(published_at) FROM outbox_events
		WHERE partition_key = ANY($1::uuid[])`, walletIDs).Scan(&total, &published)
	if err != nil {
		t.Fatal(err)
	}
	return total, published
}

// recordingPublisher wraps the real SQS publisher and records who sent what.
type recordingPublisher struct {
	inner worker.Publisher
	mu    sync.Mutex
	sent  map[uuid.UUID]int
	fail  atomic.Bool
}

func (r *recordingPublisher) Publish(ctx context.Context, m postgres.OutboxMessage) error {
	if r.fail.Load() {
		return errors.New("broker unavailable (simulated)")
	}
	if err := r.inner.Publish(ctx, m); err != nil {
		return err
	}
	r.mu.Lock()
	r.sent[m.ID]++
	r.mu.Unlock()
	return nil
}

func newRelay(t *testing.T, pub worker.Publisher, cfg config.Outbox) *worker.OutboxRelay {
	t.Helper()
	// The relay runs with its real least-privileged role (wallet_relay).
	txm := postgres.NewTxManager(relayPool, nil)
	return worker.NewOutboxRelay(postgres.NewOutboxRepo(txm), pub, "relay-"+uuid.NewString()[:8], cfg, observability.NewMetrics(), quietLog)
}

func sqsPublisher(q queueSet) *isqs.EventPublisher {
	return isqs.NewEventPublisher(sqsClient, isqs.Queues{Events: q.EventsURL})
}

// Scenario 6: two publishers compete for the same outbox.
func TestOutboxCompetingPublishers(t *testing.T) {
	isolateOutbox(t)
	q := createQueues(t)
	cfg := httpConfig(t)
	cfg.Outbox.Enabled = false // events stay committed and unpublished
	inst := startApp(t, cfg)
	var wallets []string
	for range 25 {
		wallets = append(wallets, openWallet(t, inst.Base, "10.00").ID)
	}
	total, published := outboxOf(t, wallets)
	if total != 50 || published != 0 {
		t.Fatalf("outbox before relays: total=%d published=%d", total, published)
	}

	ocfg := config.Outbox{BatchSize: 5, Lease: 10 * time.Second, BaseBackoff: 100 * time.Millisecond, MaxBackoff: time.Second}
	pubA := &recordingPublisher{inner: sqsPublisher(q), sent: map[uuid.UUID]int{}}
	pubB := &recordingPublisher{inner: sqsPublisher(q), sent: map[uuid.UUID]int{}}
	relays := []*worker.OutboxRelay{newRelay(t, pubA, ocfg), newRelay(t, pubB, ocfg)}
	parallel(2, func(i int) {
		for range 100 {
			relays[i].Tick(context.Background())
			if _, p := outboxOf(t, wallets); p == 50 {
				return
			}
		}
	})
	if _, p := outboxOf(t, wallets); p != 50 {
		t.Fatalf("published %d/50", p)
	}
	for id := range pubA.sent {
		if pubB.sent[id] > 0 {
			t.Fatalf("event %s published by both relays", id)
		}
	}
	if len(pubA.sent) == 0 || len(pubB.sent) == 0 {
		t.Logf("work split A=%d B=%d (one relay drained everything)", len(pubA.sent), len(pubB.sent))
	}
	msgs := receiveAll(t, q.EventsURL, 3*time.Second)
	ids := map[string]bool{}
	for _, m := range msgs {
		var env struct {
			EventID string `json:"eventId"`
		}
		_ = json.Unmarshal([]byte(m.Body), &env)
		if env.EventID != m.Attributes["eventId"] {
			t.Fatalf("attribute eventId %s != body %s", m.Attributes["eventId"], env.EventID)
		}
		ids[env.EventID] = true
	}
	if len(ids) != 50 || len(msgs) != 50 {
		t.Fatalf("events queue: %d messages, %d distinct", len(msgs), len(ids))
	}
}

// A failed publication is rescheduled with backoff and retried later.
func TestOutboxRetryWithBackoff(t *testing.T) {
	isolateOutbox(t)
	q := createQueues(t)
	cfg := httpConfig(t)
	cfg.Outbox.Enabled = false
	inst := startApp(t, cfg)
	w := openWallet(t, inst.Base, "10.00")

	pub := &recordingPublisher{inner: sqsPublisher(q), sent: map[uuid.UUID]int{}}
	pub.fail.Store(true)
	relay := newRelay(t, pub, config.Outbox{BatchSize: 10, Lease: 5 * time.Second, BaseBackoff: 500 * time.Millisecond, MaxBackoff: 2 * time.Second})
	relay.Tick(context.Background())

	var attempts int
	var lastError *string
	var next time.Time
	err := adminPool.QueryRow(context.Background(), `SELECT max(attempts), max(last_error), min(next_attempt_at) FROM outbox_events
		WHERE partition_key = $1`, w.ID).Scan(&attempts, &lastError, &next)
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || lastError == nil || !next.After(time.Now()) {
		t.Fatalf("after failure: attempts=%d lastError=%v next=%v", attempts, lastError, next)
	}
	pub.fail.Store(false)
	if n := relay.Tick(context.Background()); n != 0 {
		t.Fatalf("event republished before its backoff elapsed (%d)", n)
	}
	waitFor(t, 5*time.Second, "published after backoff", func() bool {
		relay.Tick(context.Background())
		_, p := outboxOf(t, []string{w.ID})
		return p == 2
	})
	if relay.Backoff(1) != 500*time.Millisecond || relay.Backoff(3) != 2*time.Second || relay.Backoff(10) != 2*time.Second {
		t.Fatal("unexpected backoff schedule")
	}
}

// Interruption between commit and publication (events committed while no
// relay runs) and between publication and confirmation (process killed right
// after SendMessage): another instance takes over and keeps the eventId.
func TestOutboxRecoveryAfterCrash(t *testing.T) {
	isolateOutbox(t)
	q := createQueues(t)
	cfg := httpConfig(t)
	cfg.Outbox.Enabled = false
	api := startApp(t, cfg)
	w := openWallet(t, api.Base, "10.00") // 2 events committed, none published

	crashCfg := baseConfig(q)
	crashCfg.SQS.ConsumerEnabled, crashCfg.Pending.Enabled = false, false
	crashCfg.FaultInjection = "outbox-crash-after-publish"
	crashCfg.Outbox.Lease = 2 * time.Second
	crashing := launch(t, crashCfg, false) // crashes on its first publication
	if code := crashing.WaitExit(t, 20*time.Second); code != 137 {
		t.Fatalf("expected fault-injected exit, got %d", code)
	}
	var crashedID uuid.UUID
	var attempts int
	err := adminPool.QueryRow(context.Background(), `SELECT id, attempts FROM outbox_events
		WHERE partition_key = $1 AND locked_by IS NOT NULL AND published_at IS NULL`, w.ID).Scan(&crashedID, &attempts)
	if err != nil {
		t.Fatalf("the crashed publisher must leave its lease behind: %v", err)
	}

	// A healthy instance reclaims the event after the lease expires.
	healthy := baseConfig(q)
	healthy.SQS.ConsumerEnabled = false
	startApp(t, healthy)
	waitFor(t, 20*time.Second, "all events published", func() bool { _, p := outboxOf(t, []string{w.ID}); return p == 2 })
	_ = adminPool.QueryRow(context.Background(), `SELECT attempts FROM outbox_events WHERE id = $1`, crashedID).Scan(&attempts)
	if attempts < 2 {
		t.Fatalf("crashed event should have been claimed again, attempts=%d", attempts)
	}
	seen := map[string]int{}
	for _, m := range receiveAll(t, q.EventsURL, 3*time.Second) {
		seen[m.Attributes["eventId"]]++
	}
	// SQS FIFO drops the republication inside the 5-minute dedup window
	// because MessageDeduplicationId = eventId; the id itself never changes.
	if seen[crashedID.String()] < 1 || len(seen) != 2 {
		t.Fatalf("events received: %v (crashed %s)", seen, crashedID)
	}
}
