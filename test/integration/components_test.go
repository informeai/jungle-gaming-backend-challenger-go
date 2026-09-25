//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestLeastPrivilegeByComponent proves, with the real database roles, that
// each component can only do what its job requires.
func TestLeastPrivilegeByComponent(t *testing.T) {
	ctx := context.Background()
	appConn, err := pgx.Connect(ctx, appURL)
	if err != nil {
		t.Fatal(err)
	}
	defer appConn.Close(ctx)
	relayConn, err := pgx.Connect(ctx, relayURL)
	if err != nil {
		t.Fatal(err)
	}
	defer relayConn.Close(ctx)

	run := func(c *pgx.Conn, sql string) error {
		// Always rolled back: allowed statements must not leave traces.
		tx, err := c.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		_, err = tx.Exec(ctx, sql)
		return err
	}
	denied := []struct {
		who  string
		conn *pgx.Conn
		sql  string
	}{
		// The relay cannot see or touch money, nor forge/alter events.
		{"relay", relayConn, `SELECT 1 FROM wallets LIMIT 1`},
		{"relay", relayConn, `SELECT 1 FROM wager_transactions LIMIT 1`},
		{"relay", relayConn, `SELECT 1 FROM wallet_ledger_entries LIMIT 1`},
		{"relay", relayConn, `SELECT 1 FROM inbox_messages LIMIT 1`},
		{"relay", relayConn, `UPDATE wallets SET balance_minor = 0`},
		{"relay", relayConn, `INSERT INTO outbox_events (id, event_type, event_version, aggregate_type, aggregate_id, partition_key, correlation_id, payload, occurred_at)
			VALUES (gen_random_uuid(), 'WalletBalanceChanged', 1, 'Wallet', gen_random_uuid(), gen_random_uuid(), 'forged', '{}', now())`},
		{"relay", relayConn, `UPDATE outbox_events SET payload = '{}'::jsonb WHERE false`},
		{"relay", relayConn, `UPDATE outbox_events SET event_type = 'X' WHERE false`},
		{"relay", relayConn, `DELETE FROM outbox_events WHERE false`},
		// Money-moving components can append events but not read, claim or
		// mark them as published.
		{"app", appConn, `SELECT 1 FROM outbox_events LIMIT 1`},
		{"app", appConn, `UPDATE outbox_events SET published_at = now() WHERE false`},
		{"app", appConn, `UPDATE outbox_events SET locked_by = 'x' WHERE false`},
		{"app", appConn, `DELETE FROM wallet_ledger_entries WHERE false`},
		{"app", appConn, `UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE false`},
		{"app", appConn, `TRUNCATE inbox_messages`},
	}
	for _, d := range denied {
		if code := pgCode(run(d.conn, d.sql)); code != "42501" {
			t.Errorf("%s should be denied (42501), got %q: %s", d.who, code, d.sql)
		}
	}
	allowed := []struct {
		who  string
		conn *pgx.Conn
		sql  string
	}{
		{"relay", relayConn, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`},
		{"relay", relayConn, `UPDATE outbox_events SET locked_by = 'r', locked_until = now(), attempts = attempts + 1 WHERE false`},
		{"relay", relayConn, `SELECT id FROM outbox_events WHERE published_at IS NULL LIMIT 1 FOR UPDATE SKIP LOCKED`},
		{"relay", relayConn, `UPDATE outbox_events SET published_at = now(), next_attempt_at = now(), last_error = NULL WHERE false`},
		{"app", appConn, `SELECT 1 FROM wallets LIMIT 1`},
	}
	for _, a := range allowed {
		if err := run(a.conn, a.sql); err != nil {
			t.Errorf("%s should be allowed: %v: %s", a.who, err, a.sql)
		}
	}
}

// TestComponentsRunSeparately runs API, consumer, pending worker and outbox
// relay as four separate Fx applications, each with only its own
// credentials, and exercises a flow that crosses all of them.
func TestComponentsRunSeparately(t *testing.T) {
	isolateOutbox(t)
	q := createQueues(t)
	only := func(api, consumer, pending, outbox bool) cfgT {
		c := baseConfig(q)
		c.HTTP.APIEnabled, c.SQS.ConsumerEnabled, c.Pending.Enabled, c.Outbox.Enabled = api, consumer, pending, outbox
		return c
	}
	relayCfg := only(false, false, false, true)
	relayCfg.Database.URL = "" // the relay never receives the money-moving credentials
	relayCfg.OIDC.Issuer = ""
	workerCfg := only(false, true, false, false)
	workerCfg.OIDC.Issuer = "" // workers do not validate tokens

	api := startApp(t, only(true, false, false, false))
	consumer := startApp(t, workerCfg)
	pending := startApp(t, only(false, false, true, false))
	relay := startApp(t, relayCfg)

	// Readiness only checks what each component uses.
	wantDeps := map[*instance][]string{
		api:      {"postgres"},
		consumer: {"postgres", "sqs-ingress"},
		pending:  {"postgres"},
		relay:    {"postgres-outbox", "sqs-events"},
	}
	for inst, deps := range wantDeps {
		r := do(t, http.MethodGet, inst.Base+"/health/ready", "", nil, nil)
		got, _ := r.Body["dependencies"].(map[string]any)
		if r.Status != 200 || len(got) != len(deps) {
			t.Fatalf("readiness %s: %d %s (want %v)", inst.Base, r.Status, r.Raw, deps)
		}
		for _, d := range deps {
			if got[d] != "UP" {
				t.Fatalf("readiness %s missing %s: %s", inst.Base, d, r.Raw)
			}
		}
	}
	// Worker-only components expose no business routes.
	for _, inst := range []*instance{consumer, pending, relay} {
		if r := do(t, http.MethodPost, inst.Base+"/wallets", token(t, "wallet-internal"), map[string]any{}, nil); r.Status != http.StatusNotFound {
			t.Fatalf("worker %s served a business route: %d", inst.Base, r.Status)
		}
		if r := do(t, http.MethodGet, inst.Base+"/metrics", "", nil, nil); r.Status != 200 {
			t.Fatalf("worker metrics: %d", r.Status)
		}
	}

	// API: open wallet. Consumer: BET from the queue. API + pending worker:
	// REFUND before its BET. Relay: publishes everything.
	w := openWallet(t, api.Base, "100.00")
	queued := w.op(uniq("split-sqs-bet"), "BET", "20.00", "")
	sendMessage(t, q.IngressURL, uniq("msg"), queued, uniq("d"))
	waitFor(t, 15*time.Second, "consumer processed", func() bool { st, _ := txStatus(t, "provider-a", queued.External); return st == "PROCESSED" })

	bet := w.op(uniq("split-bet"), "BET", "10.00", "")
	refund := w.op(uniq("split-refund"), "REFUND", "10.00", bet.External)
	if r := submit(t, api.Base, refund); r.Status != http.StatusAccepted {
		t.Fatalf("refund: %d %s", r.Status, r.Raw)
	}
	if r := submit(t, api.Base, bet); r.Status != http.StatusOK {
		t.Fatalf("bet: %d %s", r.Status, r.Raw)
	}
	waitFor(t, 15*time.Second, "pending worker resolved", func() bool { st, _ := txStatus(t, "provider-a", refund.External); return st == "PROCESSED" })

	waitFor(t, 15*time.Second, "relay published all events", func() bool {
		total, published := outboxOf(t, []string{w.ID})
		return total > 0 && total == published
	})
	types := map[string]int{}
	for _, m := range receiveAll(t, q.EventsURL, 3*time.Second) {
		var env struct {
			EventType   string `json:"eventType"`
			AggregateID string `json:"aggregateId"`
		}
		_ = json.Unmarshal([]byte(m.Body), &env)
		types[env.EventType]++
	}
	for _, want := range []string{"WagerTransactionProcessed", "WalletBalanceChanged", "WagerTransactionPendingReference"} {
		if types[want] == 0 {
			t.Fatalf("event %s not published by the relay: %v", want, types)
		}
	}
	if s := stateOf(t, w.ID); s.Balance != 8000 || s.LedgerEntries != 4 {
		t.Fatalf("state %+v", s)
	}
	assertReconciled(t, api.Base, w.ID)
}
