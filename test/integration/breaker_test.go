//go:build integration

package integration

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/config"
)

// breakerState reads circuit_breaker_state{name} (0 closed, 1 half-open, 2 open).
func breakerState(t *testing.T, inst *instance, name string) float64 {
	t.Helper()
	mfs, err := inst.Metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "circuit_breaker_state" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "name" && l.GetValue() == name {
					return m.GetGauge().GetValue()
				}
			}
		}
	}
	t.Fatalf("breaker %s not found", name)
	return -1
}

// dbBehindProxy returns a config whose money-moving pool goes through a
// proxy that can black-hole PostgreSQL.
func dbBehindProxy(t *testing.T, cfg config.Config) (config.Config, *toggleProxy) {
	t.Helper()
	u, _ := url.Parse(appURL)
	proxy := newToggleProxy(t, u.Host)
	u.Host = proxy.ln.Addr().String()
	cfg.Database.URL = u.String()
	cfg.Database.ConnectTimeout = time.Second
	return cfg, proxy
}

// With PostgreSQL black-holed, the first calls time out, the breaker opens
// after FailureThreshold of them, and from then on the API answers 503 in
// milliseconds without opening connections to the dead database. When the
// database returns, a single probe closes the breaker.
func TestCircuitBreakerFailsFastAndRecovers(t *testing.T) {
	cfg := httpConfig(t)
	cfg.Outbox.Enabled, cfg.Pending.Enabled = false, false
	cfg.HTTP.RequestTimeout = 1500 * time.Millisecond
	cfg.Breaker = config.Breaker{FailureThreshold: 3, OpenTimeout: 2 * time.Second}
	cfg, proxy := dbBehindProxy(t, cfg)
	inst := startApp(t, cfg)
	w := openWallet(t, inst.Base, "100.00")

	proxy.SetHang(true)
	slow, opened := 0, false
	for i := range 6 {
		start := time.Now()
		r := submit(t, inst.Base, w.op(uniqf("bh", i, 0), "BET", "1.00", ""))
		if r.Status != http.StatusServiceUnavailable {
			t.Fatalf("during the outage: %d %s", r.Status, r.Raw)
		}
		if time.Since(start) > 500*time.Millisecond {
			slow++
			continue
		}
		opened = true
		break
	}
	if !opened || slow > 3 {
		t.Fatalf("breaker should open after at most 3 slow calls (slow=%d opened=%v)", slow, opened)
	}
	if st := breakerState(t, inst, "postgres"); st != 2 {
		t.Fatalf("breaker state %v, want open (2)", st)
	}

	// Open breaker: every call fails fast and nothing reaches the database.
	before := proxy.attempts.Load()
	parallel(20, func(i int) {
		start := time.Now()
		r := submit(t, inst.Base, w.op(uniqf("fast", i, 0), "BET", "1.00", ""))
		if d := time.Since(start); r.Status != http.StatusServiceUnavailable || d > 300*time.Millisecond {
			t.Errorf("open breaker must answer 503 fast: %d in %s", r.Status, d)
		}
		if r.Header.Get("Retry-After") != "2" {
			t.Errorf("Retry-After %q", r.Header.Get("Retry-After"))
		}
	})
	if n := proxy.attempts.Load() - before; n != 0 {
		t.Fatalf("%d connection attempts reached the dead database while the breaker was open", n)
	}
	if r := do(t, http.MethodGet, inst.Base+"/health/ready", "", nil, nil); r.Status != 503 || r.str("dependencies", "postgres") != "DOWN" {
		t.Fatalf("readiness while open: %d %s", r.Status, r.Raw)
	}

	// Recovery: after the open timeout a probe goes through and closes it.
	proxy.SetHang(false)
	recovered := w.op(uniq("after-outage"), "BET", "1.00", "")
	waitFor(t, 15*time.Second, "breaker to close", func() bool { return submit(t, inst.Base, recovered).Status == http.StatusOK })
	if st := breakerState(t, inst, "postgres"); st != 0 {
		t.Fatalf("breaker state %v after recovery", st)
	}
	if s := stateOf(t, w.ID); s.Debits != 1 || s.Balance != 9900 {
		t.Fatalf("only the post-outage bet may be applied: %+v", s)
	}
	assertReconciled(t, inst.Base, w.ID)
}

// A database outage longer than SQS_MAX_RECEIVES retries used to move valid
// messages to the DLQ. Now the consumer pauses polling while the breaker is
// open, and a failure during an outage never counts toward the DLQ threshold.
func TestConsumerPausesDuringDatabaseOutage(t *testing.T) {
	q := createQueues(t)
	cfg := baseConfig(q)
	cfg.Outbox.Enabled, cfg.Pending.Enabled = false, false
	cfg.SQS.MaxReceives = 2
	cfg.SQS.HandlerTimeout = time.Second
	cfg.SQS.VisibilityTimeout = 3 * time.Second
	cfg.SQS.RetryBaseDelay, cfg.SQS.RetryMaxDelay = 500*time.Millisecond, time.Second
	cfg.Breaker = config.Breaker{FailureThreshold: 2, OpenTimeout: time.Second}
	cfg, proxy := dbBehindProxy(t, cfg)
	inst := startApp(t, cfg)
	w := openWallet(t, inst.Base, "100.00")

	proxy.SetHang(true)
	var ops []wagerOp
	for i := range 4 {
		op := w.op(uniqf("outage", i, 0), "BET", "5.00", "")
		ops = append(ops, op)
		sendMessage(t, q.IngressURL, uniq("msg"), op, uniq("d"))
	}
	// Without the breaker, two failed receives (a few seconds) would send
	// these messages to the DLQ.
	time.Sleep(12 * time.Second)
	if dlq := receiveAll(t, q.DLQURL, time.Second); len(dlq) != 0 {
		t.Fatalf("%d valid messages went to the DLQ during the outage: %v", len(dlq), dlq[0].Attributes)
	}
	if st := breakerState(t, inst, "postgres"); st == 0 {
		t.Fatal("breaker should not be closed while the database is down")
	}

	proxy.SetHang(false)
	waitFor(t, 30*time.Second, "messages processed after recovery", func() bool {
		for _, op := range ops {
			if st, _ := txStatus(t, "provider-a", op.External); st != "PROCESSED" {
				return false
			}
		}
		return true
	})
	if dlq := receiveAll(t, q.DLQURL, time.Second); len(dlq) != 0 {
		t.Fatalf("DLQ after recovery: %d", len(dlq))
	}
	if s := stateOf(t, w.ID); s.Debits != 4 || s.Balance != 8000 {
		t.Fatalf("state %+v", s)
	}
}

// With SQS black-holed, the outbox relay stops claiming events once its SQS
// breaker opens (events are not leased only to fail and their attempts stay
// low); when SQS returns, everything is published.
func TestRelayPausesDuringSQSOutage(t *testing.T) {
	isolateOutbox(t)
	q := createQueues(t)
	u, _ := url.Parse(awsEndpoint)
	proxy := newToggleProxy(t, u.Host)
	cfg := baseConfig(q)
	cfg.SQS.ConsumerEnabled, cfg.Pending.Enabled = false, false
	cfg.AWS.EndpointURL = "http://" + proxy.ln.Addr().String()
	cfg.Breaker = config.Breaker{FailureThreshold: 2, OpenTimeout: time.Second}
	inst := startApp(t, cfg)

	proxy.SetHang(true)
	w := openWallet(t, inst.Base, "10.00") // commits 2 events
	time.Sleep(15 * time.Second)
	var maxAttempts, unpublished int
	if err := adminPool.QueryRow(context.Background(), `SELECT COALESCE(max(attempts), 0), count(*) FILTER (WHERE published_at IS NULL)
		FROM outbox_events WHERE partition_key = $1`, w.ID).Scan(&maxAttempts, &unpublished); err != nil {
		t.Fatal(err)
	}
	if unpublished != 2 || maxAttempts > 2 {
		t.Fatalf("relay kept retrying during the outage: unpublished=%d maxAttempts=%d", unpublished, maxAttempts)
	}
	if st := breakerState(t, inst, "sqs"); st == 0 {
		t.Fatal("sqs breaker should not be closed while SQS is down")
	}

	proxy.SetHang(false)
	waitFor(t, 30*time.Second, "events published after recovery", func() bool {
		_, p := outboxOf(t, []string{w.ID})
		return p == 2
	})
	if st := breakerState(t, inst, "sqs"); st != 0 {
		t.Fatalf("sqs breaker state %v after recovery", st)
	}
}
