//go:build integration

package integration

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func outboxTypes(t *testing.T, walletID string) map[string]int {
	t.Helper()
	rows, err := adminPool.Query(context.Background(), `SELECT event_type, count(*) FROM outbox_events WHERE partition_key = $1 GROUP BY 1`, walletID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var k string
		var n int
		_ = rows.Scan(&k, &n)
		out[k] = n
	}
	return out
}

// Scenario 7: REFUND/ROLLBACK before the reference, resolved later (HTTP and
// SQS), and rejected by expiry.
func TestReversalBeforeReference(t *testing.T) {
	q := createQueues(t)
	inst := startApp(t, baseConfig(q))
	w := openWallet(t, inst.Base, "100.00")

	bet := w.op(uniq("late-bet"), "BET", "30.00", "")
	refund := w.op(uniq("early-refund"), "REFUND", "30.00", bet.External)
	r := submit(t, inst.Base, refund)
	if r.Status != http.StatusAccepted || r.str("status") != "PENDING_REFERENCE" {
		t.Fatalf("refund before bet: %d %s", r.Status, r.Raw)
	}
	if got := outboxTypes(t, w.ID)["WagerTransactionPendingReference"]; got != 1 {
		t.Fatalf("pending event count %d", got)
	}
	if again := submit(t, inst.Base, refund); again.Status != http.StatusAccepted || again.Body["idempotentReplay"] != true {
		t.Fatalf("replay of pending: %d %s", again.Status, again.Raw)
	}
	// The ROLLBACK of the bet arrives through SQS, also before the bet.
	rollback := w.op(uniq("early-rollback"), "ROLLBACK", "30.00", bet.External)
	sendMessage(t, q.IngressURL, uniq("msg"), rollback, uniq("d"))
	waitFor(t, 15*time.Second, "rollback pending", func() bool { st, _ := txStatus(t, "provider-a", rollback.External); return st == "PENDING_REFERENCE" })

	if r := submit(t, inst.Base, bet); r.Status != http.StatusOK {
		t.Fatalf("bet: %d %s", r.Status, r.Raw)
	}
	waitFor(t, 15*time.Second, "reversals resolved", func() bool {
		a, _ := txStatus(t, "provider-a", refund.External)
		b, _ := txStatus(t, "provider-a", rollback.External)
		return a != "PENDING_REFERENCE" && b != "PENDING_REFERENCE"
	})
	// Exactly one of the two compensations of the same bet succeeds.
	sa, fa := txStatus(t, "provider-a", refund.External)
	sb, fb := txStatus(t, "provider-a", rollback.External)
	if !((sa == "PROCESSED" && sb == "REJECTED" && fb == "REFERENCE_ALREADY_REVERSED") ||
		(sb == "PROCESSED" && sa == "REJECTED" && fa == "REFERENCE_ALREADY_REVERSED")) {
		t.Fatalf("refund=%s/%s rollback=%s/%s", sa, fa, sb, fb)
	}
	if s := stateOf(t, w.ID); s.Balance != 10000 || s.LedgerEntries != 3 {
		t.Fatalf("state %+v", s)
	}
	g := do(t, http.MethodGet, inst.Base+"/providers/provider-a/wagering/transactions/"+refund.External, token(t, "provider-a"), nil, nil)
	if g.Status != 200 || g.str("status") != sa {
		t.Fatalf("GET refund: %d %s", g.Status, g.Raw)
	}
	assertReconciled(t, inst.Base, w.ID)
}

func TestPendingReferenceExpires(t *testing.T) {
	cfg := httpConfig(t)
	cfg.Pending.MaxAttempts = 3
	cfg.Pending.BaseDelay = 100 * time.Millisecond
	inst := startApp(t, cfg)
	w := openWallet(t, inst.Base, "100.00")
	refund := w.op(uniq("orphan"), "REFUND", "5.00", uniq("never-arrives"))
	if r := submit(t, inst.Base, refund); r.Status != http.StatusAccepted {
		t.Fatalf("refund: %d %s", r.Status, r.Raw)
	}
	waitFor(t, 10*time.Second, "expiry", func() bool { st, _ := txStatus(t, "provider-a", refund.External); return st == "REJECTED" })
	if _, code := txStatus(t, "provider-a", refund.External); code != "REFERENCE_NOT_FOUND" {
		t.Fatalf("failure code %s", code)
	}
	if got := outboxTypes(t, w.ID)["WagerTransactionRejected"]; got != 1 {
		t.Fatalf("rejection event count %d", got)
	}
	r := submit(t, inst.Base, refund)
	if r.Status != http.StatusUnprocessableEntity || r.str("failureCode") != "REFERENCE_NOT_FOUND" || r.Body["idempotentReplay"] != true {
		t.Fatalf("replay after expiry: %d %s", r.Status, r.Raw)
	}
}

// Scenario 8: restart everything (SIGKILL) and verify idempotency, pending
// operations and financial consistency are preserved.
func TestRestartPreservesState(t *testing.T) {
	q := createQueues(t)
	cfg := baseConfig(q)
	cfg.Pending.Enabled = false // the first process only accepts; it never resolves
	first := startProcess(t, cfg)
	w := openWallet(t, first.Base, "100.00")
	bet := w.op(uniq("rs-bet"), "BET", "25.00", "")
	orig := submit(t, first.Base, bet)
	win := w.op(uniq("rs-win"), "WIN", "5.00", "")
	submit(t, first.Base, win)
	missing := uniq("rs-missing")
	refund := w.op(uniq("rs-refund"), "REFUND", "10.00", missing)
	if r := submit(t, first.Base, refund); r.Status != http.StatusAccepted {
		t.Fatalf("pending refund: %d %s", r.Status, r.Raw)
	}
	first.Kill()

	second := startProcess(t, baseConfig(q))
	r := submit(t, second.Base, bet)
	if r.Status != 200 || r.Body["idempotentReplay"] != true || r.str("transactionId") != orig.str("transactionId") ||
		r.str("balance", "amount") != "75.00" {
		t.Fatalf("replay after restart must return the original result: %d %s", r.Status, r.Raw)
	}
	if r := submit(t, second.Base, w.op(missing, "BET", "10.00", "")); r.Status != 200 {
		t.Fatalf("reference: %d %s", r.Status, r.Raw)
	}
	waitFor(t, 15*time.Second, "pending resumed by the new process", func() bool {
		st, _ := txStatus(t, "provider-a", refund.External)
		return st == "PROCESSED"
	})
	if s := stateOf(t, w.ID); s.Balance != 8000 || s.LedgerEntries != 5 {
		t.Fatalf("state %+v", s)
	}
	assertReconciled(t, second.Base, w.ID)
}

// Fx composition: start, workers running, stop, resources released.
func TestFxLifecycle(t *testing.T) {
	inst := startApp(t, baseConfig(createQueues(t)))
	if !inst.Outbox.Running() || !inst.Pending.Running() {
		t.Fatal("workers must be running after start")
	}
	if r := do(t, http.MethodGet, inst.Base+"/health/ready", "", nil, nil); r.Status != 200 {
		t.Fatalf("ready %d", r.Status)
	}
	inst.Stop(t)
	if inst.Outbox.Running() || inst.Pending.Running() {
		t.Fatal("workers must have terminated after stop")
	}
	if err := inst.Pool.Ping(context.Background()); err == nil {
		t.Fatal("pool must be closed after stop")
	}
	if _, err := http.Get(inst.Base + "/health/live"); err == nil {
		t.Fatal("server must not accept connections after stop")
	}
}

// Scenario 4: three independent OS processes (own memory and pools),
// consuming the same queue and receiving HTTP traffic concurrently.
func TestThreeIndependentProcesses(t *testing.T) {
	q := createQueues(t)
	procs := []*process{startProcess(t, baseConfig(q)), startProcess(t, baseConfig(q)), startProcess(t, baseConfig(q))}
	bases := []string{procs[0].Base, procs[1].Base, procs[2].Base}

	// 50 copies of the same bet spread over the processes.
	w := openWallet(t, bases[0], "100.00")
	op := w.op(uniq("mp-dup"), "BET", "25.00", "")
	var fresh atomic.Int32
	parallel(50, func(i int) {
		r := submit(t, bases[i%3], op)
		if r.Status != 200 {
			t.Errorf("dup %d: %d %s", i, r.Status, r.Raw)
		}
		if r.Body["idempotentReplay"] == false {
			fresh.Add(1)
		}
	})
	if s := stateOf(t, w.ID); fresh.Load() != 1 || s.Debits != 1 || s.Balance != 7500 {
		t.Fatalf("fresh=%d state=%+v", fresh.Load(), s)
	}

	// The 80/80 race with each bet on a different process.
	runTwoBetsRace(t, []string{bases[1], bases[2]}, bases[0])

	// Distinct wallets in parallel on all processes, plus HTTP+SQS for the
	// same operations.
	ws := make([]walletRef, 9)
	for i := range ws {
		ws[i] = openWallet(t, bases[i%3], "50.00")
	}
	ops := make([]wagerOp, 0, 27)
	for i, wr := range ws {
		for j := range 3 {
			ops = append(ops, wr.op(uniqf("mp", i, j), "BET", "5.00", ""))
		}
	}
	for _, o := range ops {
		sendMessage(t, q.IngressURL, uniq("msg"), o, uniq("d"))
	}
	parallel(len(ops), func(i int) {
		if r := submit(t, bases[i%3], ops[i]); r.Status != 200 {
			t.Errorf("bet: %d %s", r.Status, r.Raw)
		}
	})
	waitFor(t, 30*time.Second, "queue drained by the three consumers", func() bool {
		v, f := queueDepth(t, q.IngressURL)
		return v == 0 && f == 0
	})
	for _, wr := range ws {
		if s := stateOf(t, wr.ID); s.Balance != 3500 || s.Debits != 3 {
			t.Fatalf("wallet %s: %+v", wr.ID, s)
		}
		assertReconciled(t, bases[1], wr.ID)
	}
}

// ------------------------------------------------ PostgreSQL unavailable ---

// toggleProxy is a TCP proxy to a dependency (PostgreSQL, LocalStack) that
// can refuse every connection (SetDown) or black-hole traffic (SetHang):
// connections are accepted but nothing is ever answered, the worst kind of
// outage because callers only find out by timing out.
type toggleProxy struct {
	ln       net.Listener
	target   string
	down     atomic.Bool
	hang     atomic.Bool
	attempts atomic.Int64 // connection attempts received
	mu       sync.Mutex
	conns    []net.Conn
}

func newToggleProxy(t *testing.T, target string) *toggleProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &toggleProxy{ln: ln, target: target}
	go p.serve()
	t.Cleanup(func() { ln.Close(); p.cut() })
	return p
}

func (p *toggleProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.attempts.Add(1)
		if p.down.Load() {
			c.Close()
			continue
		}
		if p.hang.Load() {
			p.mu.Lock()
			p.conns = append(p.conns, c) // held open, never answered
			p.mu.Unlock()
			continue
		}
		up, err := net.Dial("tcp", p.target)
		if err != nil {
			c.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, c, up)
		p.mu.Unlock()
		go func() { p.pump(up, c); up.Close() }()
		go func() { p.pump(c, up); c.Close() }()
	}
}

// pump forwards bytes, silently dropping them while hanging.
func (p *toggleProxy) pump(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if err != nil {
			return
		}
		if p.hang.Load() {
			continue
		}
		if _, err := dst.Write(buf[:n]); err != nil {
			return
		}
	}
}

// SetHang black-holes new and existing connections (true) or restores them.
func (p *toggleProxy) SetHang(hang bool) { p.hang.Store(hang) }

func (p *toggleProxy) cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.Close()
	}
	p.conns = nil
}

func (p *toggleProxy) SetDown(down bool) {
	p.down.Store(down)
	if down {
		p.cut()
	}
}

func TestPostgresTemporarilyUnavailable(t *testing.T) {
	u, _ := url.Parse(appURL)
	proxy := newToggleProxy(t, u.Host)
	u.Host = proxy.ln.Addr().String()
	cfg := httpConfig(t)
	cfg.Database.URL = u.String()
	cfg.Database.ConnectTimeout = time.Second
	cfg.HTTP.RequestTimeout = 3 * time.Second
	inst := startApp(t, cfg)
	w := openWallet(t, inst.Base, "100.00")
	op := w.op(uniq("outage"), "BET", "10.00", "")

	proxy.SetDown(true)
	r := submit(t, inst.Base, op)
	if r.Status != http.StatusServiceUnavailable || r.str("error", "code") != "SERVICE_UNAVAILABLE" {
		t.Fatalf("during outage: %d %s", r.Status, r.Raw)
	}
	if ready := do(t, http.MethodGet, inst.Base+"/health/ready", "", nil, nil); ready.Status != 503 || ready.str("dependencies", "postgres") != "DOWN" {
		t.Fatalf("readiness during outage: %d %s", ready.Status, ready.Raw)
	}
	if st, _ := txStatus(t, "provider-a", op.External); st != "" {
		t.Fatal("nothing may be persisted during the outage")
	}

	proxy.SetDown(false)
	waitFor(t, 10*time.Second, "recovery", func() bool {
		r = submit(t, inst.Base, op)
		return r.Status == http.StatusOK
	})
	if r.Body["idempotentReplay"] != false {
		t.Fatalf("first successful attempt must be the original: %s", r.Raw)
	}
	if again := submit(t, inst.Base, op); again.Body["idempotentReplay"] != true {
		t.Fatalf("retry after recovery: %s", again.Raw)
	}
	if s := stateOf(t, w.ID); s.Debits != 1 {
		t.Fatalf("state %+v", s)
	}
}
