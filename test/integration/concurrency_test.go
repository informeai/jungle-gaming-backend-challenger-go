//go:build integration

package integration

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// Scenario 1: the same bet 50 times in parallel -> exactly one debit.
func TestSameBetFiftyTimesInParallel(t *testing.T) {
	inst := startApp(t, httpConfig(t))
	w := openWallet(t, inst.Base, "100.00")
	op := w.op(uniq("dup-bet"), "BET", "25.00", "")
	var fresh, replays, other atomic.Int32
	ids := make(chan string, 50)
	parallel(50, func(int) {
		r := submit(t, inst.Base, op)
		switch {
		case r.Status == http.StatusOK && r.Body["idempotentReplay"] == false:
			fresh.Add(1)
		case r.Status == http.StatusOK && r.Body["idempotentReplay"] == true:
			replays.Add(1)
		default:
			other.Add(1)
			t.Errorf("unexpected: %d %s", r.Status, r.Raw)
		}
		if r.str("balance", "amount") != "75.00" {
			t.Errorf("balance returned %s", r.Raw)
		}
		ids <- r.str("transactionId")
	})
	close(ids)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		} else if id != first {
			t.Fatalf("different transaction ids returned: %s vs %s", first, id)
		}
	}
	if fresh.Load() != 1 || replays.Load() != 49 {
		t.Fatalf("fresh=%d replays=%d other=%d", fresh.Load(), replays.Load(), other.Load())
	}
	if s := stateOf(t, w.ID); s.Debits != 1 || s.Balance != 7500 || s.Version != 2 {
		t.Fatalf("state %+v", s)
	}
	assertReconciled(t, inst.Base, w.ID)
}

// Scenario 2: 100.00 and two distinct concurrent bets of 80.00.
func TestTwoConcurrentBetsOnSameWallet(t *testing.T) {
	inst := startApp(t, httpConfig(t))
	runTwoBetsRace(t, []string{inst.Base, inst.Base}, inst.Base)
}

func runTwoBetsRace(t *testing.T, bases []string, internalBase string) {
	t.Helper()
	w := openWallet(t, internalBase, "100.00")
	ops := []wagerOp{w.op(uniq("race-a"), "BET", "80.00", ""), w.op(uniq("race-b"), "BET", "80.00", "")}
	results := make([]response, 2)
	parallel(2, func(i int) { results[i] = submit(t, bases[i%len(bases)], ops[i]) })

	processed, rejected := -1, -1
	for i, r := range results {
		switch {
		case r.Status == http.StatusOK && r.str("status") == "PROCESSED":
			processed = i
		case r.Status == http.StatusUnprocessableEntity && r.str("status") == "REJECTED" && r.str("failureCode") == "INSUFFICIENT_FUNDS":
			rejected = i
		}
	}
	if processed < 0 || rejected < 0 {
		t.Fatalf("want one PROCESSED and one INSUFFICIENT_FUNDS: %s | %s", results[0].Raw, results[1].Raw)
	}
	if results[processed].str("balance", "amount") != "20.00" {
		t.Fatalf("processed balance %s", results[processed].Raw)
	}
	check := func() {
		s := stateOf(t, w.ID)
		if s.Balance != 2000 || s.Debits != 1 {
			t.Fatalf("state %+v", s)
		}
	}
	check()
	// Resending does not change the outcome (each replays its own result).
	for round := 0; round < 3; round++ {
		parallel(2, func(i int) {
			r := submit(t, bases[(i+round)%len(bases)], ops[i])
			if r.Body["idempotentReplay"] != true || r.str("status") != results[i].str("status") || r.str("transactionId") != results[i].str("transactionId") {
				t.Errorf("resend %d: %s", i, r.Raw)
			}
		})
	}
	check()
	assertReconciled(t, internalBase, w.ID)
}

// Scenario 3: distinct wallets are processed simultaneously and in parallel.
func TestDistinctWalletsInParallel(t *testing.T) {
	inst := startApp(t, httpConfig(t))
	const wallets, betsPerWallet = 20, 10
	ws := make([]walletRef, wallets)
	for i := range ws {
		ws[i] = openWallet(t, inst.Base, "100.00")
	}
	start := time.Now()
	parallel(wallets*betsPerWallet, func(i int) {
		w := ws[i%wallets]
		r := submit(t, inst.Base, w.op(uniqf("pw", i%wallets, i), "BET", "1.50", ""))
		if r.Status != http.StatusOK {
			t.Errorf("bet: %d %s", r.Status, r.Raw)
		}
	})
	t.Logf("%d bets over %d wallets in %s", wallets*betsPerWallet, wallets, time.Since(start))
	for _, w := range ws {
		if s := stateOf(t, w.ID); s.Balance != 10000-betsPerWallet*150 || s.Debits != betsPerWallet || s.Version != 1+betsPerWallet {
			t.Fatalf("wallet %s: %+v", w.ID, s)
		}
	}
}

// Wallets of different players do not wait for each other's locks: a wallet
// held by a long transaction does not block another wallet.
func TestNoGlobalLock(t *testing.T) {
	inst := startApp(t, httpConfig(t))
	busy, free := openWallet(t, inst.Base, "100.00"), openWallet(t, inst.Base, "100.00")
	ctx := t.Context()
	tx, err := adminPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, busy.ID); err != nil {
		t.Fatal(err)
	}
	done := make(chan response, 1)
	go func() { done <- submit(t, inst.Base, busy.op(uniq("blocked"), "BET", "1.00", "")) }()

	startFree := time.Now()
	if r := submit(t, inst.Base, free.op(uniq("free"), "BET", "1.00", "")); r.Status != http.StatusOK {
		t.Fatalf("free wallet: %d %s", r.Status, r.Raw)
	}
	if d := time.Since(startFree); d > 2*time.Second {
		t.Fatalf("independent wallet waited %s", d)
	}
	select {
	case r := <-done:
		t.Fatalf("locked wallet should still be waiting, got %d", r.Status)
	case <-time.After(300 * time.Millisecond):
	}
	_ = tx.Rollback(ctx)
	if r := <-done; r.Status != http.StatusOK {
		t.Fatalf("blocked wallet after release: %d %s", r.Status, r.Raw)
	}
}

func TestHTTPContract(t *testing.T) {
	inst := startApp(t, httpConfig(t))
	w := openWallet(t, inst.Base, "100.00")
	prov := token(t, "provider-a")
	post := func(body any, key string) response {
		h := map[string]string{}
		if key != "" {
			h["Idempotency-Key"] = key
		}
		return do(t, http.MethodPost, inst.Base+"/wagering/transactions", prov, body, h)
	}
	op := w.op(uniq("c"), "BET", "10.00", "")

	cases := []struct {
		name   string
		resp   response
		status int
		code   string
	}{
		{"missing key", post(op.payload(), ""), 400, "IDEMPOTENCY_KEY_REQUIRED"},
		{"malformed json", post(`{"providerId":`, "k1"), 400, "MALFORMED_REQUEST"},
		{"number amount", post(`{"providerId":"provider-a","externalTransactionId":"n1","playerId":"`+w.Player+`","walletId":"`+w.ID+`","roundId":"r","gameId":"g","kind":"BET","money":{"amount":25.00,"currency":"BRL"}}`, "k2"), 400, "MALFORMED_REQUEST"},
		{"unknown field", post(`{"providerId":"provider-a","extra":1}`, "k3"), 400, "MALFORMED_REQUEST"},
		{"scientific amount", post(w.op("s1", "BET", "1e2", "").payload(), "k4"), 400, "INVALID_MONEY"},
		{"excess scale", post(w.op("s2", "BET", "1.001", "").payload(), "k5"), 400, "INVALID_MONEY"},
		{"negative", post(w.op("s3", "BET", "-1.00", "").payload(), "k6"), 400, "INVALID_MONEY"},
		{"opening kind", post(w.op("s4", "OPENING", "1.00", "").payload(), "k7"), 400, "OPENING_NOT_ALLOWED"},
		{"loss non zero", post(w.op("s5", "LOSS", "1.00", "").payload(), "k8"), 400, "LOSS_AMOUNT_MUST_BE_ZERO"},
		{"zero bet", post(w.op("s6", "BET", "0.00", "").payload(), "k9"), 400, "AMOUNT_MUST_BE_POSITIVE"},
		{"refund without ref", post(w.op("s7", "REFUND", "1.00", "").payload(), "k10"), 400, "REFERENCE_REQUIRED"},
		{"unknown wallet", post(wagerOp{Provider: "provider-a", External: "s8", Player: w.Player, Wallet: "0192f291-27dd-7d3f-8071-5f8685deef37", Round: "r", Kind: "BET", Amount: "1.00"}.payload(), "k11"), 404, "WALLET_NOT_FOUND"},
	}
	for _, c := range cases {
		if c.resp.Status != c.status || c.resp.str("error", "code") != c.code {
			t.Errorf("%s: %d %s (want %d %s)", c.name, c.resp.Status, c.resp.Raw, c.status, c.code)
		}
	}
	if r := post(op.payload(), op.key()); r.Status != 200 {
		t.Fatalf("bet %d %s", r.Status, r.Raw)
	}
	changed := op
	changed.Amount = "11.00"
	if r := post(changed.payload(), op.key()); r.Status != 409 || r.str("error", "code") != "IDEMPOTENCY_KEY_CONFLICT" {
		t.Errorf("key conflict: %d %s", r.Status, r.Raw)
	}
	if r := post(op.payload(), "another-key"); r.Status != 409 || r.str("error", "code") != "EXTERNAL_TRANSACTION_CONFLICT" {
		t.Errorf("external id with another key: %d %s", r.Status, r.Raw)
	}
	// Wallet conflict and reads.
	internal := token(t, "wallet-internal")
	if r := do(t, http.MethodPost, inst.Base+"/wallets", internal, map[string]any{"playerId": w.Player,
		"initialBalance": map[string]string{"amount": "5.00", "currency": "BRL"}}, nil); r.Status != 409 {
		t.Errorf("duplicate wallet: %d", r.Status)
	}
	zero := do(t, http.MethodPost, inst.Base+"/wallets", internal, map[string]any{"playerId": uniqUUID(),
		"initialBalance": map[string]string{"amount": "0.00", "currency": "BRL"}}, nil)
	if zero.Status != 201 || zero.Body["version"] != float64(1) {
		t.Fatalf("zero wallet: %d %s", zero.Status, zero.Raw)
	}
	if s := stateOf(t, zero.str("id")); s.LedgerEntries != 0 {
		t.Fatalf("zero opening created ledger: %+v", s)
	}
	var openings int
	_ = adminPool.QueryRow(t.Context(), `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1`, zero.str("id")).Scan(&openings)
	if openings != 0 {
		t.Fatal("zero opening created an OPENING transaction")
	}
	var events int
	_ = adminPool.QueryRow(t.Context(), `SELECT count(*) FROM outbox_events WHERE partition_key = $1`, w.ID).Scan(&events)
	if events < 4 { // opening: processed + balance changed; bet: processed + balance changed
		t.Fatalf("outbox events for wallet: %d", events)
	}

	// Ledger pagination with an opaque cursor and stable order.
	for i := range 5 {
		submit(t, inst.Base, w.op(fmt.Sprintf("%s-p%d", op.External, i), "BET", "1.00", ""))
	}
	seen := map[string]bool{}
	cursor, pages := "", 0
	for {
		r := do(t, http.MethodGet, inst.Base+"/wallets/"+w.ID+"/ledger?limit=2&cursor="+cursor, internal, nil, nil)
		if r.Status != 200 {
			t.Fatalf("ledger: %d %s", r.Status, r.Raw)
		}
		for _, it := range r.Body["items"].([]any) {
			id := it.(map[string]any)["id"].(string)
			if seen[id] {
				t.Fatalf("entry %s repeated across pages", id)
			}
			seen[id] = true
		}
		pages++
		next, _ := r.Body["nextCursor"].(string)
		if next == "" {
			break
		}
		cursor = next
	}
	if len(seen) != 7 || pages != 4 {
		t.Fatalf("ledger pages=%d entries=%d", pages, len(seen))
	}
	if r := do(t, http.MethodGet, inst.Base+"/wallets/"+w.ID+"/ledger?cursor=garbage", internal, nil, nil); r.Status != 400 {
		t.Errorf("invalid cursor: %d", r.Status)
	}
	// Health.
	if r := do(t, http.MethodGet, inst.Base+"/health/live", "", nil, nil); r.Status != 200 {
		t.Errorf("live %d", r.Status)
	}
	// Readiness covers the dependencies of the enabled components: postgres for
	// the API, postgres-outbox and sqs-events for the relay.
	if r := do(t, http.MethodGet, inst.Base+"/health/ready", "", nil, nil); r.Status != 200 || r.str("dependencies", "postgres") != "UP" || r.str("dependencies", "sqs-events") != "UP" {
		t.Errorf("ready %d %s", r.Status, r.Raw)
	}
}
