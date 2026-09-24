//go:build integration

package integration

import (
	"context"
	"testing"
	"time"
)

func duplicates(t *testing.T, inst *instance, source string) float64 {
	t.Helper()
	mfs, err := inst.Metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "wager_duplicates_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "source" && l.GetValue() == source {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func inboxRow(t *testing.T, messageID string) (outcome string, processed bool) {
	t.Helper()
	var o *string
	var at *time.Time
	err := adminPool.QueryRow(context.Background(), `SELECT outcome, processed_at FROM inbox_messages WHERE message_id = $1`, messageID).Scan(&o, &at)
	if err != nil {
		return "", false
	}
	if o != nil {
		outcome = *o
	}
	return outcome, at != nil
}

func TestSQSConsumerInboxAndCrossTransport(t *testing.T) {
	q := createQueues(t)
	inst := startApp(t, baseConfig(q))
	w := openWallet(t, inst.Base, "100.00")

	// 1) Message processed once; a redelivery of the same messageId (new SQS
	// dedup id, so the broker does not drop it) is answered by the inbox.
	op := w.op(uniq("sqs-bet"), "BET", "10.00", "")
	msgID := uniq("msg")
	sendMessage(t, q.IngressURL, msgID, op, uniq("dedup"))
	waitFor(t, 15*time.Second, "message processed", func() bool { st, _ := txStatus(t, "provider-a", op.External); return st == "PROCESSED" })
	sendMessage(t, q.IngressURL, msgID, op, uniq("dedup"))
	waitFor(t, 15*time.Second, "duplicate consumed", func() bool { return duplicates(t, inst, "sqs") >= 1 })
	if outcome, done := inboxRow(t, msgID); !done || outcome != "PROCESSED" {
		t.Fatalf("inbox row: %q %v", outcome, done)
	}
	// Same operation over HTTP afterwards: idempotent replay.
	if r := submit(t, inst.Base, op); r.Status != 200 || r.Body["idempotentReplay"] != true {
		t.Fatalf("http after sqs: %d %s", r.Status, r.Raw)
	}

	// 2) HTTP first, then SQS (different messageId): replay, message removed.
	op2 := w.op(uniq("http-first"), "BET", "5.00", "")
	if r := submit(t, inst.Base, op2); r.Status != 200 {
		t.Fatalf("http: %d %s", r.Status, r.Raw)
	}
	msg2 := uniq("msg")
	sendMessage(t, q.IngressURL, msg2, op2, uniq("dedup"))
	waitFor(t, 15*time.Second, "sqs replay handled", func() bool { _, done := inboxRow(t, msg2); return done })

	// 3) Business rejection from SQS is terminal: persisted and removed.
	poor := w.op(uniq("too-big"), "BET", "1000.00", "")
	sendMessage(t, q.IngressURL, uniq("msg"), poor, uniq("dedup"))
	waitFor(t, 15*time.Second, "rejection persisted", func() bool { st, _ := txStatus(t, "provider-a", poor.External); return st == "REJECTED" })

	waitFor(t, 15*time.Second, "ingress queue drained", func() bool { v, f := queueDepth(t, q.IngressURL); return v == 0 && f == 0 })
	if s := stateOf(t, w.ID); s.Debits != 2 || s.Balance != 8500 {
		t.Fatalf("state %+v", s)
	}
	if dlq := receiveAll(t, q.DLQURL, time.Second); len(dlq) != 0 {
		t.Fatalf("unexpected DLQ messages: %+v", dlq)
	}
	assertReconciled(t, inst.Base, w.ID)
}

// The same operation arrives simultaneously over HTTP (several times) and
// SQS (several deliveries): exactly one debit.
func TestSQSAndHTTPConcurrentSameOperation(t *testing.T) {
	q := createQueues(t)
	inst := startApp(t, baseConfig(q))
	w := openWallet(t, inst.Base, "100.00")
	op := w.op(uniq("cross"), "BET", "30.00", "")
	for range 5 {
		sendMessage(t, q.IngressURL, uniq("msg"), op, uniq("dedup"))
	}
	parallel(20, func(int) {
		r := submit(t, inst.Base, op)
		if r.Status != 200 || r.str("balance", "amount") != "70.00" {
			t.Errorf("http: %d %s", r.Status, r.Raw)
		}
	})
	waitFor(t, 20*time.Second, "ingress drained", func() bool { v, f := queueDepth(t, q.IngressURL); return v == 0 && f == 0 })
	if s := stateOf(t, w.ID); s.Debits != 1 || s.Balance != 7000 {
		t.Fatalf("state %+v", s)
	}
}

func TestSQSPoisonMessagesGoToDLQ(t *testing.T) {
	q := createQueues(t)
	inst := startApp(t, baseConfig(q))
	w := openWallet(t, inst.Base, "100.00")

	sendRaw(t, q.IngressURL, `{"not":"an envelope"`, "g1", uniq("d"))
	unauthorized := w.op(uniq("x"), "BET", "1.00", "")
	unauthorized.Provider = "provider-evil"
	sendMessage(t, q.IngressURL, uniq("msg"), unauthorized, uniq("d"))
	opening := w.op(uniq("x"), "OPENING", "1.00", "")
	sendMessage(t, q.IngressURL, uniq("msg"), opening, uniq("d"))
	// Same messageId, different payload -> hash mismatch.
	first := w.op(uniq("h1"), "BET", "1.00", "")
	sameID := uniq("msg")
	sendMessage(t, q.IngressURL, sameID, first, uniq("d"))
	waitFor(t, 15*time.Second, "first processed", func() bool { st, _ := txStatus(t, "provider-a", first.External); return st == "PROCESSED" })
	sendMessage(t, q.IngressURL, sameID, w.op(uniq("h2"), "BET", "2.00", ""), uniq("d"))

	reasons := map[string]int{}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && len(reasons) < 4 {
		for _, m := range receiveAll(t, q.DLQURL, time.Second) {
			reasons[m.Attributes["dlqReason"]]++
		}
	}
	for _, want := range []string{"invalid_message", "unauthorized_provider", "validation:OPENING_NOT_ALLOWED", "message_hash_mismatch"} {
		if reasons[want] != 1 {
			t.Errorf("DLQ reason %s: %d (all: %v)", want, reasons[want], reasons)
		}
	}
	if s := stateOf(t, w.ID); s.Debits != 1 {
		t.Fatalf("poison messages moved money: %+v", s)
	}
}

// A transient failure is retried with backoff; after SQS_MAX_RECEIVES the
// message goes to the DLQ. The failure is induced with a handler deadline
// shorter than any database round trip.
func TestSQSTransientFailureRetriesThenDLQ(t *testing.T) {
	q := createQueues(t)
	cfg := baseConfig(q)
	cfg.SQS.HandlerTimeout = time.Nanosecond
	cfg.SQS.MaxReceives = 2
	cfg.SQS.RetryBaseDelay = time.Second
	inst := startApp(t, cfg)
	w := openWallet(t, inst.Base, "100.00")
	op := w.op(uniq("transient"), "BET", "1.00", "")
	sendMessage(t, q.IngressURL, uniq("msg"), op, uniq("d"))

	var dlq []sqsMessage
	waitFor(t, 30*time.Second, "message in DLQ", func() bool {
		dlq = append(dlq, receiveAll(t, q.DLQURL, time.Second)...)
		return len(dlq) > 0
	})
	if dlq[0].Attributes["dlqReason"] != "retries_exhausted" || dlq[0].Attributes["receiveCount"] != "2" {
		t.Fatalf("dlq attributes %v", dlq[0].Attributes)
	}
	if st, _ := txStatus(t, "provider-a", op.External); st != "" {
		t.Fatalf("failed attempts must not persist anything, got %s", st)
	}
	mfs, _ := inst.Metrics.Registry.Gather()
	found := false
	for _, mf := range mfs {
		if mf.GetName() == "sqs_message_retries_total" && mf.GetMetric()[0].GetCounter().GetValue() >= 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("retry metric not incremented")
	}
}

// Scenario 5: the consumer process dies after the commit and before
// deleting the message; the redelivery is deduplicated by the inbox.
func TestConsumerCrashAfterCommitBeforeDelete(t *testing.T) {
	q := createQueues(t)
	api := startApp(t, httpConfig(t))
	w := openWallet(t, api.Base, "100.00")
	op := w.op(uniq("crash"), "BET", "40.00", "")
	msgID := uniq("msg")

	crashCfg := baseConfig(q)
	crashCfg.FaultInjection = "consumer-crash-after-commit"
	crashCfg.Outbox.Enabled, crashCfg.Pending.Enabled = false, false
	crashing := startProcess(t, crashCfg)
	sendMessage(t, q.IngressURL, msgID, op, uniq("d"))
	if code := crashing.WaitExit(t, 20*time.Second); code != 137 {
		t.Fatalf("expected fault-injected exit 137, got %d", code)
	}
	if st, _ := txStatus(t, "provider-a", op.External); st != "PROCESSED" {
		t.Fatalf("commit must have happened before the crash, status %q", st)
	}
	if _, inflight := queueDepth(t, q.IngressURL); inflight != 1 {
		t.Fatalf("message must still be in the queue (in flight), got %d", inflight)
	}

	// Another instance receives the redelivery after the visibility timeout.
	survivor := startProcess(t, baseConfig(q))
	waitFor(t, 30*time.Second, "redelivered message deleted", func() bool { v, f := queueDepth(t, q.IngressURL); return v == 0 && f == 0 })
	if s := stateOf(t, w.ID); s.Debits != 1 || s.Balance != 6000 {
		t.Fatalf("state after redelivery %+v", s)
	}
	_ = survivor // keeps running until cleanup
	assertReconciled(t, api.Base, w.ID)
}
