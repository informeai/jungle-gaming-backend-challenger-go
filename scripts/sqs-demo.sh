#!/usr/bin/env bash
# End-to-end demo of the SQS ingress against the compose stack:
#   1. opens a wallet (HTTP, internal client)
#   2. sends a BET through wager-transactions.fifo and waits for the consumer
#   3. redelivers the same message (same messageId): deduplicated by the inbox
#   4. sends the same operation over HTTP: idempotent replay, no new debit
#   5. sends an invalid message (OPENING): moved to the DLQ with its reason
#   6. shows the events the outbox relay published for the wallet
# Requires: docker compose stack up (make up), curl, python3.
set -euo pipefail
cd "$(dirname "$0")/.."

API="${API:-http://localhost:8081}"
QUEUE_BASE="http://localhost:4566/000000000000"
INGRESS="$QUEUE_BASE/wager-transactions.fifo"
DLQ="$QUEUE_BASE/wager-transactions-dlq.fifo"

json() { python3 -c "import sys, json; d = json.load(sys.stdin); print(eval(sys.argv[1], {}, {'d': d}))" "$1"; }

# Publishes as the producer identity (provider-gateway).
sqs() { docker compose exec -T -e AWS_ACCESS_KEY_ID=provider-gateway -e AWS_SECRET_ACCESS_KEY=provider-gateway-local localstack awslocal sqs "$@"; }

send() { # messageId dedupId kind amount
  local body
  body=$(cat <<EOF
{"messageId":"$1","type":"WagerTransactionRequested","occurredAt":"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)",
 "data":{"providerId":"provider-a","externalTransactionId":"$EXT","idempotencyKey":"provider-a:$EXT",
 "playerId":"$PLAYER","walletId":"$WALLET","roundId":"round-sqs","gameId":"fortune-chimp",
 "kind":"$3","money":{"amount":"$4","currency":"BRL"}}}
EOF
)
  sqs send-message --queue-url "$INGRESS" --message-group-id "$WALLET" \
    --message-deduplication-id "$2" --message-body "$body" >/dev/null
}

balance() { curl -sf "$API/wallets/$WALLET" -H "Authorization: Bearer $INTERNAL" | json 'd["balance"]["amount"]'; }

# Sum of sqs_messages_total{outcome=...} over both consumers (internal ports).
consumed() {
  local total=0 n
  for c in consumer-1 consumer-2; do
    n=$(docker compose exec -T "$c" wget -qO- localhost:8080/metrics 2>/dev/null |
      awk -v o="outcome=\"$1\"" 'index($0, "sqs_messages_total{") == 1 && index($0, o) { s += $2 } END { printf "%d", s }')
    total=$((total + ${n:-0}))
  done
  echo "$total"
}

wait_for() { # description command...
  local what=$1; shift
  for _ in $(seq 1 60); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  echo "timeout waiting for $what" >&2
  exit 1
}

INTERNAL=$(scripts/token.sh wallet-internal)
PROVIDER=$(scripts/token.sh provider-a)
PLAYER=$(python3 -c 'import uuid; print(uuid.uuid4())')
RUN=$(date +%s)
EXT="sqs-bet-$RUN"
MSG="msg-$RUN"

echo "== 1. open wallet with 100.00 (HTTP, internal client)"
WALLET=$(curl -sf -X POST "$API/wallets" -H "Authorization: Bearer $INTERNAL" -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}" | json 'd["id"]')
echo "   wallet $WALLET, balance $(balance)"

echo "== 2. BET 25.00 through the queue (messageId $MSG)"
send "$MSG" "$MSG" BET 25.00
tx_processed() {
  curl -sf "$API/providers/provider-a/wagering/transactions/$EXT" -H "Authorization: Bearer $PROVIDER" |
    json 'd["status"]' | grep -q PROCESSED
}
wait_for "consumer to process the message" tx_processed
echo "   status PROCESSED, balance $(balance)"

echo "== 3. redeliver the same message (same messageId, new SQS dedup id so the broker does not drop it)"
before=$(consumed duplicate)
send "$MSG" "$MSG-redelivery" BET 25.00
dedup_seen() { [ "$(consumed duplicate)" -gt "$before" ]; }
wait_for "inbox deduplication" dedup_seen
echo "   inbox detected the duplicate (sqs_messages_total{outcome=\"duplicate\"} $before -> $(consumed duplicate)), balance $(balance)"

echo "== 4. same operation over HTTP (Idempotency-Key provider-a:$EXT)"
curl -s -X POST "$API/wagering/transactions" -H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: provider-a:$EXT" \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$EXT\",\"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"round-sqs\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}" |
  json '"   status %s, idempotentReplay %s, balance %s" % (d["status"], str(d["idempotentReplay"]).lower(), d["balance"]["amount"])'

echo "== 5. invalid message (kind OPENING) -> DLQ"
BAD="msg-invalid-$RUN"
EXT="sqs-opening-$RUN"
send "$BAD" "$BAD" OPENING 10.00
dlq_reason() {
  sqs receive-message --queue-url "$DLQ" --max-number-of-messages 10 --wait-time-seconds 1 \
    --message-attribute-names All --visibility-timeout 0 --output json |
    BAD="$BAD" python3 -c '
import json, os, sys
raw = sys.stdin.read().strip()
for m in (json.loads(raw) if raw else {}).get("Messages", []):
    if os.environ["BAD"] in m["Body"]:
        print(m["MessageAttributes"]["dlqReason"]["StringValue"]); sys.exit(0)
sys.exit(1)'
}
wait_for "message in the DLQ" dlq_reason
echo "   found in wager-transactions-dlq.fifo with dlqReason=$(dlq_reason)"

echo "== 6. events published by the outbox relay for this wallet"
sleep 1
docker compose exec -T postgres psql -U postgres -d wallet -c \
  "SELECT event_type, id AS event_id, published_at IS NOT NULL AS published, attempts
     FROM outbox_events WHERE partition_key = '$WALLET' ORDER BY occurred_at, id"

echo "== final balance $(balance) (one single debit of 25.00)"
