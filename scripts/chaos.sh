#!/usr/bin/env bash
# Outage simulation against the compose stack (make up / make observability).
#
#   scripts/chaos.sh db  [seconds]   # freeze PostgreSQL
#   scripts/chaos.sh sqs [seconds]   # freeze LocalStack (SQS)
#
# `docker compose pause` freezes the container: TCP connections are accepted
# but never answered - the worst kind of outage, where callers only find out by
# timing out. The script shows the circuit breakers opening (fail-fast 503,
# consumers and relays paused, no valid message sent to the DLQ) and the
# automatic recovery after `unpause`. Requires curl and python3.
set -euo pipefail
cd "$(dirname "$0")/.."

TARGET="${1:-}"
DURATION="${2:-45}"
API="${API:-http://localhost:8081}"
QUEUE_BASE="http://localhost:4566/000000000000"
INGRESS="$QUEUE_BASE/wager-transactions.fifo"
DLQ="$QUEUE_BASE/wager-transactions-dlq.fifo"

case "$TARGET" in
  db) SERVICE=postgres ;;
  sqs) SERVICE=localstack ;;
  *) echo "usage: $0 db|sqs [seconds]" >&2; exit 2 ;;
esac

paused=false
restore() { if $paused; then docker compose unpause "$SERVICE" >/dev/null 2>&1 || true; fi; }
trap restore EXIT

json() { python3 -c "import sys, json; d = json.load(sys.stdin); print(eval(sys.argv[1], {}, {'d': d}))" "$1"; }
sqs() { docker compose exec -T -e AWS_ACCESS_KEY_ID=provider-gateway -e AWS_SECRET_ACCESS_KEY=provider-gateway-local localstack awslocal sqs "$@"; }
state_name() { case "${1%%.*}" in 0) echo closed ;; 1) echo half-open ;; 2) echo OPEN ;; *) echo "?" ;; esac; }

# breakers <service>: circuit_breaker_state of every breaker in that container.
breakers() {
  docker compose exec -T "$1" wget -qO- localhost:8080/metrics 2>/dev/null |
    awk '/^circuit_breaker_state\{/ { match($0, /name="[^"]*"/); n = substr($0, RSTART + 6, RLENGTH - 7); printf "%s=%s ", n, $2 }'
}
# wait_closed <services...>: until every breaker is closed (max 30s).
wait_closed() {
  for _ in $(seq 1 30); do
    local all="" svc
    for svc in "$@"; do all+=$(breakers "$svc"); done
    echo "$all" | grep -Eq '=(1|2)' || return 0
    sleep 1
  done
}
show_breakers() {
  local line="" svc out n v
  for svc in "$@"; do
    out=$(breakers "$svc")
    line+="$svc["
    for kv in $out; do n=${kv%%=*}; v=${kv#*=}; line+="$n:$(state_name "$v") "; done
    line="${line% }] "
  done
  echo "   breakers: $line"
}

open_wallet() {
  curl -sf -X POST "$API/wallets" -H "Authorization: Bearer $INTERNAL" -H 'Content-Type: application/json' \
    -d "{\"playerId\":\"$(python3 -c 'import uuid; print(uuid.uuid4())')\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}" |
    json '"%s %s" % (d["id"], d["playerId"])'
}

bet_body() { # wallet player externalId
  echo "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$3\",\"playerId\":\"$2\",\"walletId\":\"$1\",\"roundId\":\"chaos\",\"gameId\":\"chaos\",\"kind\":\"BET\",\"money\":{\"amount\":\"1.00\",\"currency\":\"BRL\"}}"
}

# bet <wallet> <player> <externalId>: prints "<http code> <seconds> <Retry-After>"
bet() {
  local hdr
  hdr=$(mktemp)
  curl -s -o /dev/null -D "$hdr" --max-time 20 -w '%{http_code} %{time_total}' -X POST "$API/wagering/transactions" \
    -H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' -H "Idempotency-Key: provider-a:$3" \
    -d "$(bet_body "$1" "$2" "$3")" || true
  printf ' %s\n' "$(awk 'tolower($1) == "retry-after:" { gsub("\r", ""); print $2 }' "$hdr")"
  rm -f "$hdr"
}

send_msg() { # wallet player externalId
  local body
  body="{\"messageId\":\"$3\",\"type\":\"WagerTransactionRequested\",\"occurredAt\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",\"data\":$(bet_body "$1" "$2" "$3" | python3 -c "import sys, json; d = json.load(sys.stdin); d['idempotencyKey'] = 'provider-a:$3'; print(json.dumps(d))")}"
  sqs send-message --queue-url "$INGRESS" --message-group-id "$1" --message-deduplication-id "$3" --message-body "$body" >/dev/null
}

tx_status() { # externalId
  curl -s "$API/providers/provider-a/wagering/transactions/$1" -H "Authorization: Bearer $PROVIDER" | json 'd.get("status", "-")' 2>/dev/null || echo "-"
}

dlq_has() { # marker
  sqs receive-message --queue-url "$DLQ" --max-number-of-messages 10 --visibility-timeout 0 --wait-time-seconds 1 --output json 2>/dev/null |
    MARK="$1" python3 -c '
import json, os, sys
raw = sys.stdin.read().strip()
msgs = (json.loads(raw) if raw else {}).get("Messages", [])
print(sum(os.environ["MARK"] in m["Body"] for m in msgs))'
}

INTERNAL=$(scripts/token.sh wallet-internal)
PROVIDER=$(scripts/token.sh provider-a)
RUN="chaos-$(date +%s)"

# ------------------------------------------------------------------ database
if [ "$TARGET" = db ]; then
  echo "== preparing: 5 wallets (one SQS message group each)"
  WALLETS=()
  for _ in 1 2 3 4 5; do WALLETS+=("$(open_wallet)"); done
  read -r W P <<<"${WALLETS[0]}"
  show_breakers api-1 consumer-1 pending-worker-1

  echo "== freezing PostgreSQL for ${DURATION}s (docker compose pause postgres)"
  docker compose pause postgres >/dev/null
  paused=true
  START=$(date +%s)

  echo "== 8 concurrent bets on api-1: they wait for the request timeout, then the breaker opens"
  for i in 1 2 3 4 5 6 7 8; do bet "$W" "$P" "$RUN-slow-$i" >"/tmp/$RUN-slow-$i" & done
  echo "== meanwhile, one bet per wallet through SQS"
  for i in 0 1 2 3 4; do read -r wi pi <<<"${WALLETS[$i]}"; send_msg "$wi" "$pi" "$RUN-sqs-$i"; done
  wait
  for i in 1 2 3 4 5 6 7 8; do read -r code secs _ <"/tmp/$RUN-slow-$i"; printf '   slow bet %d: HTTP %s in %ss\n' "$i" "$code" "$secs"; rm -f "/tmp/$RUN-slow-$i"; done
  show_breakers api-1

  echo "== with the breaker open: fail fast, nothing sent to the frozen database"
  for i in 1 2 3 4 5; do
    read -r code secs retry <<<"$(bet "$W" "$P" "$RUN-fast-$i")"
    note=""
    python3 -c "import sys; sys.exit(0 if float('$secs') > 1 else 1)" && note="  <- half-open probe: this one request tests the database"
    printf '   bet %d: HTTP %s in %ss (Retry-After: %s)%s\n' "$i" "$code" "$secs" "${retry:--}" "$note"
  done
  echo "   readiness api-1: $(curl -s "$API/health/ready")"

  now=$(date +%s); remaining=$(( DURATION - (now - START) ))
  if [ "$remaining" -gt 0 ]; then
    echo "== outage continues for ${remaining}s more (consumers paused: messages stay in the queue)"
    sleep "$remaining"
  fi
  show_breakers api-1 consumer-1 consumer-2 pending-worker-1
  echo "   consumer logs: $(docker compose logs --since "${DURATION}s" consumer-1 consumer-2 2>/dev/null | grep -c 'consumer paused') pause event(s)"

  echo "== unfreezing PostgreSQL"
  docker compose unpause postgres >/dev/null
  paused=false
  T0=$(date +%s)
  until [ "$(bet "$W" "$P" "$RUN-recovered" | cut -d' ' -f1)" = 200 ]; do sleep 1; done
  echo "   API recovered in $(( $(date +%s) - T0 ))s (breaker half-open probe succeeded)"
  for i in 0 1 2 3 4; do
    for _ in $(seq 1 60); do [ "$(tx_status "$RUN-sqs-$i")" = PROCESSED ] && break; sleep 1; done
    echo "   SQS bet $i: $(tx_status "$RUN-sqs-$i")"
  done
  echo "   valid messages in the DLQ from this run: $(dlq_has "$RUN")"
  wait_closed api-1 consumer-1 consumer-2 pending-worker-1 pending-worker-2
  show_breakers api-1 consumer-1 consumer-2 pending-worker-1 pending-worker-2
  echo "== reconciliation of the first wallet"
  curl -s -X POST "$API/wallets/$W/reconciliation" -H "Authorization: Bearer $INTERNAL" |
    json '"   consistent=%s stored=%s ledger=%s" % (d["consistent"], d["storedBalance"]["amount"], d["calculatedBalance"]["amount"])'
  exit 0
fi

# ----------------------------------------------------------------------- sqs
echo "== freezing LocalStack (SQS) for ${DURATION}s (docker compose pause localstack)"
docker compose pause localstack >/dev/null
paused=true
START=$(date +%s)
echo "== the HTTP API does not depend on SQS: operations keep working"
read -r W P <<<"$(open_wallet)"
for i in 1 2 3; do
  read -r code secs _ <<<"$(bet "$W" "$P" "$RUN-bet-$i")"
  printf '   bet %d: HTTP %s in %ss\n' "$i" "$code" "$secs"
done
unpublished() {
  docker compose exec -T postgres psql -U postgres -d wallet -tAc \
    "SELECT count(*) FILTER (WHERE published_at IS NULL) || ' unpublished, max attempts ' || COALESCE(max(attempts), 0) FROM outbox_events WHERE partition_key = '$W'"
}
echo "== events of this wallet are committed in the outbox and wait for SQS"
now=$(date +%s); remaining=$(( DURATION - (now - START) ))
[ "$remaining" -gt 0 ] && sleep "$remaining"
echo "   outbox: $(unpublished)"
show_breakers outbox-relay-1 outbox-relay-2 consumer-1

echo "== unfreezing LocalStack"
docker compose unpause localstack >/dev/null
paused=false
T0=$(date +%s)
until [ "$(unpublished | cut -d' ' -f1)" = 0 ]; do sleep 1; [ $(( $(date +%s) - T0 )) -gt 90 ] && break; done
echo "   outbox after recovery ($(( $(date +%s) - T0 ))s): $(unpublished)"
wait_closed outbox-relay-1 outbox-relay-2 consumer-1 consumer-2
show_breakers outbox-relay-1 outbox-relay-2 consumer-1 consumer-2
