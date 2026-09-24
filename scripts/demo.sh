#!/usr/bin/env bash
# End-to-end demo against the compose stack: opens a wallet, bets, replays,
# wins, refunds before/after the bet, reconciles. Requires curl + python3.
set -euo pipefail
API="${API:-http://localhost:8081}"
INTERNAL=$(scripts/token.sh wallet-internal)
PROVIDER=$(scripts/token.sh provider-a)
PLAYER=$(python3 -c 'import uuid; print(uuid.uuid4())')
RUN=$(date +%s)

call() { # method path token [body] [idempotency-key]
  local args=(-s -w '  -> HTTP %{http_code}\n' -X "$1" "$API$2" -H "Authorization: Bearer $3" -H 'Content-Type: application/json')
  [ -n "${4:-}" ] && args+=(-d "$4")
  [ -n "${5:-}" ] && args+=(-H "Idempotency-Key: $5")
  curl "${args[@]}"
}

echo "== open wallet (internal client)"
WALLET_JSON=$(curl -s -X POST "$API/wallets" -H "Authorization: Bearer $INTERNAL" -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"1000.00\",\"currency\":\"BRL\"}}")
echo "$WALLET_JSON"
WALLET=$(echo "$WALLET_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])')

op() { # externalId kind amount [reference]
  local ref=""
  [ -n "${4:-}" ] && ref=",\"referenceExternalTransactionId\":\"$4\""
  echo "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$1\",\"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"round-$RUN\",\"gameId\":\"fortune-chimp\",\"kind\":\"$2\",\"money\":{\"amount\":\"$3\",\"currency\":\"BRL\"}$ref}"
}

echo "== BET 25.00"
call POST /wagering/transactions "$PROVIDER" "$(op bet-$RUN BET 25.00)" "provider-a:bet-$RUN"
echo "== same BET again (idempotent replay)"
call POST /wagering/transactions "$PROVIDER" "$(op bet-$RUN BET 25.00)" "provider-a:bet-$RUN"
echo "== same key, different payload (conflict)"
call POST /wagering/transactions "$PROVIDER" "$(op bet-$RUN BET 30.00)" "provider-a:bet-$RUN"
echo "== REFUND before its BET exists (pending reference)"
call POST /wagering/transactions "$PROVIDER" "$(op refund-$RUN REFUND 10.00 bet2-$RUN)" "provider-a:refund-$RUN"
echo "== BET2 10.00 arrives; worker resolves the refund"
call POST /wagering/transactions "$PROVIDER" "$(op bet2-$RUN BET 10.00)" "provider-a:bet2-$RUN"
sleep 2
call GET "/providers/provider-a/wagering/transactions/refund-$RUN" "$PROVIDER"
echo "== WIN 50.00 referencing BET"
call POST /wagering/transactions "$PROVIDER" "$(op win-$RUN WIN 50.00 bet-$RUN)" "provider-a:win-$RUN"
echo "== LOSS 0.00"
call POST /wagering/transactions "$PROVIDER" "$(op loss-$RUN LOSS 0.00)" "provider-a:loss-$RUN"
echo "== ledger"
call GET "/wallets/$WALLET/ledger?limit=10" "$INTERNAL"
echo "== reconciliation"
call POST "/wallets/$WALLET/reconciliation" "$INTERNAL"
