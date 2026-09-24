#!/usr/bin/env bash
# Usage: scripts/token.sh <client-id> [client-secret]
# Prints an access token obtained with client_credentials from Keycloak.
set -euo pipefail
CLIENT="${1:?client id}"
SECRET="${2:-${CLIENT}-secret}"
KEYCLOAK_URL="${KEYCLOAK_URL:-http://localhost:8180}"
curl -sf -d grant_type=client_credentials -d client_id="$CLIENT" -d client_secret="$SECRET" \
  "$KEYCLOAK_URL/realms/jungle/protocol/openid-connect/token" |
  python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])'
