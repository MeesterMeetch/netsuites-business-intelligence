#!/usr/bin/env bash
set -euo pipefail

STORE="${1:-}"
RESET="${2:-false}"
DAYS="${3:-365}"

if [[ -z "$STORE" ]]; then
  echo "Usage: backfill_one.sh <store> [reset=false] [days=365]" >&2
  exit 1
fi

TOKEN_OPT=""
if [[ -n "${BACKFILL_TOKEN:-}" ]]; then
  TOKEN_OPT="&token=$BACKFILL_TOKEN"
fi

for a in 1 2 3; do
  echo "Backfill $STORE days=$DAYS reset=$RESET (attempt $a)"
  R=$(curl -sS -i -X POST \
     "https://netsuite-bi-ingest.mitchbiworker.workers.dev/api/admin/backfill?days=$DAYS&store=$STORE&hard_reset=$RESET$TOKEN_OPT")

  STATUS=$(printf "%s" "$R" | sed -n '1s/^[^ ]* \([0-9][0-9][0-9]\).*/\1/p')
  BODY=$(printf "%s" "$R" | sed '1,/^\r\?$/d')

  if [[ "$STATUS" == "200" ]] && echo "$BODY" | jq . >/dev/null 2>&1; then
    echo "$BODY" | jq .
    exit 0
  else
    echo "HTTP $STATUS"
    echo "$BODY"
    sleep 3
  fi
done

echo "Backfill failed after retries" >&2
exit 1
