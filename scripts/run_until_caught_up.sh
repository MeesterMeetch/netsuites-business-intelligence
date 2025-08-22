#!/usr/bin/env bash
set -euo pipefail

BASE="https://netsuite-bi-ingest.mitchbiworker.workers.dev/ingest/shopify/run"
STORES=(
  "essential-electric-supply.myshopify.com"
  "cbguys.myshopify.com"
  # "united-fuses.myshopify.com"  # already caught up
)

run_store() {
  local store="$1"
  local round=0
  while :; do
    round=$((round+1))
    echo "→ ${store} (round ${round})"
    out="$(curl -s -X POST "${BASE}?store=${store}")"
    echo "$out"
    ingested="$(echo "$out" | jq -r ".summary[\"${store}\"].ordersIngested // 0")"
    # fallback if jq didn’t parse (e.g., 1102): sleep and retry small
    if [[ -z "${ingested}" || "${ingested}" == "null" ]]; then
      echo "   ! could not parse ordersIngested; backing off 10s"
      sleep 10
      continue
    fi
    if [[ "$ingested" -eq 0 ]]; then
      echo "✓ ${store} caught up"
      break
    fi
    sleep 2  # tiny pause to avoid hammering
  done
}

for s in "${STORES[@]}"; do
  run_store "$s"
done