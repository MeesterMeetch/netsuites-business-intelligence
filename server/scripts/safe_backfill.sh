# scripts/safe_backfill.sh
#!/usr/bin/env bash
set -euo pipefail

# ===== Config (override with env vars) =====
WORKER_BASE="${WORKER_BASE:-https://netsuite-bi-ingest.mitchbiworker.workers.dev}"
STORES="${STORES:-essential-electric-supply.myshopify.com cbguys.myshopify.com united-fuses.myshopify.com}"
DAYS="${DAYS:-30}"      # how many days of orders to pull per store
SLEEP="${SLEEP:-2}"     # seconds between stores to be polite
JQ="${JQ:-jq}"          # jq binary (must be installed locally)

hit() { curl -sS -X "$1" "$2"; }

echo "== Resetting cursors =="
for store in $STORES; do
  echo "• reset $store"
  hit POST "$WORKER_BASE/api/debug/reset?store=$store" | $JQ . || true
  sleep 1
done

echo
echo "== Running ingest (days=$DAYS) =="
total_pages=0
total_orders=0

for store in $STORES; do
  echo "• ingest $store"
  tries=0
  last_res=""
  while (( tries < 3 )); do
    ((tries++))
    echo "  - attempt $tries"
    res=$(hit POST "$WORKER_BASE/ingest/shopify/run?store=$store&days=$DAYS") || true
    last_res="$res"
    echo "$res" | $JQ . || echo "$res"
    if echo "$res" | $JQ -e '.ok==true' >/dev/null 2>&1; then
      pages=$(echo "$res"   | $JQ -r ".summary[\"$store\"].pages // 0")
      orders=$(echo "$res"  | $JQ -r ".summary[\"$store\"].ordersIngested // 0")
      total_pages=$(( total_pages + pages ))
      total_orders=$(( total_orders + orders ))
      break
    fi
    sleep 3
  done
  if (( tries == 3 )) && ! echo "$last_res" | $JQ -e '.ok==true' >/dev/null 2>&1; then
    echo "  ! WARN: $store failed after retries"
  fi
  sleep "$SLEEP"
done

echo
echo "== Health snapshot =="
hit GET "$WORKER_BASE/api/debug/health" | $JQ . || true

echo
echo "== Summary =="
echo "  total pages:  $total_pages"
echo "  total orders: $total_orders"