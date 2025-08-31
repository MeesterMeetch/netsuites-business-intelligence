#!/usr/bin/env bash
set -euo pipefail

# Set your worker base once here or pass BASE env when running the script
BASE="${BASE:-https://netsuite-bi-ingest.mitchbiworker.workers.dev}"

ts() { date +%s; }
section() { echo; echo "────────────────────────────────────────"; echo "$@"; }

req() {
  # usage: req METHOD PATH
  local method="$1"; shift
  local path="$1"; shift
  echo
  echo "$method $path"
  # Try to parse as JSON; if that fails, show first 400 chars raw
  if ! curl -sS -X "$method" -H 'Accept: application/json' "$BASE$path" | jq .; then
    echo "↳ Non‑JSON body (first 400 chars):"
    curl -sS -X "$method" -H 'Accept: application/json' "$BASE$path" | head -c 400; echo
  fi
}

# 1) CORS preflight sanity
section "OPTIONS / (CORS preflight)"
curl -si -X OPTIONS "$BASE/" | sed -n '1,20p'
echo "✓ OPTIONS responded"

# 2) Ping
section "GET /api/debug/ping"
req GET "/api/debug/ping?_ts=$(ts)"

# 3) Health (light = cheap and reliable)
section "GET /api/debug/health?light=true"
req GET "/api/debug/health?light=true&_ts=$(ts)"

# 4) Health (full = attempts heavy counts but still returns JSON even on failure if you've applied Option B)
section "GET /api/debug/health?full=true"
req GET "/api/debug/health?full=true&_ts=$(ts)"

# 5) Rolling KPIs (forces JSON; if CF throws a plain error page you'll see it as Non‑JSON)
section "GET /api/kpis/rolling"
req GET "/api/kpis/rolling?_ts=$(ts)"

# 6) Manual ingest kick (edit store as needed)
section "POST /ingest/shopify/run (cbguys, 30d)"
req POST "/ingest/shopify/run?store=cbguys.myshopify.com&days=30"