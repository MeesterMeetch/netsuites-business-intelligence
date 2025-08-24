#!/usr/bin/env bash
set -euo pipefail
STORE="${1:-}"; RESET="${2:-false}"; DAYS="${3:-365}"
TOKEN_OPT=""; [[ -n "${BACKFILL_TOKEN:-}" ]] && TOKEN_OPT="&token=$BACKFILL_TOKEN"
curl -i -X POST "https://netsuite-bi-ingest.mitchbiworker.workers.dev/api/admin/backfill?days=$DAYS&store=$STORE&hard_reset=$RESET$TOKEN_OPT"
