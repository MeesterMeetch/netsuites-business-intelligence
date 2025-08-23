#!/usr/bin/env bash
set -euo pipefail
MSG="${1:-quick release}"
BR="feat/release-$(date +%Y%m%d-%H%M%S)"

git checkout -b "$BR"
git add -A
git commit -m "$MSG" || echo "no changes to commit"
git pull --rebase origin main || true
git push -u origin "$BR"

echo "Open a PR for $BR, merge it, then press enter to continue..."
read -r

git checkout main
git pull --rebase origin main
( cd server && npx wrangler deploy )
