# Patch: Cursor typecast fix + health endpoint + SQL transform

This bundle contains:
- `server/src/worker.ts` — Fixes the “could not determine data type of parameter $2” error by casting to text in `setCursor()`, plus MT logging, per‑store health, cron round‑robin, cursors.
- `db/transform_from_staging_v4.sql` — MERGE‑safe transform that writes `orders.shop_domain`, syncs `order_items`, and backfills `shop_domain` from `staging_raw`.

## Apply

```bash
# unzip into your repo root (so it writes to server/src and db/)
unzip -o patch_cursor_typecast_fix_v1.zip

# deploy the worker
cd server
wrangler deploy
cd ..

# run the transform
psql "$DATABASE_URL" -f db/transform_from_staging_v4.sql

# verify
curl https://netsuite-bi-ingest.<your-subdomain>.workers.dev/api/debug/health
# yours:
# curl https://netsuite-bi-ingest.mitchbiworker.workers.dev/api/debug/health
```

## Local testing (optional)
Create `server/.dev.vars` with your secrets, then:
```bash
cd server
wrangler dev --local
# in another terminal:
curl -X POST "http://127.0.0.1:8787/ingest/shopify/run"
```

If anything shouts, paste the error and I’ll tweak fast.
