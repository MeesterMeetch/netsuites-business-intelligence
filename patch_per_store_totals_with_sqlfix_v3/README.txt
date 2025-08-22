# Patch: Per‑store totals + Health endpoint (MT) + SQL MERGE fixes

This bundle contains:
- `server/src/worker.ts` — adds `/api/debug/health` with per‑store totals, MT logging, cursor resume, round‑robin cron.
- `db/transform_from_staging_v4.sql` — writes `orders.shop_domain`, keeps `order_items` synced, and backfills `shop_domain` from `staging_raw`.

## Apply

1) Unzip into your repo root (will place files under `server/src/` and `db/`):
   ```bash
   unzip -o patch_per_store_totals_with_sqlfix_v3.zip
   ```

2) Deploy the Worker:
   ```bash
   cd server
   wrangler deploy
   cd ..
   ```

3) Run the transform:
   ```bash
   psql "$DATABASE_URL" -f db/transform_from_staging_v4.sql
   ```

4) Verify health (per‑store totals):
   ```bash
   curl https://netsuite-bi-ingest.<your-subdomain>.workers.dev/api/debug/health
   # for you:
   # curl https://netsuite-bi-ingest.mitchbiworker.workers.dev/api/debug/health
   ```

If you see any stores reporting `(unknown)` for `shop_domain`, re‑ingest a recent window with a reset so `staging_raw.domain` is populated, then re‑run the transform:
```bash
curl -X POST "https://netsuite-bi-ingest.<subdomain>.workers.dev/ingest/shopify/run?store=<domain>&days=7&reset=true"
psql "$DATABASE_URL" -f db/transform_from_staging_v4.sql
```
