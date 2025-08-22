# COGS Import & Allocation Bundle

This adds **NetSuite COGS per SKU** and allocates per-line costs to `order_items.landed_cost_alloc` so your margin charts reflect true gross margin.

## 1) Run the SQL
```bash
psql "$DATABASE_URL" -f db/sku_costs.sql
```

## 2) Add env var (secure admin endpoints)
Set a secret for admin:
```bash
wrangler secret put ADMIN_TOKEN
```

## 3) Add the Worker patch
Open `server/src/worker.ts` and paste the PATCH block into your file:
- Add `ADMIN_TOKEN` to the `Env` type
- Add the two routes under `fetch()`
- Add the helper functions at the bottom

## 4) Upload a NetSuite COGS CSV
Format: `sku,cost,effective_from,effective_to?`
Example:
```
sku,cost,effective_from,effective_to
QO250,38.50,2025-01-01,
QO250,39.75,2025-06-01,
THQB2150,62.10,2025-03-01,2025-07-31
```

Upload via curl (Bearer token required):
```bash
curl -X POST "https://<your-worker>.workers.dev/admin/costs/upload" \
  -H "Authorization: Bearer <ADMIN_TOKEN>" \
  -H "Content-Type: text/csv" \
  --data-binary @netsuite_cogs.csv
```

## 5) Recompute landed costs
```bash
curl -X POST "https://<your-worker>.workers.dev/admin/costs/recompute?range=365d&shop=all" \
  -H "Authorization: Bearer <ADMIN_TOKEN>"
```

This sets `order_items.landed_cost_alloc = qty * matched_cost(sku, placed_at)` using the most recent effective cost valid on the order date.

## 6) See margins
Your existing `GET /api/metrics/channel_margin` will now reflect **Revenue - Landed Cost** by channel.

---

**Tip:** You can run the recompute daily after ingest as a cron in Cloudflare (or call it from your CI).