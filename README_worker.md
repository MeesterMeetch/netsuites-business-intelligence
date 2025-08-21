# Cloudflare Worker — Shopify Multistore Ingest

## 1) Create DB and run schema
```bash
psql "$DATABASE_URL" -f db/schema.sql
```

## 2) Deploy Worker
```bash
cd server
npm i
# Set secrets (paste JSON array with your 3 stores)
# Example shape: [{"domain":"essentialparts.myshopify.com","token":"..."}, {"domain":"unitedfuses.myshopify.com","token":"..."}, {"domain":"cbg.myshopify.com","token":"..."}]
wrangler secret put SHOPIFY_STORES
wrangler secret put DATABASE_URL
wrangler publish
```

## 3) Run an ingest on-demand
```bash
curl -X POST https://<your-worker-subdomain>.workers.dev/ingest/shopify/run
```

## 4) Fetch AOV metric in your static site
```bash
fetch('https://<your-worker-subdomain>.workers.dev/api/metrics/aov?range=30d&shop=all')
  .then(r => r.json()).then(console.log)
```

## 5) New endpoints

**Orders trend (daily):**
```
GET /api/metrics/orders_trend?range=30d&shop=all|{shop_id}
```

**Returning customer rate:**
```
GET /api/metrics/returning_rate?range=90d&shop=all|{shop_id}
```

Use `shop=all` for roll-up or a specific `shop_id` for per-store.
