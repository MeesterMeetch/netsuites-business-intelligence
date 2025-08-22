# Worker Patch: Domain Sanitization + Debug Endpoint

This patch updates `src/worker.ts` to sanitize Shopify store domains and adds a debug endpoint.

## New Features
- Sanitizes domains (`https://`, trailing `/`, stray quotes removed)
- Validates domains must end with `myshopify.com`
- `/api/debug/stores`: show raw vs sanitized domains with validity
- `/api/shops` and `/ingest/shopify/run` remain

## Install
```bash
# from repo root
unzip -o worker_sanitize_patch.zip

cd server
npm install
wrangler deploy
```

## Test
```bash
# Debug view of stores
curl https://netsuite-bi-ingest.mitchbiworker.workers.dev/api/debug/stores

# Trigger ingest
curl -X POST https://netsuite-bi-ingest.mitchbiworker.workers.dev/ingest/shopify/run
```

Check Neon afterwards:
```bash
psql "$DATABASE_URL" -c "select count(*) as raw_orders from staging_raw;"
```
