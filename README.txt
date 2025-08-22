
# Patch bundle
- server/src/worker.ts  (fixes Shopify pagination 400 + inserts into staging_raw with channel_id/source/kind/domain when present)
- db/transform_from_staging_v4.sql (handles NOT NULL channel_id on customers & sync_state)

## Install
unzip -o patch_cf_worker_and_sql_v4.zip

cd server
npm install @neondatabase/serverless
wrangler deploy

## Run transform
psql "$DATABASE_URL" -f db/transform_from_staging_v4.sql
