# Patch: Cursor typecast fix + no backtick escapes + SQL transform

Apply:
```bash
# unzip into your repo root
unzip -o patch_cursor_typecast_fix_v2.zip

# deploy
cd server
wrangler deploy
cd ..

# run transform
psql "$DATABASE_URL" -f db/transform_from_staging_v4.sql

# check health
curl "https://netsuite-bi-ingest.mitchbiworker.workers.dev/api/debug/health"
```
