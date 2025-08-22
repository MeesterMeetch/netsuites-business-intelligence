
# Transform from Staging (v2)

This version does **not** rely on `staging_raw.source` or `staging_raw.kind`. It reads directly from `staging_raw.payload`, detects Shopify orders by the presence of `line_items` and `id`, and uses **MERGE** to upsert without needing unique constraints.

## Run
```bash
psql "$DATABASE_URL" -f db/transform_from_staging_v2.sql
```

## Verify
```bash
psql "$DATABASE_URL" -c "select count(*) as orders, min(placed_at) as first, max(placed_at) as last from orders;"
psql "$DATABASE_URL" -c "select count(*) as items from order_items;"
```
