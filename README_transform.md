# Transform from Staging (Shopify → Analytics)

This script normalizes Shopify **orders** that your Worker saved into `staging_raw` into your analytics tables:
- channels, shops, customers, orders, order_items
- allocates per-line landed cost using `sku_costs` (effective-dated)
- updates a `sync_state` record

## Run it
```bash
psql "$DATABASE_URL" -f db/transform_from_staging.sql
```

## Verify
```bash
psql "$DATABASE_URL" -c "select count(*) as orders, min(placed_at) as first, max(placed_at) as last from orders;"
psql "$DATABASE_URL" -c "select count(*) as items from order_items;"
```

If any column/table name differs in your schema, tell me which error you see and I’ll tweak the SQL.
