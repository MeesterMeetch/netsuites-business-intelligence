# Transform v5 (no r.kind/r.source)

This fixes the `column r.kind does not exist` errors by removing those optional filters.
It also backfills `orders.shop_domain` from `staging_raw.domain` so your per-store totals stop showing `(unknown)`.

## Apply

```bash
# from your repo root
psql "$DATABASE_URL" -f db/transform_from_staging_v5.sql

# verify overall + per-store
psql "$DATABASE_URL" -c "select count(*) as orders, min(placed_at) first, max(placed_at) last from orders;"
psql "$DATABASE_URL" -c "select count(*) as items from order_items;"
psql "$DATABASE_URL" -c "select shop_domain, count(*) orders from orders group by 1 order by 2 desc;"
```

If `db/transform_from_staging_v5.sql` is not in your repo yet, copy it into `db/` first.
