-- db/transform_from_staging_v6b.sql
-- Safe schema/transform patch for current setup (Neon/Postgres)

-- 1) Extensions (idempotent)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 2) Ensure expected unique indexes exist (idempotent, no transactions)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='public' AND indexname='uq_orders_external_id'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uq_orders_external_id ON orders(external_id)';
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='public' AND indexname='uq_order_items_order_extitem'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uq_order_items_order_extitem ON order_items(order_id, external_item_id)';
  END IF;
END$$;

-- 3) Ensure orders.shop_domain column exists (you already have it, but safe guard)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='orders' AND column_name='shop_domain'
  ) THEN
    EXECUTE 'ALTER TABLE orders ADD COLUMN shop_domain text';
  END IF;
END$$;

-- 4) Backfill orders.shop_domain from staging_raw.domain when available
--    (uses Shopify order id in payload to match existing orders.external_id)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='staging_raw' AND column_name='domain'
  ) THEN
    UPDATE orders o
    SET shop_domain = s.domain
    FROM staging_raw s
    WHERE (s.payload->>'id')::text = o.external_id
      AND COALESCE(o.shop_domain,'') = ''
      AND COALESCE(s.domain,'') <> '';
  END IF;
END$$;

-- 5) (Optional) Light vacuum analyze on the touched tables (safe/no-op if perms restricted)
-- COMMENT OUT IF YOUR ROLE LACKS PRIVS
-- ANALYZE orders;
-- ANALYZE order_items;