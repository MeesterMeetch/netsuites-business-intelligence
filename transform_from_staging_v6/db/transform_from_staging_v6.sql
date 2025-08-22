-- db/transform_from_staging_v6.sql
-- Safer transform:
-- - Removes references to r.kind/r.source
-- - Guarantees orders.shop_domain backfill
-- - Deduplicates by external_id using latest staging_raw.received_at
-- - Makes customers.channel_id nullable (to avoid NOT NULL violations)
-- - Adds order_items.external_item_id if missing and unique key (order_id, external_item_id)
-- - Idempotent MERGEs

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ==== SCHEMA GUARDS ====

-- orders.shop_domain
ALTER TABLE IF EXISTS orders
  ADD COLUMN IF NOT EXISTS shop_domain text;

-- channel_id columns can be nullable
DO $$BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='customers' AND column_name='channel_id') THEN
    EXECUTE 'ALTER TABLE customers ALTER COLUMN channel_id DROP NOT NULL';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='sync_state' AND column_name='channel_id') THEN
    EXECUTE 'ALTER TABLE sync_state ALTER COLUMN channel_id DROP NOT NULL';
  END IF;
END$$;

-- order_items.external_item_id + uniqueness
DO $$BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='order_items' AND column_name='external_item_id') THEN
    ALTER TABLE order_items ADD COLUMN external_item_id text;
  END IF;
  -- add a unique constraint if missing
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'uq_order_items_order_extitem'
  ) THEN
    ALTER TABLE order_items ADD CONSTRAINT uq_order_items_order_extitem UNIQUE (order_id, external_item_id);
  END IF;
EXCEPTION WHEN undefined_table THEN
  -- if order_items doesn't exist yet, skip silently
  NULL;
END$$;

-- helpful index for staging lookups (safe if already there)
DO $$BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='staging_raw') THEN
    IF NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                   WHERE c.relname='idx_staging_raw_payload_id' AND n.nspname='public') THEN
      CREATE INDEX idx_staging_raw_payload_id ON staging_raw ((payload->>'id'));
    END IF;
  END IF;
END$$;


-- ==== DEDUPED SOURCE SETS ====

-- Use latest row per Shopify order id based on staging_raw.received_at
WITH dedup AS (
  SELECT
    (r.payload->>'id')::text                    AS external_id,
    (r.payload->>'created_at')::timestamptz     AS placed_at,
    COALESCE((r.payload->>'total_price')::numeric, 0)::numeric(12,2) AS total,
    r.domain                                    AS shop_domain,
    r.received_at,
    r.payload
  FROM staging_raw r
  WHERE r.payload ? 'id'
),
latest AS (
  SELECT DISTINCT ON (external_id)
    external_id, placed_at, total, shop_domain, received_at, payload
  FROM dedup
  ORDER BY external_id, received_at DESC
),

-- customers deduped by email (nullable safe)
cust AS (
  SELECT DISTINCT NULLIF(COALESCE((payload->'customer'->>'email')::text, payload->>'email'), '') AS email
  FROM latest
  WHERE email IS NOT NULL
),

upsert_customers AS (
  -- insert only new customers by email
  INSERT INTO customers (email)
  SELECT c.email FROM cust c
  LEFT JOIN customers x ON x.email = c.email
  WHERE c.email IS NOT NULL AND x.email IS NULL
  RETURNING 1
),

-- ==== ORDERS ====
upsert_orders AS (
  MERGE INTO orders o
  USING (
    SELECT external_id, placed_at, total, shop_domain FROM latest
  ) s
  ON (o.external_id = s.external_id)
  WHEN MATCHED THEN
    UPDATE SET
      placed_at   = COALESCE(s.placed_at, o.placed_at),
      total       = COALESCE(s.total, o.total),
      shop_domain = COALESCE(s.shop_domain, o.shop_domain)
  WHEN NOT MATCHED THEN
    INSERT (external_id, placed_at, total, shop_domain)
    VALUES (s.external_id, s.placed_at, s.total, s.shop_domain)
  RETURNING 1
),

-- ==== ORDER ITEMS ====
raw_items AS (
  SELECT
    (l.payload->>'id')::text AS external_order_id,
    jsonb_array_elements(l.payload->'line_items') AS li
  FROM latest l
  WHERE l.payload ? 'line_items'
),
items AS (
  SELECT
    external_order_id,
    (li->>'id')::text                                   AS external_item_id,
    COALESCE((li->>'quantity')::int, 1)                 AS quantity,
    COALESCE((li->>'price')::numeric, 0)::numeric(12,2) AS price
  FROM raw_items
),
link AS (
  SELECT o.id AS order_id, i.external_item_id, i.quantity, i.price
  FROM items i
  JOIN orders o ON o.external_id = i.external_order_id
)
MERGE INTO order_items oi
USING link s
ON (oi.order_id = s.order_id AND oi.external_item_id = s.external_item_id)
WHEN MATCHED THEN UPDATE SET
  quantity = s.quantity,
  price    = s.price
WHEN NOT MATCHED THEN
  INSERT (order_id, external_item_id, quantity, price)
  VALUES (s.order_id, s.external_item_id, s.quantity, s.price);

-- Final safety backfill for shop_domain, in case anything matched older rows
UPDATE orders o
SET shop_domain = l.shop_domain
FROM latest l
WHERE o.external_id = l.external_id
  AND (o.shop_domain IS NULL OR o.shop_domain = '');

