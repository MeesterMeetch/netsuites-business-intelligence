-- db/transform_from_staging_v5.sql
-- Purpose: fix errors about r.kind / r.source (not present in staging_raw)
-- Also populates orders.shop_domain from staging_raw.domain and syncs order_items safely.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Ensure columns exist
ALTER TABLE IF EXISTS orders
  ADD COLUMN IF NOT EXISTS shop_domain text;

ALTER TABLE IF EXISTS customers
  ADD COLUMN IF NOT EXISTS channel_id int;

ALTER TABLE IF EXISTS sync_state
  ADD COLUMN IF NOT EXISTS channel_id int;

-- Optional helpful indexes (safe if they already exist)
DO $$BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                 WHERE c.relname='idx_staging_raw_payload_id' AND n.nspname='public') THEN
    CREATE INDEX idx_staging_raw_payload_id ON staging_raw ((payload->>'id'));
  END IF;
EXCEPTION WHEN undefined_table THEN
  -- staging_raw might not exist yet; ignore
END$$;

-- === CUSTOMERS (minimal upsert by email) ===
WITH raw AS (
  SELECT DISTINCT
    NULLIF(COALESCE((payload->'customer'->>'email')::text, payload->>'email'), '') AS email
  FROM staging_raw r
  WHERE r.payload ? 'id'
)
MERGE INTO customers c
USING raw s
  ON (c.email = s.email)
WHEN NOT MATCHED AND s.email IS NOT NULL THEN
  INSERT (email) VALUES (s.email);

-- === ORDERS (external_id + placed_at + total + shop_domain) ===
WITH raw AS (
  SELECT
    (payload->>'id')::text                AS external_id,
    (payload->>'created_at')::timestamptz AS placed_at,
    COALESCE((payload->>'total_price')::numeric, 0)::numeric(12,2) AS total,
    r.domain                              AS shop_domain
  FROM staging_raw r
  WHERE r.payload ? 'id' AND r.payload ? 'created_at'
)
MERGE INTO orders o
USING raw s
  ON (o.external_id = s.external_id)
WHEN MATCHED THEN
  UPDATE SET
    placed_at   = COALESCE(s.placed_at, o.placed_at),
    total       = COALESCE(s.total, o.total),
    shop_domain = COALESCE(s.shop_domain, o.shop_domain)
WHEN NOT MATCHED THEN
  INSERT (external_id, placed_at, total, shop_domain)
  VALUES (s.external_id, s.placed_at, s.total, s.shop_domain);

-- Backfill shop_domain for any legacy rows still null
UPDATE orders o
SET shop_domain = r.domain
FROM staging_raw r
WHERE o.shop_domain IS NULL
  AND r.payload ? 'id'
  AND (r.payload->>'id')::text = o.external_id;

-- === ORDER ITEMS (idempotent) ===
WITH raw AS (
  SELECT
    (r.payload->>'id')::text AS external_order_id,
    jsonb_array_elements(r.payload->'line_items') AS li
  FROM staging_raw r
  WHERE r.payload ? 'id' AND r.payload ? 'line_items'
),
items AS (
  SELECT
    external_order_id,
    (li->>'id')::text                                   AS external_item_id,
    COALESCE((li->>'quantity')::int, 1)                  AS quantity,
    COALESCE((li->>'price')::numeric, 0)::numeric(12,2)  AS price
  FROM raw
)
MERGE INTO order_items oi
USING (
  SELECT o.id AS order_id, i.external_item_id, i.quantity, i.price
  FROM items i
  JOIN orders o ON o.external_id = i.external_order_id
) s
ON (oi.order_id = s.order_id AND oi.external_item_id = s.external_item_id)
WHEN MATCHED THEN UPDATE SET
  quantity = s.quantity,
  price    = s.price
WHEN NOT MATCHED THEN INSERT (order_id, external_item_id, quantity, price)
VALUES (s.order_id, s.external_item_id, s.quantity, s.price);
