BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Required columns/indexes (idempotent)
ALTER TABLE IF NOT EXISTS orders
  ADD COLUMN IF NOT EXISTS shop_domain text;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'uq_orders_external'
  ) THEN
    ALTER TABLE orders
      ADD CONSTRAINT uq_orders_external UNIQUE (external_id);
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='public' AND indexname='uq_order_items_order_extitem'
  ) THEN
    CREATE UNIQUE INDEX uq_order_items_order_extitem
      ON order_items(order_id, external_item_id);
  END IF;
END$$;

-- Pull Shopify orders from staging (optionally tagged with s.domain)
WITH src AS (
  SELECT
    (payload->>'id')::text                            AS external_id,
    COALESCE(
      NULLIF(s.domain, ''),
      (payload->>'email')::text,                -- fallback not ideal, but keep for now
      ''
    )                                               AS shop_domain_guess,
    (payload->>'created_at')::timestamptz            AS placed_at,
    COALESCE((payload->>'total_price')::numeric,0)   AS total
  FROM staging_raw s
  WHERE (payload->>'id') IS NOT NULL
    AND (payload->>'created_at') IS NOT NULL
),
upsert_orders AS (
  INSERT INTO orders (external_id, placed_at, total, shop_domain)
  SELECT
    external_id,
    placed_at,
    total,
    NULLIF(shop_domain_guess,'')  -- allow null if unknown
  FROM src
  ON CONFLICT (external_id) DO UPDATE
    SET placed_at = EXCLUDED.placed_at,
        total     = EXCLUDED.total,
        shop_domain = COALESCE(orders.shop_domain, EXCLUDED.shop_domain)
  RETURNING id, external_id
)

-- Items extract
INSERT INTO order_items (order_id, sku, external_product_id, title, qty, unit_price, discount, tax, fees, landed_cost_alloc, external_item_id)
SELECT
  o.id AS order_id,
  (li->>'sku')::text                                 AS sku,
  (li->>'product_id')::text                          AS external_product_id,
  COALESCE(NULLIF(li->>'title',''), (li->>'name'))   AS title,
  COALESCE((li->>'quantity')::int,0)                 AS qty,
  COALESCE((li->>'price')::numeric,0)                AS unit_price,
  0::numeric                                         AS discount,
  0::numeric                                         AS tax,
  0::numeric                                         AS fees,
  0::numeric                                         AS landed_cost_alloc,
  (li->>'id')::text                                  AS external_item_id
FROM staging_raw s
JOIN orders o
  ON (s.payload->>'id')::text = o.external_id
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(s.payload->'line_items','[]'::jsonb)) li
ON CONFLICT (order_id, external_item_id) DO UPDATE
  SET qty        = EXCLUDED.qty,
      unit_price = EXCLUDED.unit_price,
      title      = EXCLUDED.title;

-- Backfill shop_domain on orders from staging_raw.domain when blank
UPDATE orders o
SET shop_domain = s.domain
FROM staging_raw s
WHERE (s.payload->>'id')::text = o.external_id
  AND COALESCE(o.shop_domain,'') = ''
  AND COALESCE(s.domain,'') <> '';

COMMIT;