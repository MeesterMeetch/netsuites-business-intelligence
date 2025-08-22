
-- transform_from_staging_v2.sql
-- Robust transform that reads Shopify orders directly from staging_raw.payload
-- (no dependency on staging_raw.source/kind), and uses MERGE to upsert
-- without requiring unique constraints.

-- 0) Enable pgcrypto for gen_random_uuid (safe if already present)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1) Ensure Shopify channel exists
INSERT INTO channels(name)
SELECT 'Shopify'
WHERE NOT EXISTS (SELECT 1 FROM channels WHERE name='Shopify');

-- 2) Upsert shops inferred from payload->>'domain'
WITH inferred AS (
  SELECT DISTINCT NULLIF(payload->>'domain','') AS domain
  FROM staging_raw
  WHERE payload ? 'line_items' AND payload ? 'id'
)
INSERT INTO shops(handle, domain, is_active)
SELECT split_part(i.domain, '.', 1) AS handle, i.domain, TRUE
FROM inferred i
WHERE i.domain IS NOT NULL
ON CONFLICT (domain) DO UPDATE SET
  handle = EXCLUDED.handle,
  is_active = TRUE;

-- 3) Upsert customers by email
INSERT INTO customers(id, email)
SELECT DISTINCT gen_random_uuid(), (payload->'customer'->>'email') AS email
FROM staging_raw
WHERE payload ? 'line_items' AND payload ? 'id'
  AND payload->'customer'->>'email' IS NOT NULL
ON CONFLICT (email) DO NOTHING;

-- 4) Upsert orders using MERGE (by external_id = payload->>'id')
WITH src AS (
  SELECT
    (r.payload->>'id')::text                    AS external_id,
    (r.payload->>'created_at')::timestamptz     AS placed_at,
    COALESCE((r.payload->>'total_price')::numeric,0) AS total,
    s.id                                        AS shop_id,
    (SELECT id FROM channels WHERE name='Shopify') AS channel_id,
    c.id                                        AS customer_id
  FROM staging_raw r
  LEFT JOIN shops s
    ON s.domain = NULLIF(r.payload->>'domain','')
  LEFT JOIN customers c
    ON c.email = r.payload->'customer'->>'email'
  WHERE r.payload ? 'line_items' AND r.payload ? 'id'
)
MERGE INTO orders AS o
USING src
  ON (o.external_id = src.external_id)
WHEN MATCHED THEN
  UPDATE SET placed_at = src.placed_at,
             total     = src.total,
             shop_id   = src.shop_id,
             channel_id= src.channel_id,
             customer_id = src.customer_id
WHEN NOT MATCHED THEN
  INSERT (external_id, placed_at, total, shop_id, channel_id, customer_id)
  VALUES (src.external_id, src.placed_at, src.total, src.shop_id, src.channel_id, src.customer_id);

-- 5) Upsert order_items using MERGE (by external_item_id = line_item.id)
WITH src AS (
  SELECT
    (r.payload->>'id')::text AS external_order_id,
    jsonb_array_elements(r.payload->'line_items') AS li
  FROM staging_raw r
  WHERE r.payload ? 'line_items' AND r.payload ? 'id'
),
ord AS (
  SELECT id, external_id FROM orders
),
items AS (
  SELECT
    o.id                                               AS order_id,
    (li->>'id')::text                                  AS external_item_id,
    COALESCE(NULLIF(li->>'sku',''), '')                AS sku,
    COALESCE(li->>'name','')                           AS title,
    COALESCE((li->>'quantity')::int,0)                 AS qty,
    COALESCE((li->>'price')::numeric,0)                AS unit_price,
    COALESCE((li->>'total_discount')::numeric,0)       AS discount,
    COALESCE((SELECT SUM( (t->>'price')::numeric )
              FROM jsonb_array_elements(COALESCE(li->'tax_lines','[]'::jsonb)) t), 0) AS tax
  FROM src s
  JOIN ord o ON o.external_id = s.external_order_id
)
MERGE INTO order_items AS oi
USING items
  ON (oi.external_item_id = items.external_item_id)
WHEN MATCHED THEN
  UPDATE SET order_id   = items.order_id,
             sku        = items.sku,
             title      = items.title,
             qty        = items.qty,
             unit_price = items.unit_price,
             discount   = items.discount,
             tax        = items.tax
WHEN NOT MATCHED THEN
  INSERT (external_item_id, order_id, sku, title, qty, unit_price, discount, tax)
  VALUES (items.external_item_id, items.order_id, items.sku, items.title, items.qty, items.unit_price, items.discount, items.tax);

-- 6) Allocate landed_cost_alloc from sku_costs (effective-dated)
WITH li AS (
  SELECT i.id AS item_id, i.sku, i.qty, o.placed_at::date AS d
  FROM order_items i
  JOIN orders o ON o.id = i.order_id
  WHERE COALESCE(i.sku,'') <> ''
),
priced AS (
  SELECT li.item_id, (li.qty * sc.cost)::numeric AS alloc_cost
  FROM li
  JOIN LATERAL (
    SELECT cost
    FROM sku_costs sc
    WHERE sc.sku = li.sku
      AND sc.effective_from <= li.d
      AND (sc.effective_to IS NULL OR sc.effective_to >= li.d)
    ORDER BY sc.effective_from DESC
    LIMIT 1
  ) sc ON TRUE
)
UPDATE order_items i
SET landed_cost_alloc = p.alloc_cost
FROM priced p
WHERE i.id = p.item_id;

-- 7) Update sync_state
INSERT INTO sync_state(key, value, updated_at)
VALUES ('shopify:last_transform',
        jsonb_build_object(
          'at', now(),
          'orders', (SELECT COUNT(*) FROM orders),
          'order_items', (SELECT COUNT(*) FROM order_items),
          'customers', (SELECT COUNT(*) FROM customers)
        ),
        now())
ON CONFLICT (key) DO UPDATE SET
  value = EXCLUDED.value,
  updated_at = EXCLUDED.updated_at;
