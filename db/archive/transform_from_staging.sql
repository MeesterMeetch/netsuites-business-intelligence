
-- transform_from_staging.sql
-- Normalizes Shopify orders stored in staging_raw into analytics tables:
-- channels, shops, customers, orders, order_items, and allocates landed_cost from sku_costs.
-- Safe to run repeatedly (idempotent).

-- Requirements (present in your DB per your \dt output):
--   channels(name)
--   shops(id serial, handle text, domain text unique, is_active bool)
--   customers(id uuid default gen_random_uuid(), email text unique)
--   orders(id uuid default gen_random_uuid(), external_id text unique, placed_at timestamptz, total numeric,
--          shop_id int, channel_id int, customer_id uuid)
--   order_items(id uuid default gen_random_uuid(), external_item_id text unique, order_id uuid,
--               sku text, title text, qty int, unit_price numeric, discount numeric, tax numeric,
--               landed_cost_alloc numeric)
--   sku_costs(sku text, cost numeric, effective_from date, effective_to date null)
--   staging_raw(id uuid, source text, kind text, external_id text, payload jsonb, received_at timestamptz)
--   sync_state(key text primary key, value jsonb, updated_at timestamptz default now())

-- 0) Ensure pgcrypto (for gen_random_uuid) if you ever need it in ad-hoc inserts here.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1) Channel: Shopify
INSERT INTO channels(name)
SELECT 'Shopify'
WHERE NOT EXISTS (SELECT 1 FROM channels WHERE name='Shopify');

-- 2) Shops: infer from staging_raw.source (domain), fallback to payload->>'domain'
WITH inferred AS (
  SELECT DISTINCT
         COALESCE(NULLIF(source, ''), payload->>'domain') AS domain,
         split_part(COALESCE(NULLIF(source, ''), payload->>'domain'), '.', 1) AS handle
  FROM staging_raw
  WHERE kind = 'order'
)
INSERT INTO shops(handle, domain, is_active)
SELECT i.handle, i.domain, TRUE
FROM inferred i
WHERE i.domain IS NOT NULL
ON CONFLICT (domain) DO UPDATE SET
  handle = EXCLUDED.handle,
  is_active = TRUE;

-- 3) Customers from orders
INSERT INTO customers(id, email)
SELECT DISTINCT gen_random_uuid(), (r.payload->'customer'->>'email') AS email
FROM staging_raw r
WHERE r.kind = 'order'
  AND r.payload->'customer'->>'email' IS NOT NULL
ON CONFLICT (email) DO NOTHING;

-- 4) Orders (upsert by external_id)
WITH src AS (
  SELECT
    (r.payload->>'id')::text                      AS external_id,
    (r.payload->>'created_at')::timestamptz       AS placed_at,
    COALESCE((r.payload->>'total_price')::numeric, 0) AS total,
    s.id                                          AS shop_id,
    (SELECT id FROM channels WHERE name='Shopify') AS channel_id,
    c.id                                          AS customer_id
  FROM staging_raw r
  LEFT JOIN shops s
         ON s.domain = COALESCE(NULLIF(r.source, ''), r.payload->>'domain')
  LEFT JOIN customers c
         ON c.email = r.payload->'customer'->>'email'
  WHERE r.kind='order'
)
INSERT INTO orders(external_id, placed_at, total, shop_id, channel_id, customer_id)
SELECT external_id, placed_at, total, shop_id, channel_id, customer_id FROM src
ON CONFLICT (external_id) DO UPDATE SET
  placed_at   = EXCLUDED.placed_at,
  total       = EXCLUDED.total,
  shop_id     = EXCLUDED.shop_id,
  channel_id  = EXCLUDED.channel_id,
  customer_id = EXCLUDED.customer_id;

-- 5) Order Items (upsert by external_item_id)
WITH src AS (
  SELECT
    (r.payload->>'id')::text AS external_order_id,
    jsonb_array_elements(r.payload->'line_items') AS li
  FROM staging_raw r
  WHERE r.kind='order'
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
INSERT INTO order_items(external_item_id, order_id, sku, title, qty, unit_price, discount, tax)
SELECT external_item_id, order_id, sku, title, qty, unit_price, discount, tax
FROM items
ON CONFLICT (external_item_id) DO UPDATE SET
  order_id   = EXCLUDED.order_id,
  sku        = EXCLUDED.sku,
  title      = EXCLUDED.title,
  qty        = EXCLUDED.qty,
  unit_price = EXCLUDED.unit_price,
  discount   = EXCLUDED.discount,
  tax        = EXCLUDED.tax;

-- 6) Allocate landed cost from sku_costs by effective dating
--    Match each line's (sku, order placed_at::date) to the correct cost version.
WITH li AS (
  SELECT i.id AS item_id, i.sku, i.qty, o.placed_at::date AS d
  FROM order_items i
  JOIN orders o ON o.id = i.order_id
  WHERE COALESCE(i.sku,'') <> ''
),
priced AS (
  SELECT
    li.item_id,
    (li.qty * sc.cost)::numeric AS alloc_cost
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

-- 7) Sync state (lightweight bookkeeping)
INSERT INTO sync_state(key, value, updated_at)
VALUES (
  'shopify:last_transform',
  jsonb_build_object(
    'at', now(),
    'orders', (SELECT COUNT(*) FROM orders),
    'order_items', (SELECT COUNT(*) FROM order_items),
    'customers', (SELECT COUNT(*) FROM customers)
  ),
  now()
)
ON CONFLICT (key) DO UPDATE SET
  value = EXCLUDED.value,
  updated_at = EXCLUDED.updated_at;

-- 8) (Optional) Inventory snapshots
-- Your DB already uses inventory_snapshots; Shopify order ingest does not include inventory.
-- If you later ingest Shopify Inventory Levels or NetSuite stock, insert latest per (sku, shop) into inventory_snapshots.
-- Keeping as a placeholder here so this script remains idempotent and future-proof.
-- Example expected columns: (sku text, shop_id int, on_hand int, committed int, backordered int, updated_at timestamptz)
-- INSERT ... ON CONFLICT ...   -- left commented intentionally.
