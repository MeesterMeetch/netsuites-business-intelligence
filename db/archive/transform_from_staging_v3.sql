-- db/transform_from_staging_v3.sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

INSERT INTO channels(name)
SELECT 'Shopify' WHERE NOT EXISTS (SELECT 1 FROM channels WHERE name='Shopify');

WITH inferred AS (
  SELECT DISTINCT NULLIF(r.payload->>'domain','') AS domain,
         split_part(NULLIF(r.payload->>'domain',''), '.', 1) AS handle
  FROM staging_raw r
  WHERE r.payload ? 'id' AND r.payload ? 'line_items'
)
MERGE INTO shops AS t
USING inferred s
  ON (t.domain = s.domain)
WHEN MATCHED THEN
  UPDATE SET handle = s.handle, is_active = TRUE
WHEN NOT MATCHED AND s.domain IS NOT NULL THEN
  INSERT (handle, domain, is_active) VALUES (s.handle, s.domain, TRUE);

WITH c_src AS (
  SELECT DISTINCT (r.payload->'customer'->>'email') AS email
  FROM staging_raw r
  WHERE r.payload ? 'id' AND r.payload ? 'line_items'
    AND r.payload->'customer'->>'email' IS NOT NULL
)
MERGE INTO customers AS c
USING c_src s
  ON (c.email = s.email)
WHEN NOT MATCHED THEN
  INSERT (id, email) VALUES (gen_random_uuid(), s.email);

WITH o_src AS (
  SELECT
    (r.payload->>'id')::text                      AS external_id,
    (r.payload->>'created_at')::timestamptz       AS placed_at,
    COALESCE((r.payload->>'total_price')::numeric,0) AS total,
    sh.id                                         AS shop_id,
    (SELECT id FROM channels WHERE name='Shopify') AS channel_id,
    cu.id                                         AS customer_id
  FROM staging_raw r
  LEFT JOIN shops sh ON sh.domain = NULLIF(r.payload->>'domain','')
  LEFT JOIN customers cu ON cu.email = r.payload->'customer'->>'email'
  WHERE r.payload ? 'id' AND r.payload ? 'line_items'
)
MERGE INTO orders AS o
USING o_src s
  ON (o.external_id = s.external_id)
WHEN MATCHED THEN
  UPDATE SET placed_at = s.placed_at,
             total     = s.total,
             shop_id   = s.shop_id,
             channel_id= s.channel_id,
             customer_id = s.customer_id
WHEN NOT MATCHED THEN
  INSERT (external_id, placed_at, total, shop_id, channel_id, customer_id)
  VALUES (s.external_id, s.placed_at, s.total, s.shop_id, s.channel_id, s.customer_id);

WITH affected_orders AS (
  SELECT DISTINCT o.id AS order_id
  FROM staging_raw r
  JOIN orders o ON o.external_id = (r.payload->>'id')::text
  WHERE r.payload ? 'id' AND r.payload ? 'line_items'
)
DELETE FROM order_items oi
USING affected_orders a
WHERE oi.order_id = a.order_id;

WITH src AS (
  SELECT
    o.id AS order_id,
    jsonb_array_elements(r.payload->'line_items') AS li
  FROM staging_raw r
  JOIN orders o ON o.external_id = (r.payload->>'id')::text
  WHERE r.payload ? 'line_items' AND r.payload ? 'id'
),
items AS (
  SELECT
    order_id,
    COALESCE(NULLIF(li->>'sku',''), '')                AS sku,
    COALESCE(li->>'name','')                           AS title,
    COALESCE((li->>'quantity')::int,0)                 AS qty,
    COALESCE((li->>'price')::numeric,0)                AS unit_price,
    COALESCE((li->>'total_discount')::numeric,0)       AS discount,
    COALESCE((SELECT SUM( (t->>'price')::numeric )
              FROM jsonb_array_elements(COALESCE(li->'tax_lines','[]'::jsonb)) t), 0) AS tax
  FROM src
)
INSERT INTO order_items(id, order_id, sku, title, qty, unit_price, discount, tax)
SELECT gen_random_uuid(), order_id, sku, title, qty, unit_price, discount, tax
FROM items;

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

WITH s AS (
  SELECT 'shopify:last_transform'::text AS key,
         jsonb_build_object(
           'at', now(),
           'orders', (SELECT COUNT(*) FROM orders),
           'order_items', (SELECT COUNT(*) FROM order_items),
           'customers', (SELECT COUNT(*) FROM customers)
         ) AS value
)
MERGE INTO sync_state t
USING s
  ON (t.key = s.key)
WHEN MATCHED THEN
  UPDATE SET value = s.value, updated_at = now()
WHEN NOT MATCHED THEN
  INSERT (key, value, updated_at) VALUES (s.key, s.value, now());
