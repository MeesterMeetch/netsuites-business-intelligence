-- db/views_bi.sql
-- BI Views: safe to re-run (CREATE OR REPLACE). Assumes tables:
--   orders(id, placed_at, total, shop_domain, customer_id)
--   order_items(id, order_id, sku, title, qty, unit_price, external_item_id)
--   customers(id, email, ...)
-- If your columns differ, tell me and I'll adapt.

---------------------------------------------
-- 1) Daily sales by store
---------------------------------------------
CREATE OR REPLACE VIEW v_sales_by_store_daily AS
WITH item_rev AS (
  SELECT
    oi.order_id,
    SUM(COALESCE(oi.unit_price,0)::numeric * COALESCE(oi.qty,0))::numeric(18,2) AS items_revenue,
    SUM(COALESCE(oi.qty,0))::int AS units
  FROM order_items oi
  GROUP BY oi.order_id
)
SELECT
  o.shop_domain,
  DATE_TRUNC('day', o.placed_at) AS day,
  COUNT(DISTINCT o.id)           AS orders,
  COALESCE(ir.units,0)           AS units,
  -- prefer order "total" if present; fall back to summed item revenue
  SUM(COALESCE(o.total, ir.items_revenue, 0))::numeric(18,2) AS revenue
FROM orders o
LEFT JOIN item_rev ir ON ir.order_id = o.id
GROUP BY o.shop_domain, DATE_TRUNC('day', o.placed_at), ir.units
ORDER BY day DESC, o.shop_domain;

---------------------------------------------
-- 2) Monthly sales by store
---------------------------------------------
CREATE OR REPLACE VIEW v_sales_by_store_monthly AS
WITH item_rev AS (
  SELECT
    oi.order_id,
    SUM(COALESCE(oi.unit_price,0)::numeric * COALESCE(oi.qty,0))::numeric(18,2) AS items_revenue,
    SUM(COALESCE(oi.qty,0))::int AS units
  FROM order_items oi
  GROUP BY oi.order_id
)
SELECT
  o.shop_domain,
  DATE_TRUNC('month', o.placed_at) AS month,
  COUNT(DISTINCT o.id)             AS orders,
  SUM(COALESCE(ir.units,0))        AS units,
  SUM(COALESCE(o.total, ir.items_revenue, 0))::numeric(18,2) AS revenue
FROM orders o
LEFT JOIN item_rev ir ON ir.order_id = o.id
GROUP BY o.shop_domain, DATE_TRUNC('month', o.placed_at)
ORDER BY month DESC, o.shop_domain;

---------------------------------------------
-- 3) Top SKUs last 30 days
---------------------------------------------
CREATE OR REPLACE VIEW v_top_skus_30d AS
WITH src AS (
  SELECT
    o.shop_domain,
    COALESCE(oi.sku, '(no-sku)')                          AS sku,
    COALESCE(NULLIF(oi.title, ''), oi.sku, '(no-title)')  AS title,
    COALESCE(oi.qty, 0)::int                              AS units,
    (COALESCE(oi.unit_price, 0)::numeric * COALESCE(oi.qty, 0))::numeric(18,2) AS item_revenue
  FROM order_items oi
  JOIN orders o ON o.id = oi.order_id
  WHERE o.placed_at >= (NOW() - INTERVAL '30 days')
)
SELECT
  shop_domain,
  sku,
  title,
  SUM(units)                         AS units,
  SUM(item_revenue)::numeric(18,2)   AS revenue
FROM src
GROUP BY shop_domain, sku, title
HAVING SUM(units) > 0 OR SUM(item_revenue) > 0
ORDER BY revenue DESC, units DESC;

---------------------------------------------
-- 4) Customer repeat rates
---------------------------------------------
CREATE OR REPLACE VIEW v_customer_repeat_rates AS
WITH per_customer AS (
  SELECT
    o.shop_domain,
    o.customer_id,
    COUNT(DISTINCT o.id) AS order_count
  FROM orders o
  GROUP BY o.shop_domain, o.customer_id
)
SELECT
  shop_domain,
  COUNT(*) FILTER (WHERE customer_id IS NOT NULL)                         AS total_customers,
  COUNT(*) FILTER (WHERE customer_id IS NOT NULL AND order_count > 1)     AS repeat_customers,
  CASE
    WHEN COUNT(*) FILTER (WHERE customer_id IS NOT NULL) = 0 THEN 0
    ELSE ROUND(
      (COUNT(*) FILTER (WHERE customer_id IS NOT NULL AND order_count > 1))::numeric
      / NULLIF(COUNT(*) FILTER (WHERE customer_id IS NOT NULL), 0),
      4
    )
  END AS repeat_rate
FROM per_customer
GROUP BY shop_domain
ORDER BY repeat_rate DESC NULLS LAST;

---------------------------------------------
-- 5) Monthly cohort analysis (by customer's first month)
---------------------------------------------
CREATE OR REPLACE VIEW v_orders_cohort_monthly AS
WITH first_order AS (
  SELECT
    o.customer_id,
    MIN(o.placed_at) AS first_ts
  FROM orders o
  WHERE o.customer_id IS NOT NULL
  GROUP BY o.customer_id
),
item_rev AS (
  SELECT
    oi.order_id,
    SUM(COALESCE(oi.unit_price,0)::numeric * COALESCE(oi.qty,0))::numeric(18,2) AS items_revenue
  FROM order_items oi
  GROUP BY oi.order_id
)
SELECT
  DATE_TRUNC('month', f.first_ts)         AS cohort_month,
  DATE_TRUNC('month', o.placed_at)        AS order_month,
  o.shop_domain,
  COUNT(DISTINCT o.id)                    AS orders,
  SUM(COALESCE(o.total, ir.items_revenue, 0))::numeric(18,2) AS revenue
FROM orders o
JOIN first_order f ON f.customer_id = o.customer_id
LEFT JOIN item_rev ir ON ir.order_id = o.id
GROUP BY 1,2,3
ORDER BY cohort_month, order_month, shop_domain;
