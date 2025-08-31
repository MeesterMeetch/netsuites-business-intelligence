BEGIN;

-- Drop KPI views first so we can recreate safely
DROP VIEW IF EXISTS v_store_summary_yday_vs_prev7;
DROP VIEW IF EXISTS v_kpis_rolling_7_30;
DROP VIEW IF EXISTS v_aov_by_store_daily;
DROP VIEW IF EXISTS v_daily_kpis_by_store;
DROP VIEW IF EXISTS v_order_items_365_mt;
DROP VIEW IF EXISTS v_orders_365_mt;

-- 1) Orders (last 365d) with MT-local day
CREATE VIEW v_orders_365_mt AS
SELECT
  o.id,
  o.channel_id,
  o.shop_id,
  o.external_id,
  o.order_number,
  o.name,
  o.placed_at,
  (o.placed_at AT TIME ZONE 'America/Denver')::date AS day_mt,
  o.currency,
  COALESCE(o.subtotal,0)::numeric(20,2)  AS subtotal,
  COALESCE(o.shipping,0)::numeric(20,2)  AS shipping,
  COALESCE(o.tax,0)::numeric(20,2)       AS tax,
  COALESCE(o.discounts,0)::numeric(20,2) AS discounts,
  COALESCE(o.fees,0)::numeric(20,2)      AS fees,
  COALESCE(o.total,0)::numeric(20,2)     AS total,
  COALESCE(o.total,0)::numeric(20,2)     AS order_total,
  o.financial_status,
  o.fulfillment_status,
  o.customer_id,
  o.shop_domain
FROM orders o
WHERE o.placed_at >= (CURRENT_DATE - INTERVAL '365 days');

-- 2) Items (last 365d) joined to orders, with MT-local day
CREATE VIEW v_order_items_365_mt AS
SELECT
  oi.id,
  oi.order_id,
  oi.sku,
  oi.external_product_id,
  oi.title,
  oi.qty,
  oi.unit_price,
  oi.discount,
  oi.tax,
  oi.fees,
  oi.landed_cost_alloc,
  oi.external_item_id,
  o.shop_domain,
  (o.placed_at AT TIME ZONE 'America/Denver')::date AS day_mt
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
WHERE o.placed_at >= (CURRENT_DATE - INTERVAL '365 days');

-- 3) Daily KPIs by store (MT)
CREATE VIEW v_daily_kpis_by_store AS
WITH daily_orders AS (
  SELECT
    o.shop_domain                           AS shop_domain,
    o.day_mt                                AS day_mt,
    SUM(o.order_total)::numeric(20,2)       AS revenue,
    COUNT(DISTINCT o.id)::int               AS orders
  FROM v_orders_365_mt o
  GROUP BY o.shop_domain, o.day_mt
),
daily_units AS (
  SELECT
    i.shop_domain AS shop_domain,
    i.day_mt      AS day_mt,
    SUM(i.qty)::int AS units
  FROM v_order_items_365_mt i
  GROUP BY i.shop_domain, i.day_mt
)
SELECT
  d.day_mt,
  d.shop_domain,
  d.revenue,
  d.orders,
  COALESCE(u.units, 0) AS units
FROM daily_orders d
LEFT JOIN daily_units u
  ON u.shop_domain = d.shop_domain
 AND u.day_mt      = d.day_mt;

-- 4) AOV by store per day
CREATE VIEW v_aov_by_store_daily AS
SELECT
  day_mt,
  shop_domain,
  revenue,
  orders,
  units,
  CASE WHEN orders > 0
       THEN (revenue / orders)::numeric(20,2)
       ELSE 0::numeric(20,2)
  END AS aov
FROM v_daily_kpis_by_store;

-- 5) Rolling 7 / 30 snapshot (as of “today” MT)
CREATE VIEW v_kpis_rolling_7_30 AS
WITH base AS (
  SELECT *
  FROM v_daily_kpis_by_store
  WHERE day_mt >= (CURRENT_DATE - INTERVAL '30 days')
),
agg_7 AS (
  SELECT
    shop_domain,
    SUM(revenue)::numeric(20,2) AS revenue_7d,
    SUM(orders)::int            AS orders_7d,
    SUM(units)::int             AS units_7d
  FROM base
  WHERE day_mt >= (CURRENT_DATE - INTERVAL '7 days')
  GROUP BY shop_domain
),
agg_30 AS (
  SELECT
    shop_domain,
    SUM(revenue)::numeric(20,2) AS revenue_30d,
    SUM(orders)::int            AS orders_30d,
    SUM(units)::int             AS units_30d
  FROM base
  GROUP BY shop_domain
)
SELECT
  a30.shop_domain,
  COALESCE(a7.revenue_7d, 0)::numeric(20,2) AS revenue_7d,
  COALESCE(a7.orders_7d, 0)::int            AS orders_7d,
  COALESCE(a7.units_7d, 0)::int             AS units_7d,
  a30.revenue_30d,
  a30.orders_30d,
  a30.units_30d
FROM agg_30 a30
LEFT JOIN agg_7 a7
  ON a7.shop_domain = a30.shop_domain;

-- 6) Store summary: yesterday vs previous 7‑day avg (MT)
-- (Keeps the column names you saw: yday_*, avg7_*, deltas, pct)
CREATE VIEW v_store_summary_yday_vs_prev7 AS
WITH yday AS (
  SELECT
    shop_domain,
    revenue AS yday_revenue,
    orders  AS yday_orders,
    units   AS yday_units
  FROM v_daily_kpis_by_store
  WHERE day_mt = (CURRENT_DATE - INTERVAL '1 day')
),
prev7 AS (
  SELECT
    shop_domain,
    AVG(revenue)::numeric(20,2) AS avg7_revenue,
    AVG(orders)::numeric(20,2)  AS avg7_orders,
    AVG(units)::numeric(20,2)   AS avg7_units
  FROM v_daily_kpis_by_store
  WHERE day_mt >= (CURRENT_DATE - INTERVAL '8 days')
    AND day_mt <  (CURRENT_DATE - INTERVAL '1 day')
  GROUP BY shop_domain
)
SELECT
  COALESCE(y.shop_domain, p.shop_domain)              AS shop_domain,
  COALESCE(y.yday_revenue, 0)::numeric(20,2)          AS yday_revenue,
  COALESCE(y.yday_orders, 0)::numeric(20,2)           AS yday_orders,
  COALESCE(y.yday_units, 0)::numeric(20,2)            AS yday_units,
  COALESCE(p.avg7_revenue, 0)::numeric(20,2)          AS avg7_revenue,
  COALESCE(p.avg7_orders, 0)::numeric(20,2)           AS avg7_orders,
  COALESCE(p.avg7_units, 0)::numeric(20,2)            AS avg7_units,
  (COALESCE(y.yday_revenue,0) - COALESCE(p.avg7_revenue,0))::numeric(20,2) AS delta_revenue,
  (COALESCE(y.yday_orders,0)  - COALESCE(p.avg7_orders,0))::numeric(20,2)  AS delta_orders,
  (COALESCE(y.yday_units,0)   - COALESCE(p.avg7_units,0))::numeric(20,2)   AS delta_units,
  CASE WHEN COALESCE(p.avg7_revenue,0) <> 0
       THEN ((COALESCE(y.yday_revenue,0) - COALESCE(p.avg7_revenue,0)) / p.avg7_revenue)::numeric(20,4)
       ELSE NULL::numeric(20,4)
  END AS pct_revenue,
  CASE WHEN COALESCE(p.avg7_orders,0) <> 0
       THEN ((COALESCE(y.yday_orders,0)  - COALESCE(p.avg7_orders,0)) / p.avg7_orders)::numeric(20,4)
       ELSE NULL::numeric(20,4)
  END AS pct_orders,
  CASE WHEN COALESCE(p.avg7_units,0) <> 0
       THEN ((COALESCE(y.yday_units,0)   - COALESCE(p.avg7_units,0)) / p.avg7_units)::numeric(20,4)
       ELSE NULL::numeric(20,4)
  END AS pct_units
FROM yday y
FULL OUTER JOIN prev7 p
  ON p.shop_domain = y.shop_domain;

COMMIT;