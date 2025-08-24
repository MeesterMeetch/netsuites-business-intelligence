-- server/sql/001_kpi_views.sql
-- Recreate KPI views used by the Worker + dashboard
-- Safe to run repeatedly on deploy.

-- 0) Drop existing (handles dependency order)
DO $$
DECLARE v text;
BEGIN
  FOR v IN
    SELECT format('%I.%I', n.nspname, c.relname)
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'v'
      AND c.relname = ANY (ARRAY[
        'v_aov_by_store_daily',
        'v_store_summary_yday_vs_prev7',
        'v_kpis_rolling_7_30',
        'v_daily_kpis_by_store',
        'v_sales_by_store_daily'
      ])
  LOOP
    EXECUTE 'DROP VIEW IF EXISTS ' || v || ' CASCADE';
  END LOOP;
END$$;

-- 1) Per‑store per‑day sales (orders, units, revenue)
CREATE OR REPLACE VIEW public.v_sales_by_store_daily AS
WITH orders_mt AS (
  SELECT
    COALESCE(NULLIF(o.shop_domain,''),'(unknown)') AS shop_domain,
    (date_trunc('day', (o.placed_at AT TIME ZONE 'America/Denver')))::date AS day_mt,
    o.id,
    (o.total)::numeric(12,2) AS order_total
  FROM public.orders o
),
items AS (
  SELECT
    oi.order_id,
    SUM(COALESCE(oi.qty,0))::int AS units,
    SUM((COALESCE(oi.qty,0))::numeric * COALESCE(oi.unit_price,0::numeric))::numeric(12,2) AS item_revenue
  FROM public.order_items oi
  GROUP BY 1
)
SELECT
  om.shop_domain,
  om.day_mt,
  COUNT(*)::int                                  AS orders,
  COALESCE(SUM(i.units),0)::int                  AS units,
  COALESCE(SUM(i.item_revenue),0)::numeric(12,2) AS revenue
FROM orders_mt om
LEFT JOIN items i ON i.order_id = om.id
GROUP BY om.shop_domain, om.day_mt;

-- 2) Simple daily KPIs compatibility view (used by /api/kpis/daily)
CREATE OR REPLACE VIEW public.v_daily_kpis_by_store AS
SELECT shop_domain, day_mt, orders, units, revenue
FROM public.v_sales_by_store_daily;

-- 3) AOV by day (handy for dashboards that need it)
CREATE OR REPLACE VIEW public.v_aov_by_store_daily AS
SELECT
  shop_domain,
  day_mt,
  orders,
  revenue,
  CASE WHEN orders > 0 THEN (revenue / orders)::numeric(12,2) ELSE NULL END AS aov
FROM public.v_sales_by_store_daily;

-- 4) Rolling 7d / 30d per store (used by /api/kpis/rolling)
CREATE OR REPLACE VIEW public.v_kpis_rolling_7_30 AS
WITH base AS (
  SELECT shop_domain, day_mt, orders, units, revenue
  FROM public.v_daily_kpis_by_store
)
SELECT
  shop_domain,
  -- last 7 days
  SUM(revenue) FILTER (WHERE day_mt >= (CURRENT_DATE - INTERVAL '7 days'))::numeric(12,2)  AS revenue_7d,
  SUM(orders)  FILTER (WHERE day_mt >= (CURRENT_DATE - INTERVAL '7 days'))::int           AS orders_7d,
  SUM(units)   FILTER (WHERE day_mt >= (CURRENT_DATE - INTERVAL '7 days'))::int           AS units_7d,
  -- last 30 days
  SUM(revenue) FILTER (WHERE day_mt >= (CURRENT_DATE - INTERVAL '30 days'))::numeric(12,2) AS revenue_30d,
  SUM(orders)  FILTER (WHERE day_mt >= (CURRENT_DATE - INTERVAL '30 days'))::int           AS orders_30d,
  SUM(units)   FILTER (WHERE day_mt >= (CURRENT_DATE - INTERVAL '30 days'))::int           AS units_30d
FROM base
WHERE day_mt >= (CURRENT_DATE - INTERVAL '30 days')
GROUP BY shop_domain;

-- 5) Yesterday vs prior 7‑day avg (used by /api/kpis/summary)
CREATE OR REPLACE VIEW public.v_store_summary_yday_vs_prev7 AS
WITH base AS (
  SELECT * FROM public.v_sales_by_store_daily
),
yday AS (
  SELECT
    shop_domain,
    SUM(revenue)::numeric(12,2) AS yday_revenue,
    SUM(orders)::int            AS yday_orders,
    SUM(units)::int             AS yday_units
  FROM base
  WHERE day_mt = (CURRENT_DATE - INTERVAL '1 day')::date
  GROUP BY shop_domain
),
prev7 AS (
  SELECT
    shop_domain,
    SUM(revenue)::numeric(12,2) AS sum7_rev,
    SUM(orders)::int            AS sum7_ord,
    SUM(units)::int             AS sum7_units,
    COUNT(DISTINCT day_mt)::int AS days_seen
  FROM base
  WHERE day_mt BETWEEN (CURRENT_DATE - INTERVAL '8 days')::date
                    AND (CURRENT_DATE - INTERVAL '2 days')::date
  GROUP BY shop_domain
)
SELECT
  COALESCE(y.shop_domain, p.shop_domain)                    AS shop_domain,
  COALESCE(y.yday_revenue, 0)::numeric(12,2)                AS yday_revenue,
  COALESCE(y.yday_orders, 0)::int                           AS yday_orders,
  COALESCE(y.yday_units, 0)::int                            AS yday_units,
  -- average over the days we actually saw in the 7‑day window
  (COALESCE(p.sum7_rev,0)  / NULLIF(p.days_seen,0))::numeric(12,2) AS avg7_revenue,
  (COALESCE(p.sum7_ord,0)  / NULLIF(p.days_seen,0))::numeric(12,2) AS avg7_orders,
  (COALESCE(p.sum7_units,0)/ NULLIF(p.days_seen,0))::numeric(12,2) AS avg7_units,
  -- deltas and pct change (vs avg)
  (COALESCE(y.yday_revenue,0) - (COALESCE(p.sum7_rev,0)  / NULLIF(p.days_seen,0)))::numeric(12,2) AS delta_revenue,
  (COALESCE(y.yday_orders,0)  - (COALESCE(p.sum7_ord,0)  / NULLIF(p.days_seen,0)))::numeric(12,2) AS delta_orders,
  (COALESCE(y.yday_units,0)   - (COALESCE(p.sum7_units,0)/ NULLIF(p.days_seen,0)))::numeric(12,2) AS delta_units,
  CASE
    WHEN COALESCE(p.sum7_rev,0)  = 0 OR p.days_seen = 0 THEN NULL
    ELSE ((COALESCE(y.yday_revenue,0) / (p.sum7_rev / p.days_seen)) - 1)::numeric(8,4)
  END AS pct_revenue,
  CASE
    WHEN COALESCE(p.sum7_ord,0)  = 0 OR p.days_seen = 0 THEN NULL
    ELSE ((COALESCE(y.yday_orders,0)  / (p.sum7_ord / p.days_seen)) - 1)::numeric(8,4)
  END AS pct_orders,
  CASE
    WHEN COALESCE(p.sum7_units,0) = 0 OR p.days_seen = 0 THEN NULL
    ELSE ((COALESCE(y.yday_units,0)   / (p.sum7_units / p.days_seen)) - 1)::numeric(8,4)
  END AS pct_units
FROM yday y
FULL JOIN prev7 p ON p.shop_domain = y.shop_domain;