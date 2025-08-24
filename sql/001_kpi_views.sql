-- ============================================================================
-- KPI compat bundle (idempotent)
-- Creates daily KPI views + Top/Bottom SKU functions with optional 365d fields
-- ============================================================================

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_orders_shop_day
  ON orders (shop_domain, (date_trunc('day', placed_at)));

-- =========================
-- v_sales_by_store_daily
-- =========================
CREATE OR REPLACE VIEW v_sales_by_store_daily AS
SELECT
  (date_trunc('day', placed_at AT TIME ZONE 'America/Denver'))::date AS day_mt,
  COALESCE(shop_domain,'(unknown)') AS shop_domain,
  COUNT(*)::int AS orders,
  SUM(COALESCE(total,0))::numeric(20,2) AS revenue
FROM orders
GROUP BY 1,2;

-- =========================
-- v_daily_kpis_by_store
-- =========================
CREATE OR REPLACE VIEW v_daily_kpis_by_store AS
WITH it AS (
  SELECT
    (date_trunc('day', o.placed_at AT TIME ZONE 'America/Denver'))::date AS day_mt,
    COALESCE(o.shop_domain,'(unknown)') AS shop_domain,
    oi.qty::int AS units,
    (COALESCE(oi.qty,0) * COALESCE(oi.unit_price,0)
      - COALESCE(oi.discount,0)
      + COALESCE(oi.tax,0)
      + COALESCE(oi.fees,0)
    )::numeric(20,2) AS line_total
  FROM order_items oi
  JOIN orders o ON o.id = oi.order_id
)
SELECT
  day_mt,
  shop_domain,
  COUNT(*) FILTER (WHERE units IS NOT NULL)::int AS items,
  COALESCE(SUM(units),0)::int AS units,
  COALESCE(SUM(line_total),0)::numeric(20,2) AS revenue
FROM it
GROUP BY 1,2;

-- ======================================================
-- Top/Bottom SKU compatibility functions (window + 365)
-- ======================================================

-- Drop to avoid "cannot change return type of existing function" on re-run
DROP FUNCTION IF EXISTS v_top_skus_window(integer);
DROP FUNCTION IF EXISTS v_bottom_skus_window(integer);

-- ---------- v_top_skus_window(p_days) ----------
CREATE OR REPLACE FUNCTION v_top_skus_window(p_days integer)
RETURNS TABLE (
  sku text,
  title text,
  shop_domain text,
  units_window integer,
  revenue_window numeric,
  units_365 integer,
  revenue_365 numeric
)
LANGUAGE sql
STABLE
AS $$
WITH w AS (
  SELECT
    oi.sku,
    COALESCE(oi.title,'') AS title,
    o.shop_domain,
    SUM(oi.qty)::int AS units_window,
    SUM( (COALESCE(oi.qty,0) * COALESCE(oi.unit_price,0)
        - COALESCE(oi.discount,0)
        + COALESCE(oi.tax,0)
        + COALESCE(oi.fees,0)) )::numeric(20,2) AS revenue_window
  FROM order_items oi
  JOIN orders o ON o.id = oi.order_id
  WHERE o.placed_at >= (CURRENT_DATE - make_interval(days => p_days))
  GROUP BY 1,2,3
),
t365 AS (
  SELECT
    oi.sku,
    COALESCE(oi.title,'') AS title,
    o.shop_domain,
    SUM(oi.qty)::int AS units_365,
    SUM( (COALESCE(oi.qty,0) * COALESCE(oi.unit_price,0)
        - COALESCE(oi.discount,0)
        + COALESCE(oi.tax,0)
        + COALESCE(oi.fees,0)) )::numeric(20,2) AS revenue_365
  FROM order_items oi
  JOIN orders o ON o.id = oi.order_id
  WHERE o.placed_at >= (CURRENT_DATE - INTERVAL '365 days')
  GROUP BY 1,2,3
)
SELECT
  w.sku, w.title, w.shop_domain,
  w.units_window, w.revenue_window,
  COALESCE(t365.units_365,0)::int       AS units_365,
  COALESCE(t365.revenue_365,0)::numeric AS revenue_365
FROM w
LEFT JOIN t365
  ON t365.sku = w.sku
 AND t365.shop_domain = w.shop_domain
ORDER BY w.revenue_window DESC NULLS LAST
LIMIT 1000;
$$;

-- ---------- v_bottom_skus_window(p_days) ----------
CREATE OR REPLACE FUNCTION v_bottom_skus_window(p_days integer)
RETURNS TABLE (
  sku text,
  title text,
  shop_domain text,
  units_window integer,
  revenue_window numeric,
  units_365 integer,
  revenue_365 numeric
)
LANGUAGE sql
STABLE
AS $$
WITH w AS (
  SELECT
    oi.sku,
    COALESCE(oi.title,'') AS title,
    o.shop_domain,
    SUM(oi.qty)::int AS units_window,
    SUM( (COALESCE(oi.qty,0) * COALESCE(oi.unit_price,0)
        - COALESCE(oi.discount,0)
        + COALESCE(oi.tax,0)
        + COALESCE(oi.fees,0)) )::numeric(20,2) AS revenue_window
  FROM order_items oi
  JOIN orders o ON o.id = oi.order_id
  WHERE o.placed_at >= (CURRENT_DATE - make_interval(days => p_days))
  GROUP BY 1,2,3
),
t365 AS (
  SELECT
    oi.sku,
    COALESCE(oi.title,'') AS title,
    o.shop_domain,
    SUM(oi.qty)::int AS units_365,
    SUM( (COALESCE(oi.qty,0) * COALESCE(oi.unit_price,0)
        - COALESCE(oi.discount,0)
        + COALESCE(oi.fees,0)) )::numeric(20,2) AS revenue_365
  FROM order_items oi
  JOIN orders o ON o.id = oi.order_id
  WHERE o.placed_at >= (CURRENT_DATE - INTERVAL '365 days')
  GROUP BY 1,2,3
)
SELECT
  w.sku, w.title, w.shop_domain,
  w.units_window, w.revenue_window,
  COALESCE(t365.units_365,0)::int       AS units_365,
  COALESCE(t365.revenue_365,0)::numeric AS revenue_365
FROM w
LEFT JOIN t365
  ON t365.sku = w.sku
 AND t365.shop_domain = w.shop_domain
ORDER BY w.revenue_window ASC NULLS LAST, w.units_window ASC NULLS LAST
LIMIT 1000;
$$;
