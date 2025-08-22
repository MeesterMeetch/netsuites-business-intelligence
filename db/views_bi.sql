-- 1) daily sales by store
CREATE OR REPLACE VIEW v_sales_by_store_daily AS
SELECT
  DATE_TRUNC('day', o.placed_at)                          AS day,
  COALESCE(NULLIF(o.shop_domain, ''), '(unknown)')        AS shop_domain,
  COUNT(*)                                                AS orders,
  SUM(o.total)::numeric(12,2)                             AS revenue
FROM orders o
GROUP BY 1,2
ORDER BY 1 DESC, 2;

-- 2) top SKUs last 30 days
CREATE OR REPLACE VIEW v_top_skus_30d AS
WITH src AS (
  SELECT
    oi.sku,
    COALESCE(oi.title, oi.sku, 'Unknown') AS title,
    o.shop_domain,
    COALESCE(oi.quantity,0)::int          AS qty,
    COALESCE(oi.total,
             (COALESCE(oi.price,0)::numeric * COALESCE(oi.quantity,0)),
             0)::numeric(12,2)            AS item_revenue,
    o.placed_at
  FROM order_items oi
  JOIN orders o ON o.id = oi.order_id
  WHERE o.placed_at >= (NOW() - INTERVAL '30 days')
)
SELECT
  shop_domain,
  sku,
  title,
  SUM(qty)                           AS units,
  SUM(item_revenue)::numeric(12,2)   AS revenue
FROM src
GROUP BY 1,2,3
HAVING SUM(qty) > 0 OR SUM(item_revenue) > 0
ORDER BY revenue DESC, units DESC;

-- 3) customer repeat rates (uses orders.customer_id if present)
CREATE OR REPLACE VIEW v_customer_repeat_rates AS
WITH per_customer AS (
  SELECT
    o.shop_domain,
    o.customer_id,
    COUNT(*)                          AS order_count,
    MIN(o.placed_at)                  AS first_order_at,
    MAX(o.placed_at)                  AS last_order_at,
    SUM(o.total)::numeric(12,2)       AS lifetime_value
  FROM orders o
  GROUP BY 1,2
),
totals AS (
  SELECT
    shop_domain,
    COUNT(*) FILTER (WHERE order_count = 1)  AS one_time_customers,
    COUNT(*) FILTER (WHERE order_count >= 2) AS repeat_customers,
    COUNT(*)                                 AS customers_total
  FROM per_customer
  GROUP BY 1
)
SELECT
  shop_domain,
  customers_total,
  one_time_customers,
  repeat_customers,
  ROUND(100.0 * repeat_customers / NULLIF(customers_total,0), 2) AS repeat_rate_pct,
  ROUND( (SELECT AVG(lifetime_value)
          FROM per_customer pc
          WHERE pc.shop_domain=t.shop_domain AND pc.order_count >= 2)
        ,2) AS avg_ltv_repeat
FROM totals t
ORDER BY repeat_rate_pct DESC NULLS LAST;

-- 4) monthly order cohorts (first purchase month)
CREATE OR REPLACE VIEW v_orders_cohort_monthly AS
WITH firsts AS (
  SELECT
    COALESCE(NULLIF(o.shop_domain,''), '(unknown)') AS shop_domain,
    o.customer_id,
    DATE_TRUNC('month', MIN(o.placed_at)) AS cohort_month
  FROM orders o
  GROUP BY 1,2
),
orders_by_month AS (
  SELECT
    COALESCE(NULLIF(o.shop_domain,''), '(unknown)') AS shop_domain,
    o.customer_id,
    DATE_TRUNC('month', o.placed_at) AS order_month,
    SUM(o.total)::numeric(12,2)      AS revenue
  FROM orders o
  GROUP BY 1,2,3
)
SELECT
  f.shop_domain,
  f.cohort_month,
  obm.order_month,
  COUNT(*)                        AS orders,
  SUM(obm.revenue)::numeric(12,2) AS revenue
FROM firsts f
JOIN orders_by_month obm
  ON obm.shop_domain = f.shop_domain
 AND obm.customer_id = f.customer_id
GROUP BY 1,2,3
ORDER BY f.cohort_month DESC, obm.order_month DESC, f.shop_domain;
