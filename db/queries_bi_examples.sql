-- db/queries_bi_examples.sql
-- Handy selects to run anytime

-- Daily sales by store (latest 14 days)
SELECT * FROM v_sales_by_store_daily
WHERE day >= (NOW() - INTERVAL '14 days')
ORDER BY day DESC, shop_domain;

-- Monthly sales by store (this year)
SELECT * FROM v_sales_by_store_monthly
WHERE date_part('year', month) = date_part('year', NOW())
ORDER BY month DESC, shop_domain;

-- Top SKUs (30d)
SELECT * FROM v_top_skus_30d LIMIT 50;

-- Customer repeat rates
SELECT * FROM v_customer_repeat_rates;

-- Cohort analysis (first 12 cohort x order months)
SELECT * FROM v_orders_cohort_monthly
ORDER BY cohort_month, order_month, shop_domain
LIMIT 200;
