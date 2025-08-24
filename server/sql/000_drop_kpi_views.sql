DO $$
DECLARE
  v text;
BEGIN
  -- List of KPI views to drop (and any dependents) so we can recreate them
  FOR v IN
    SELECT format('%I.%I', n.nspname, c.relname) AS qname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'v'
      AND n.nspname = 'public'
      AND c.relname IN (
        'v_sales_by_store_daily',
        'v_daily_kpis_by_store',
        'v_kpis_rolling_7_30',
        'v_store_summary_yday_vs_prev7',
        'v_aov_by_store_daily'   -- include this dependent so it gets recreated
      )
  LOOP
    EXECUTE format('DROP VIEW IF EXISTS %s CASCADE', v);
  END LOOP;
END$$;