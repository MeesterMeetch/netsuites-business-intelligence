DO $$
DECLARE v text;
BEGIN
  -- List of root KPI views we manage in this repo
  FOR v IN 
    SELECT format('%I.%I', n.nspname, c.relname)
    FROM pg_class c 
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'v'
      AND c.relname = ANY (ARRAY[
        'v_sales_by_store_daily',
        'v_daily_kpis_by_store',
        'v_kpis_rolling_7_30',
        'v_store_summary_yday_vs_prev7',
        'v_aov_by_store_daily'   -- include this dependent so it gets recreated
      ])
  LOOP
    EXECUTE 'DROP VIEW IF EXISTS ' || v || ' CASCADE';
  END LOOP;
END$$;
