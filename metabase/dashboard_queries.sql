-- Product View Analytics dashboard queries for Metabase.
-- Connect Metabase to PostgreSQL: host=postgres, port=5432, database=postgres.

-- 1. KPI summary
SELECT
  SUM(view_count) AS total_views,
  SUM(unique_visitors) AS total_unique_visitors,
  COUNT(*) AS fact_rows,
  MAX(processed_at) AS last_processed_at
FROM public.fact_product_views;

-- 2. Views by date
SELECT
  dd.date,
  SUM(fpv.view_count) AS total_views,
  SUM(fpv.unique_visitors) AS unique_visitors
FROM public.fact_product_views fpv
JOIN public.dim_date dd ON fpv.date_key = dd.date_key
GROUP BY dd.date
ORDER BY dd.date;

-- 3. Top products by views
SELECT
  dp.product_id,
  dp.product_name,
  SUM(fpv.view_count) AS total_views,
  SUM(fpv.unique_visitors) AS unique_visitors
FROM public.fact_product_views fpv
JOIN public.dim_product dp ON fpv.product_key = dp.product_key
GROUP BY dp.product_id, dp.product_name
ORDER BY total_views DESC
LIMIT 20;

-- 4. Top stores by views
SELECT
  dc.store_id,
  dc.country_name,
  SUM(fpv.view_count) AS total_views,
  SUM(fpv.unique_visitors) AS unique_visitors
FROM public.fact_product_views fpv
LEFT JOIN public.dim_country dc ON fpv.country_key = dc.country_key
GROUP BY dc.store_id, dc.country_name
ORDER BY total_views DESC
LIMIT 20;

-- 5. Referrer type breakdown
SELECT
  dr.referrer_type,
  SUM(fpv.view_count) AS total_views,
  SUM(fpv.unique_visitors) AS unique_visitors
FROM public.fact_product_views fpv
LEFT JOIN public.dim_referrer dr ON fpv.referrer_key = dr.referrer_key
GROUP BY dr.referrer_type
ORDER BY total_views DESC;

-- 6. Top referrer domains
SELECT
  NULLIF(dr.referrer_domain, '') AS referrer_domain,
  SUM(fpv.view_count) AS total_views
FROM public.fact_product_views fpv
LEFT JOIN public.dim_referrer dr ON fpv.referrer_key = dr.referrer_key
GROUP BY NULLIF(dr.referrer_domain, '')
ORDER BY total_views DESC
LIMIT 20;

-- 7. Device traffic
SELECT
  ddv.device_type,
  SUM(fpv.view_count) AS total_views,
  SUM(fpv.unique_visitors) AS unique_visitors
FROM public.fact_product_views fpv
LEFT JOIN public.dim_device ddv ON fpv.device_key = ddv.device_key
GROUP BY ddv.device_type
ORDER BY total_views DESC;

-- 8. Product/store matrix
SELECT
  dp.product_id,
  dc.store_id,
  SUM(fpv.view_count) AS total_views
FROM public.fact_product_views fpv
JOIN public.dim_product dp ON fpv.product_key = dp.product_key
LEFT JOIN public.dim_country dc ON fpv.country_key = dc.country_key
GROUP BY dp.product_id, dc.store_id
ORDER BY total_views DESC
LIMIT 50;
