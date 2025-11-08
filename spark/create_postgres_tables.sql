-- Tạo tables để lưu kết quả phân tích từ Spark

-- Table 1: Product views daily
CREATE TABLE IF NOT EXISTS public.product_views_daily (
    product_id VARCHAR(50),
    view_count BIGINT,
    unique_visitors BIGINT,
    last_view_time VARCHAR(50),
    report_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, report_date)
);

CREATE INDEX IF NOT EXISTS idx_product_views_date ON public.product_views_daily(report_date);
CREATE INDEX IF NOT EXISTS idx_product_views_count ON public.product_views_daily(view_count DESC);

-- Table 2: Country views daily
CREATE TABLE IF NOT EXISTS public.country_views_daily (
    store_id VARCHAR(10),
    view_count BIGINT,
    unique_visitors BIGINT,
    report_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (store_id, report_date)
);

CREATE INDEX IF NOT EXISTS idx_country_views_date ON public.country_views_daily(report_date);

-- Table 3: Referrer views daily
CREATE TABLE IF NOT EXISTS public.referrer_views_daily (
    referrer_url TEXT,
    view_count BIGINT,
    report_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_referrer_views_date ON public.referrer_views_daily(report_date);
CREATE INDEX IF NOT EXISTS idx_referrer_views_count ON public.referrer_views_daily(view_count DESC);

-- View để xem top products
CREATE OR REPLACE VIEW public.top_products_today AS
SELECT 
    product_id,
    view_count,
    unique_visitors,
    last_view_time,
    report_date
FROM public.product_views_daily
WHERE report_date = CURRENT_DATE
ORDER BY view_count DESC
LIMIT 100;

-- View để xem top countries
CREATE OR REPLACE VIEW public.top_countries_today AS
SELECT 
    store_id,
    view_count,
    unique_visitors,
    report_date
FROM public.country_views_daily
WHERE report_date = CURRENT_DATE
ORDER BY view_count DESC;

-- View để xem top referrers
CREATE OR REPLACE VIEW public.top_referrers_today AS
SELECT 
    referrer_url,
    view_count,
    report_date
FROM public.referrer_views_daily
WHERE report_date = CURRENT_DATE
ORDER BY view_count DESC
LIMIT 50;
