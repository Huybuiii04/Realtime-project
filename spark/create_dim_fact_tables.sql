-- SQL Script để tạo Dimension & Fact tables cho Data Warehouse
-- Chạy script này trong PostgreSQL trước khi chạy Spark job

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- DIM_DATE: Bảng dimension cho ngày
CREATE TABLE IF NOT EXISTS public.dim_date (
    date_key INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DIM_PRODUCT: Bảng dimension cho sản phẩm
CREATE TABLE IF NOT EXISTS public.dim_product (
    product_key BIGINT PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    product_name VARCHAR(500),
    category VARCHAR(255),
    price DECIMAL(10,2),
    is_active BOOLEAN DEFAULT TRUE,
    created_date DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id)
);

-- DIM_COUNTRY: Bảng dimension cho quốc gia
CREATE TABLE IF NOT EXISTS public.dim_country (
    country_key BIGINT PRIMARY KEY,
    store_id VARCHAR(255) NOT NULL,
    country_name VARCHAR(255),
    country_code VARCHAR(10),
    region VARCHAR(255),
    continent VARCHAR(255),
    created_date DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(store_id)
);

-- DIM_REFERRER: Bảng dimension cho referrer
CREATE TABLE IF NOT EXISTS public.dim_referrer (
    referrer_key BIGINT PRIMARY KEY,
    referrer_url TEXT NOT NULL,
    referrer_domain VARCHAR(500),
    referrer_type VARCHAR(255),
    is_paid_traffic BOOLEAN DEFAULT FALSE,
    created_date DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(referrer_url)
);

-- DIM_DEVICE: Bảng dimension cho thiết bị
CREATE TABLE IF NOT EXISTS public.dim_device (
    device_key BIGINT PRIMARY KEY,
    device_id VARCHAR(255) NOT NULL,
    device_type VARCHAR(255),
    browser_info TEXT,
    os_info VARCHAR(255),
    is_mobile BOOLEAN,
    created_date DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(device_id)
);

-- ============================================================================
-- FACT TABLE
-- ============================================================================

-- FACT_PRODUCT_VIEWS: Bảng fact chính cho product views
CREATE TABLE IF NOT EXISTS public.fact_product_views (
    fact_key BIGSERIAL PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES public.dim_date(date_key),
    product_key BIGINT NOT NULL REFERENCES public.dim_product(product_key),
    country_key BIGINT REFERENCES public.dim_country(country_key),
    referrer_key BIGINT REFERENCES public.dim_referrer(referrer_key),
    device_key BIGINT REFERENCES public.dim_device(device_key),

    -- Measures
    view_count INTEGER NOT NULL DEFAULT 0,
    unique_visitors INTEGER NOT NULL DEFAULT 0,
    view_duration_seconds INTEGER,
    avg_view_timestamp DOUBLE PRECISION,

    -- Timestamps
    first_view_time TIMESTAMP,
    last_view_time TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Metadata
    data_source VARCHAR(255) DEFAULT 'kafka_mongodb',
    batch_id VARCHAR(255),

    UNIQUE(date_key, product_key, country_key, referrer_key, device_key)
);

-- ============================================================================
-- INDEXES for Performance
-- ============================================================================

-- Fact table indexes
CREATE INDEX IF NOT EXISTS idx_fact_product_views_date ON public.fact_product_views(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_product_views_product ON public.fact_product_views(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_product_views_country ON public.fact_product_views(country_key);
CREATE INDEX IF NOT EXISTS idx_fact_product_views_referrer ON public.fact_product_views(referrer_key);
CREATE INDEX IF NOT EXISTS idx_fact_product_views_device ON public.fact_product_views(device_key);
CREATE INDEX IF NOT EXISTS idx_fact_product_views_processed_at ON public.fact_product_views(processed_at);

-- Dimension table indexes
CREATE INDEX IF NOT EXISTS idx_dim_product_product_id ON public.dim_product(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_country_store_id ON public.dim_country(store_id);
CREATE INDEX IF NOT EXISTS idx_dim_referrer_url ON public.dim_referrer(referrer_url);
CREATE INDEX IF NOT EXISTS idx_dim_device_device_id ON public.dim_device(device_id);

-- ============================================================================
-- SAMPLE QUERIES
-- ============================================================================

/*
-- Query mẫu: Tổng view theo ngày và sản phẩm
SELECT
    dd.date,
    dp.product_name,
    SUM(fpv.view_count) as total_views,
    SUM(fpv.unique_visitors) as total_unique_visitors
FROM public.fact_product_views fpv
JOIN public.dim_date dd ON fpv.date_key = dd.date_key
JOIN public.dim_product dp ON fpv.product_key = dp.product_key
GROUP BY dd.date, dp.product_name
ORDER BY dd.date DESC, total_views DESC;

-- Query mẫu: Top countries theo views
SELECT
    dc.country_name,
    dc.region,
    SUM(fpv.view_count) as total_views
FROM public.fact_product_views fpv
JOIN public.dim_country dc ON fpv.country_key = dc.country_key
GROUP BY dc.country_name, dc.region
ORDER BY total_views DESC
LIMIT 10;

-- Query mẫu: Traffic source analysis
SELECT
    dr.referrer_type,
    dr.referrer_domain,
    SUM(fpv.view_count) as total_views,
    COUNT(DISTINCT fpv.device_key) as unique_devices
FROM public.fact_product_views fpv
JOIN public.dim_referrer dr ON fpv.referrer_key = dr.referrer_key
GROUP BY dr.referrer_type, dr.referrer_domain
ORDER BY total_views DESC;
*/