"""
Spark job để xử lý data từ MongoDB và lưu vào PostgreSQL
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_spark_session():
    """Tạo Spark session với config MongoDB và PostgreSQL"""
    spark = SparkSession.builder \
        .appName("Kafka_MongoDB_to_PostgreSQL") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    logging.info("✅ Spark session created successfully")
    return spark


def read_from_mongodb(spark):
    """Đọc data từ MongoDB collection sử dụng PyMongo"""
    logging.info("📖 Reading data from MongoDB...")
    
    from pymongo import MongoClient
    import json
    
    try:
        # Kết nối MongoDB
        # Use host.docker.internal to access host MongoDB from Docker
        mongo_host = os.getenv('MONGO_HOST', 'host.docker.internal')
        mongo_port = os.getenv('MONGO_PORT', '27017')
        mongo_uri = os.getenv('MONGO_URI', f"mongodb://{mongo_host}:{mongo_port}/")
        mongo_db = os.getenv('MONGO_DB', 'kafka_data_db')
        mongo_collection = os.getenv('MONGO_COLLECTION', 'product_views_records')
        batch_size = int(os.getenv('MONGO_BATCH_SIZE', '5000'))

        client = MongoClient(mongo_uri)
        db = client[mongo_db]
        collection = db[mongo_collection]

        def normalize_doc(doc):
            processed_doc = {}
            for key, value in doc.items():
                if isinstance(value, (dict, list)):
                    # Convert dict or list to JSON string
                    processed_doc[key] = json.dumps(value)
                elif value is None:
                    # Convert None to empty string for consistency
                    processed_doc[key] = ""
                else:
                    # Keep other types as is
                    processed_doc[key] = value
            return processed_doc

        df = None
        batch = []

        for doc in collection.find({}).batch_size(batch_size):
            batch.append(normalize_doc(doc))
            if len(batch) >= batch_size:
                batch_df = spark.createDataFrame(batch)
                df = batch_df if df is None else df.unionByName(batch_df, allowMissingColumns=True)
                batch = []

        if batch:
            batch_df = spark.createDataFrame(batch)
            df = batch_df if df is None else df.unionByName(batch_df, allowMissingColumns=True)

        if df is None:
            logging.warning("⚠️ No documents found in MongoDB collection")
            from pyspark.sql.types import StructType
            empty_schema = StructType([])
            return spark.createDataFrame([], schema=empty_schema)

        df = df.na.fill("")
        count = df.count()
        logging.info(f"✅ Read {count} records from MongoDB")
        
        client.close()
        return df
        
    except Exception as e:
        logging.error(f"❌ Error reading from MongoDB: {str(e)}")
        raise


def process_data_dim_fact(df):
    """Transform data thành Dimension và Fact tables theo Star Schema"""
    logging.info("⚙️ Processing data into Dimension & Fact tables...")

    # Lấy ngày mới nhất từ data
    latest_date_row = df.select(F.max(F.substring("local_time", 1, 10)).alias("max_date")).collect()[0]
    report_date = latest_date_row["max_date"] if latest_date_row["max_date"] else datetime.now().strftime("%Y-%m-%d")

    logging.info(f"📅 Processing data for date: {report_date}")

    # ============================================================================
    # DIMENSION TABLES
    # ============================================================================

    # 1. DIM_DATE - Bảng dimension ngày
    dim_date = (
        df
        # 1. Parse date một lần từ local_time
        .withColumn(
            "date",
            F.to_date(F.substring("local_time", 1, 10))
        )
        # 2. Lọc những dòng có date hợp lệ
        .filter(F.col("date").isNotNull())
        # 3. Tạo các thuộc tính ngày
        .withColumn("year",        F.year("date"))
        .withColumn("month",       F.month("date"))
        .withColumn("day",         F.dayofmonth("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("week_of_year",F.weekofyear("date"))
        .withColumn("quarter",     F.quarter("date"))
        # 4. Tạo date_key dạng int: yyyyMMdd
        .withColumn(
            "date_key",
            F.date_format("date", "yyyyMMdd").cast("int")
        )
        # 5. Chỉ giữ các giá trị duy nhất
        .select(
            "date", "year", "month", "day",
            "day_of_week", "week_of_year", "quarter", "date_key"
        )
        .distinct()
    )

    logging.info(f"📅 DIM_DATE: {dim_date.count()} dates")

    # 2. DIM_PRODUCT - Bảng dimension sản phẩm
    dim_product = (
        df
        .select("product_id")
        .filter(F.col("product_id").isNotNull())
        .distinct()
        .withColumn("product_key", F.pmod(F.xxhash64("product_id"), F.lit(9223372036854775807)) + 1)
        .withColumn("product_name", F.concat(F.lit("Product "), F.col("product_id")))
        .withColumn("created_date", F.current_date())
        .withColumn("is_active", F.lit(True))
    )

    logging.info(f"📦 DIM_PRODUCT: {dim_product.count()} products")

    # 3. DIM_COUNTRY - Bảng dimension quốc gia
    dim_country = (
        df
        .select("store_id")
        .filter(F.col("store_id").isNotNull())
        .distinct()
        .withColumn("country_key", F.pmod(F.xxhash64("store_id"), F.lit(9223372036854775807)) + 1)
        .withColumn("country_name", F.concat(F.lit("Country "), F.col("store_id")))
        .withColumn("region", F.lit("Unknown"))
        .withColumn("created_date", F.current_date())
    )

    logging.info(f"🌍 DIM_COUNTRY: {dim_country.count()} countries")

    # 4. DIM_REFERRER - Bảng dimension referrer
    dim_referrer = (
        df
        .select("referrer_url")
        .filter(F.col("referrer_url").isNotNull())
        .distinct()
        .withColumn("referrer_key", F.pmod(F.xxhash64("referrer_url"), F.lit(9223372036854775807)) + 1)
        .withColumn("referrer_hash", F.sha2(F.col("referrer_url"), 256))
        .withColumn("referrer_domain",
            F.regexp_extract(F.col("referrer_url"), r"https?://([^/]+)", 1))
        .withColumn("referrer_type",
            F.when(F.col("referrer_url").contains("google"), "Search Engine")
             .when(F.col("referrer_url").contains("facebook"), "Social Media")
             .when(F.col("referrer_url").contains("direct"), "Direct")
             .otherwise("Other"))
        .withColumn("created_date", F.current_date())
    )

    logging.info(f"🔗 DIM_REFERRER: {dim_referrer.count()} referrers")

    # 5. DIM_DEVICE - Bảng dimension thiết bị
    dim_device = (
        df
        .select("device_id")
        .filter(F.col("device_id").isNotNull())
        .distinct()
        .withColumn("device_key", F.pmod(F.xxhash64("device_id"), F.lit(9223372036854775807)) + 1)
        .withColumn("device_type", F.lit("Unknown"))
        .withColumn("browser_info", F.lit("Unknown"))
        .withColumn("created_date", F.current_date())
    )

    logging.info(f"📱 DIM_DEVICE: {dim_device.count()} devices")

    # ============================================================================
    # FACT TABLE
    # ============================================================================

    # FACT_PRODUCT_VIEWS - Bảng fact chính
    fact_product_views = df.filter(
        F.col("local_time").startswith(report_date) #chi xu ly data ngày mới nhất
    ).alias("main").join(dim_date.alias("dd"),
        F.to_date(F.substring(F.col("main.local_time"), 1, 10)) == F.col("dd.date"),
        "left"
    ).join(dim_product.alias("dp"),
        F.col("main.product_id") == F.col("dp.product_id"),
        "left"
    ).join(dim_country.alias("dc"),
        F.col("main.store_id") == F.col("dc.store_id"),
        "left"
    ).join(dim_referrer.alias("dr"),
        F.col("main.referrer_url") == F.col("dr.referrer_url"),
        "left"
    ).join(dim_device.alias("dv"),
        F.col("main.device_id") == F.col("dv.device_id"),
        "left"
    ).select(
        F.col("dd.date_key"),
        F.col("main.product_id"),
        F.col("main.store_id"),
        F.col("main.referrer_url"),
        F.col("main.device_id"),
        F.col("main.local_time"),
    ).groupBy(
        "date_key", "product_id", "store_id", "referrer_url", "device_id"
    ).agg(
        F.count("*").alias("view_count"),
        F.countDistinct("device_id").alias("unique_visitors"),
        F.max(F.to_timestamp(F.regexp_replace("local_time", "T", " "))).alias("last_view_time"),
        F.min(F.to_timestamp(F.regexp_replace("local_time", "T", " "))).alias("first_view_time"),
        F.avg(F.unix_timestamp(F.to_timestamp(F.regexp_replace("local_time", "T", " ")))).alias("avg_view_timestamp")
    ).withColumn("view_duration_seconds",
        F.unix_timestamp("last_view_time") - F.unix_timestamp("first_view_time")
    ).withColumn("processed_at", F.current_timestamp())

    logging.info(f"📊 FACT_PRODUCT_VIEWS: {fact_product_views.count()} fact records")

    return {
        "dim_date": dim_date,
        "dim_product": dim_product,
        "dim_country": dim_country,
        "dim_referrer": dim_referrer,
        "dim_device": dim_device,
        "fact_product_views": fact_product_views
    }


def write_to_postgres(reports, jdbc_url, properties):
    """Ghi kết quả Dimension & Fact tables vào PostgreSQL"""
    logging.info("💾 Writing Dimension & Fact tables to PostgreSQL...")

    import psycopg2

    staging_tables = {
        "dim_date": "public.stg_dim_date",
        "dim_product": "public.stg_dim_product",
        "dim_country": "public.stg_dim_country",
        "dim_referrer": "public.stg_dim_referrer",
        "dim_device": "public.stg_dim_device",
        "fact_product_views": "public.stg_fact_product_views",
    }

    for report_name, table_name in staging_tables.items():
        reports[report_name].write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="overwrite",
            properties=properties,
        )
        logging.info(f"✅ {report_name} written to staging table {table_name}")

    upsert_queries = [
        """
        INSERT INTO public.dim_date (date_key, date, year, month, day, day_of_week, week_of_year, quarter)
        SELECT date_key, date, year, month, day, day_of_week, week_of_year, quarter
        FROM public.stg_dim_date
        ON CONFLICT (date_key) DO UPDATE SET
            date = EXCLUDED.date,
            year = EXCLUDED.year,
            month = EXCLUDED.month,
            day = EXCLUDED.day,
            day_of_week = EXCLUDED.day_of_week,
            week_of_year = EXCLUDED.week_of_year,
            quarter = EXCLUDED.quarter;
        """,
        """
        INSERT INTO public.dim_product (product_key, product_id, product_name, created_date, is_active)
        SELECT product_key, product_id, product_name, created_date, is_active
        FROM public.stg_dim_product
        ON CONFLICT (product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            created_date = EXCLUDED.created_date,
            is_active = EXCLUDED.is_active,
            updated_at = CURRENT_TIMESTAMP;
        """,
        """
        INSERT INTO public.dim_country (country_key, store_id, country_name, region, created_date)
        SELECT country_key, store_id, country_name, region, created_date
        FROM public.stg_dim_country
        ON CONFLICT (store_id) DO UPDATE SET
            country_name = EXCLUDED.country_name,
            region = EXCLUDED.region,
            created_date = EXCLUDED.created_date,
            updated_at = CURRENT_TIMESTAMP;
        """,
        """
        INSERT INTO public.dim_referrer (referrer_key, referrer_url, referrer_hash, referrer_domain, referrer_type, created_date)
        SELECT referrer_key, referrer_url, referrer_hash, referrer_domain, referrer_type, created_date
        FROM public.stg_dim_referrer
        ON CONFLICT (referrer_hash) DO UPDATE SET
            referrer_url = EXCLUDED.referrer_url,
            referrer_domain = EXCLUDED.referrer_domain,
            referrer_type = EXCLUDED.referrer_type,
            created_date = EXCLUDED.created_date,
            updated_at = CURRENT_TIMESTAMP;
        """,
        """
        INSERT INTO public.dim_device (device_key, device_id, device_type, browser_info, created_date)
        SELECT device_key, device_id, device_type, browser_info, created_date
        FROM public.stg_dim_device
        ON CONFLICT (device_id) DO UPDATE SET
            device_type = EXCLUDED.device_type,
            browser_info = EXCLUDED.browser_info,
            created_date = EXCLUDED.created_date,
            updated_at = CURRENT_TIMESTAMP;
        """,
        """
        INSERT INTO public.fact_product_views (
            date_key, product_key, country_key, referrer_key, device_key,
            view_count, unique_visitors, last_view_time, first_view_time,
            avg_view_timestamp, view_duration_seconds, processed_at
        )
        SELECT
            sf.date_key, dp.product_key, dc.country_key, dr.referrer_key, dv.device_key,
            sf.view_count, sf.unique_visitors, sf.last_view_time, sf.first_view_time,
            sf.avg_view_timestamp, sf.view_duration_seconds, sf.processed_at
        FROM public.stg_fact_product_views sf
        JOIN public.dim_product dp ON sf.product_id = dp.product_id
        LEFT JOIN public.dim_country dc ON sf.store_id = dc.store_id
        LEFT JOIN public.dim_referrer dr ON encode(digest(sf.referrer_url, 'sha256'), 'hex') = dr.referrer_hash
        LEFT JOIN public.dim_device dv ON sf.device_id = dv.device_id
        ON CONFLICT (date_key, product_key, country_key, referrer_key, device_key) DO UPDATE SET
            view_count = EXCLUDED.view_count,
            unique_visitors = EXCLUDED.unique_visitors,
            last_view_time = EXCLUDED.last_view_time,
            first_view_time = EXCLUDED.first_view_time,
            avg_view_timestamp = EXCLUDED.avg_view_timestamp,
            view_duration_seconds = EXCLUDED.view_duration_seconds,
            processed_at = EXCLUDED.processed_at;
        """,
    ]

    try:
        conn = psycopg2.connect(
            host="postgres",
            port="5432",
            database="postgres",
            user=properties["user"],
            password=properties["password"]
        )
        cursor = conn.cursor()

        for query in upsert_queries:
            cursor.execute(query)

        for table_name in staging_tables.values():
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("✅ Dimension and fact tables upserted")

    except Exception as e:
        logging.error(f"❌ Error upserting tables: {str(e)}")
        raise


def main():
    """Main execution"""
    
    logging.info("🚀 Starting Spark MongoDB → PostgreSQL Processing")
    
    
    # PostgreSQL connection config
    jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
    properties = {
        "user": "postgres",
        "password": "UnigapPostgres@123",
        "driver": "org.postgresql.Driver"
    }
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Read from MongoDB
        df = read_from_mongodb(spark)
        
        if df.count() == 0:
            logging.warning("⚠️ No data found in MongoDB. Skipping processing.")
            return
        
        # Process data into Dim/Fact tables
        reports = process_data_dim_fact(df)
        
        # Write to PostgreSQL
        write_to_postgres(reports, jdbc_url, properties)
        
        
        logging.info("✅ Spark job completed successfully!")
        
        
    except Exception as e:
        logging.error(f"❌ Error in Spark job: {e}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logging.info("🔌 Spark session stopped")


if __name__ == "__main__":
    main()
    
    
    
    
    
    
    
