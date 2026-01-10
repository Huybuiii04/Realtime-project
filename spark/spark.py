"""
Spark job để xử lý data từ MongoDB và lưu vào PostgreSQL
Được gọi từ Airflow DAG
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
        .appName("Kafka_MongoDB_to_PostgreSQL_Airflow") \
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
    import pandas as pd
    import json
    
    try:
        # Kết nối MongoDB
        # Use host.docker.internal to access host MongoDB from Docker
        mongo_host = os.getenv('MONGO_HOST', 'host.docker.internal')
        mongo_port = os.getenv('MONGO_PORT', '27017')
        client = MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
        db = client["kafka_data_db"]
        collection = db["product_views_records"]
        
        # Lấy tất cả documents
        documents = list(collection.find({}))  # Get all documents with _id
        
        if not documents:
            logging.warning("⚠️ No documents found in MongoDB collection")
            # Create an empty dataframe with a simple schema
            from pyspark.sql.types import StructType
            empty_schema = StructType([])
            return spark.createDataFrame([], schema=empty_schema)
        
        # Preprocess documents to handle type inconsistencies
        processed_docs = []
        for doc in documents:
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
            processed_docs.append(processed_doc)
        
        # Convert to pandas DataFrame first, then to Spark DataFrame
        pdf = pd.DataFrame(processed_docs)
        
        # Fill any remaining NaN values
        pdf = pdf.fillna("")
        
        # Convert to Spark DataFrame
        df = spark.createDataFrame(pdf)
        
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
        .withColumn("product_key", F.monotonically_increasing_id() + 1)
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
        .withColumn("country_key", F.monotonically_increasing_id() + 1)
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
        .withColumn("referrer_key", F.monotonically_increasing_id() + 1)
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
        .withColumn("device_key", F.monotonically_increasing_id() + 1)
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
        F.col("dp.product_key"),
        F.col("dc.country_key"),
        F.col("dr.referrer_key"),
        F.col("dv.device_key"),
        F.col("main.local_time"),
        F.col("main.device_id")
    ).groupBy(
        "date_key", "product_key", "country_key", "referrer_key", "device_key"
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

    # Truncate dimension tables first (to avoid foreign key conflicts)
    import psycopg2

    try:
        conn = psycopg2.connect(
            host="postgres",
            port="5432",
            database="postgres",
            user=properties["user"],
            password=properties["password"]
        )
        cursor = conn.cursor()

        # Truncate dimension tables (CASCADE will handle foreign keys)
        truncate_queries = [
            "TRUNCATE TABLE public.dim_date CASCADE;",
            "TRUNCATE TABLE public.dim_product CASCADE;",
            "TRUNCATE TABLE public.dim_country CASCADE;",
            "TRUNCATE TABLE public.dim_referrer CASCADE;",
            "TRUNCATE TABLE public.dim_device CASCADE;"
        ]

        for query in truncate_queries:
            cursor.execute(query)
            logging.info(f"✅ Executed: {query}")

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("✅ All dimension tables truncated")

    except Exception as e:
        logging.error(f"❌ Error truncating tables: {str(e)}")
        raise

    # Write Dimension tables (append mode after truncate)
    reports["dim_date"].write \
        .jdbc(url=jdbc_url, table="public.dim_date", mode="append", properties=properties)
    logging.info("✅ DIM_DATE written to PostgreSQL")

    reports["dim_product"].write \
        .jdbc(url=jdbc_url, table="public.dim_product", mode="append", properties=properties)
    logging.info("✅ DIM_PRODUCT written to PostgreSQL")

    reports["dim_country"].write \
        .jdbc(url=jdbc_url, table="public.dim_country", mode="append", properties=properties)
    logging.info("✅ DIM_COUNTRY written to PostgreSQL")

    reports["dim_referrer"].write \
        .jdbc(url=jdbc_url, table="public.dim_referrer", mode="append", properties=properties)
    logging.info("✅ DIM_REFERRER written to PostgreSQL")

    reports["dim_device"].write \
        .jdbc(url=jdbc_url, table="public.dim_device", mode="append", properties=properties)
    logging.info("✅ DIM_DEVICE written to PostgreSQL")

    # Write Fact table (append mode to accumulate data)
    reports["fact_product_views"].write \
        .jdbc(url=jdbc_url, table="public.fact_product_views", mode="append", properties=properties)
    logging.info("✅ FACT_PRODUCT_VIEWS written to PostgreSQL")


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
    
    
    
    
    
    
    