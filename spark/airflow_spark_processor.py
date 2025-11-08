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
        .master("spark://spark:7077") \
        .config("spark.mongodb.read.connection.uri", "mongodb://mongo:27017/kafka_data_db.product_views_records") \
        .config("spark.mongodb.write.connection.uri", "mongodb://mongo:27017/kafka_data_db") \
        .config("spark.jars.packages", 
                "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1,"
                "org.postgresql:postgresql:42.7.4") \
        .getOrCreate()
    
    logging.info("✅ Spark session created successfully")
    return spark


def read_from_mongodb(spark):
    """Đọc data từ MongoDB collection"""
    logging.info("📖 Reading data from MongoDB...")
    
    df = spark.read \
        .format("mongodb") \
        .option("uri", "mongodb://mongo:27017/kafka_data_db.product_views_records") \
        .option("database", "kafka_data_db") \
        .option("collection", "product_views_records") \
        .load()
    
    count = df.count()
    logging.info(f"✅ Read {count} records from MongoDB")
    
    return df


def process_data(df):
    """Xử lý và aggregate data"""
    logging.info("⚙️ Processing data...")
    
    # Get the latest date in the dataset
    latest_date_row = df.select(F.max(F.substring("local_time", 1, 10)).alias("max_date")).collect()[0]
    report_date = latest_date_row["max_date"] if latest_date_row["max_date"] else datetime.now().strftime("%Y-%m-%d")
    
    logging.info(f"📅 Processing data for date: {report_date}")
    
    # Report 1: View count by product_id (for the latest date in data)
    product_views_today = df.filter(
        F.col("local_time").startswith(report_date)
    ).groupBy("product_id") \
     .agg(
         F.count("*").alias("view_count"),
         F.countDistinct("device_id").alias("unique_visitors"),
         F.max("local_time").alias("last_view_time")
     ).filter(F.col("product_id").isNotNull()) \
     .withColumn("report_date", F.to_date(F.lit(report_date)))
    
    logging.info(f"📊 Product views for {report_date}: {product_views_today.count()} products")
    
    # Report 2: View count by country (store_id)
    country_views_today = df.filter(
        F.col("local_time").startswith(report_date)
    ).groupBy("store_id") \
     .agg(
         F.count("*").alias("view_count"),
         F.countDistinct("device_id").alias("unique_visitors")
     ).filter(F.col("store_id").isNotNull()) \
     .withColumn("report_date", F.to_date(F.lit(report_date)))
    
    logging.info(f"🌍 Country views for {report_date}: {country_views_today.count()} countries")
    
    # Report 3: View count by referrer
    referrer_views_today = df.filter(
        F.col("local_time").startswith(report_date)
    ).groupBy("referrer_url") \
     .agg(
         F.count("*").alias("view_count")
     ).filter(F.col("referrer_url").isNotNull()) \
     .withColumn("report_date", F.to_date(F.lit(report_date))) \
      .orderBy(F.desc("view_count")) \
      .limit(100)  # Top 100 referrers
    
    logging.info(f"🔗 Top referrers for {report_date}: {referrer_views_today.count()} sources")
    
    return {
        "product_views": product_views_today,
        "country_views": country_views_today,
        "referrer_views": referrer_views_today
    }


def write_to_postgres(reports, jdbc_url, properties):
    """Ghi kết quả vào PostgreSQL"""
    logging.info("💾 Writing results to PostgreSQL...")
    
    # Write product views
    reports["product_views"].write \
        .jdbc(url=jdbc_url, table="public.product_views_daily", mode="append", properties=properties)
    logging.info("✅ Product views written to PostgreSQL")
    
    # Write country views
    reports["country_views"].write \
        .jdbc(url=jdbc_url, table="public.country_views_daily", mode="append", properties=properties)
    logging.info("✅ Country views written to PostgreSQL")
    
    # Write referrer views
    reports["referrer_views"].write \
        .jdbc(url=jdbc_url, table="public.referrer_views_daily", mode="append", properties=properties)
    logging.info("✅ Referrer views written to PostgreSQL")


def main():
    """Main execution"""
    logging.info("=" * 80)
    logging.info("🚀 Starting Spark MongoDB → PostgreSQL Processing")
    logging.info("=" * 80)
    
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
        
        # Process data
        reports = process_data(df)
        
        # Write to PostgreSQL
        write_to_postgres(reports, jdbc_url, properties)
        
        logging.info("=" * 80)
        logging.info("✅ Spark job completed successfully!")
        logging.info("=" * 80)
        
    except Exception as e:
        logging.error(f"❌ Error in Spark job: {e}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logging.info("🔌 Spark session stopped")


if __name__ == "__main__":
    main()
