"""
Producer: Bridge data từ Remote Kafka → Local Kafka
Consumer: Lưu data từ Local Kafka → MongoDB
Spark: Transform data thành Dim/Fact tables → PostgreSQL

Flow: Producer Task → Consumer Task → Spark Task
Sử dụng import functions thay vì subprocess.run để hiệu quả hơn
"""
from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator

# Add kafka path to sys.path để import được modules
kafka_path = '/opt/airflow/kafka'
if kafka_path not in sys.path:
    sys.path.insert(0, kafka_path)

# Import functions từ producer và consumer
try:
    from producer_app import run_bridge as producer_run_bridge
    from consumer_app import run_consumer as consumer_run_consumer
    print("✅ Successfully imported producer and consumer functions")
except ImportError as e:
    print(f"❌ Failed to import functions: {e}")
    raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def run_kafka_producer(**context):
    """
    Chạy producer_app.py - import trực tiếp function thay vì subprocess
    """
    print("🚀 Starting Kafka Producer - Bridge Remote → Local")

    # Set environment variable để Python đọc .env.docker
    os.environ['ENV_FILE'] = '/opt/airflow/kafka/.env.docker'

    try:
        # Gọi trực tiếp function từ imported module
        producer_run_bridge()
        print("✅ Producer completed successfully!")
        return 'Producer completed'
    except Exception as e:
        print(f"❌ Producer failed: {e}")
        raise


def run_kafka_consumer(**context):
    """
    Chạy consumer_app.py - import trực tiếp function thay vì subprocess
    """
    print("🚀 Starting Kafka Consumer - Local Kafka → MongoDB")

    # Set environment variable để Python đọc .env.docker
    os.environ['ENV_FILE'] = '/opt/airflow/kafka/.env.docker'

    try:
        # Gọi trực tiếp function từ imported module
        consumer_run_consumer()
        print("✅ Consumer completed successfully!")
        return 'Consumer completed'
    except Exception as e:
        print(f"❌ Consumer failed: {e}")
        raise


def run_spark_processing(**context):
    """
    Chạy Spark job để xử lý data từ MongoDB và lưu vào PostgreSQL
    """
    spark_script_path = '/opt/bitnami/spark/airflow_spark_processor.py'
    

    print(" Starting Spark Processing - MongoDB → PostgreSQL")
  
    
    # Build spark-submit command (user 'spark' đã được tạo sẵn trong container)
    # Dùng mongo-spark-connector 10.4.0 compatible với Spark 3.5.x
    spark_command = (
        f"export SPARK_HOME=/opt/bitnami/spark && "
        f"export HOME=/tmp/spark-home && "
        f"mkdir -p /tmp/spark-home && "
        f"/opt/bitnami/spark/bin/spark-submit "
        f"--master spark://spark:7077 "
        f"--packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,org.postgresql:postgresql:42.7.4 "
        f"--conf spark.jars.ivy=/tmp/.ivy2 "
        f"--conf spark.mongodb.read.connection.uri=mongodb://mongo:27017/kafka_data_db.product_views_records "
        f"{spark_script_path}"
    )
    
    # Submit Spark job via bash -c
    result = subprocess.run(
        ['docker', 'exec', 'project--1-spark-1', 'bash', '-c', spark_command],
        capture_output=True,
        text=True,
        timeout=7200,  # 2 hours timeout
    )
    
    print("\n SPARK OUTPUT:")
    print(result.stdout)
    
    if result.stderr:
        print("\n SPARK STDERR:")
        print(result.stderr)
    
    if result.returncode != 0:
        raise Exception(f"Spark job failed with return code {result.returncode}")
    
    print("\n Spark processing completed successfully!")
    return 'Spark completed'


with DAG(
    'pipeline_v1',
    default_args=default_args,
    description='Complete Kafka Pipeline: Remote → Local → MongoDB → PostgreSQL (Import Functions)',
    schedule_interval='0 */2 * * *',  # Chạy mỗi 2 giờ
    catchup=False,
    tags=['kafka', 'pipeline', 'producer', 'consumer', 'spark', 'import-functions'],
    max_active_runs=1,  # Chỉ cho phép 1 instance chạy cùng lúc
) as dag:


    producer_task = PythonOperator(
        task_id='kafka_producer_bridge',
        python_callable=run_kafka_producer,
        execution_timeout=timedelta(hours=2),
    )

    consumer_task = PythonOperator(
        task_id='kafka_consumer_to_mongodb',
        python_callable=run_kafka_consumer,
        execution_timeout=timedelta(hours=2),
    )

    # Task 3: Spark - Xử lý data từ MongoDB và lưu vào PostgreSQL
    spark_task = PythonOperator(
        task_id='spark_mongodb_to_postgres',
        python_callable=run_spark_processing,
        execution_timeout=timedelta(hours=2),
    )

    # Định nghĩa flow: Producer → Consumer → Spark
    producer_task >> consumer_task >> spark_task
