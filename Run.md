##Create database PostgreSQL

# 1. Copy file vào container PostgreSQL
docker cp spark/create_dim_fact_tables.sql postgres:/tmp/

# 2. Chạy SQL script trong PostgreSQL
docker exec postgres psql -U postgres -d postgres -f /tmp/create_dim_fact_tables.sql

# 3. Liet ke table trong postgres
docker exec postgres psql -U postgres -d postgres -c "\dt"

# 4. Dem du lieu trong postgres
docker exec postgres psql -U postgres -d postgres -c "SELECT COUNT(*) FROM dim_date;"

##================== RUN SPARK JOB ==================

# 1. Khởi động Docker Compose (nếu chưa chạy)
docker-compose up -d

# 2. Kiểm tra các container đang chạy
docker-compose ps

# 3. Cài đặt các dependencies trong Spark container (chỉ cần chạy 1 lần)
docker-compose exec -T spark bash -c "pip install pymongo pandas psycopg2-binary"

# 4. Download PostgreSQL JDBC driver (chỉ cần chạy 1 lần)
docker-compose exec -T spark bash -c "cd /opt/bitnami/spark/jars && curl -sL -o postgresql-42.7.4.jar https://jdbc.postgresql.org/download/postgresql-42.7.4.jar && ls -lh postgresql-42.7.4.jar"

# 5. Copy Spark script vào container
docker cp spark/spark.py project--1-spark-1:/tmp/spark.py

# 6. Chạy Spark job (cách 1: local mode)
docker-compose exec -T spark bash -c "SPARK_LOCAL_IP=127.0.0.1 spark-submit --driver-java-options '-Divy.cache.dir=/tmp/ivy -Divy.home=/tmp/ivy' --master local[1] /tmp/spark.py"

# 7. Chạy Spark job (cách 2: với output ngắn gọn)
docker-compose exec -T spark bash -c "SPARK_LOCAL_IP=127.0.0.1 spark-submit --driver-java-options '-Divy.cache.dir=/tmp/ivy -Divy.home=/tmp/ivy' --master local[1] /tmp/spark.py 2>&1 | tail -60"

# 8. Xem dữ liệu sau khi chạy Spark
docker exec postgres psql -U postgres -d postgres -c "SELECT COUNT(*) FROM dim_date; SELECT COUNT(*) FROM fact_product_views;"

##================== TROUBLESHOOTING ==================

# Kiểm tra dữ liệu MongoDB
docker-compose exec -T mongo mongosh --eval "db.product_views_records.countDocuments()"

# Kiểm tra kết nối MongoDB từ Spark
docker-compose exec -T spark python -c "from pymongo import MongoClient; client = MongoClient('mongodb://host.docker.internal:27017/'); db = client['kafka_data_db']; print(f'Count: {db.product_views_records.count_documents({})}')"

# Xem logs PostgreSQL
docker-compose logs postgres

# Xem logs Spark
docker-compose logs spark
