# GIỚI THIỆU PROJECT: REAL-TIME DATA STREAMING & ANALYTICS PIPELINE

## 1. TỔNG QUAN PROJECT

Project này là một **hệ thống xử lý dữ liệu real-time với kiến trúc Data Engineering hiện đại**, xử lý luồng dữ liệu product view events từ nguồn Kafka từ xa, lưu trữ vào MongoDB, sau đó transform và load vào PostgreSQL theo mô hình Star Schema để phân tích.

**Bài toán thực tế**: Tracking và phân tích hành vi xem sản phẩm của người dùng trong thời gian thực trên một nền tảng e-commerce.

## 2. KIẾN TRÚC HỆ THỐNG (ARCHITECTURE)

```
Remote Kafka → Local Kafka Cluster (3 nodes) → MongoDB → Apache Spark → PostgreSQL (Star Schema)
     ↓              ↓                              ↓            ↓              ↓
  Producer      Consumer                      Raw Data      ETL Job        Data Warehouse
```

**Data Flow chi tiết:**
- **Producer App**: Consume data từ remote Kafka → Publish vào local Kafka cluster
- **Consumer App**: Read từ local Kafka → Insert vào MongoDB (raw data storage)
- **Spark Job**: Extract từ MongoDB → Transform theo Star Schema → Load vào PostgreSQL

## 3. CÔNG NGHỆ SỬ DỤNG

### Infrastructure & Orchestration:
- **Docker & Docker Compose**: Containerize toàn bộ services
- **Docker Network**: Isolated network cho các services giao tiếp

### Data Streaming:
- **Apache Kafka 7.6.1**: Message broker với KRaft mode (không dùng Zookeeper)
  - Cluster 3 nodes (kafka-0, kafka-1, kafka-2)
  - Replication factor = 3 (high availability)
  - SASL/PLAIN authentication
  - 3 partitions per topic
- **kafka-python**: Python client library

### Data Storage:
- **MongoDB 7.0**: NoSQL database lưu raw events
- **PostgreSQL 16.3**: Relational database cho Data Warehouse
- **Star Schema**: Dimensional modeling (5 dimension tables + 1 fact table)

### Data Processing:
- **Apache Spark 3.5**: Distributed data processing
  - 1 Master node + 2 Workers (8GB RAM, 4 cores mỗi worker)
  - PySpark API
  - Custom UDFs cho data transformation

### Monitoring & Management:
- **AKHQ**: Kafka UI (port 8180)
- **Adminer**: Database management UI (port 8380)

### Programming:
- **Python 3.12+**
- Libraries: kafka-python, pymongo, pyspark, psycopg2-binary, pandas

## 4. CHI TIẾT IMPLEMENTATION

### 4.1. Kafka Producer (kafka/producer_app.py)
- **Multi-threaded processing**: ThreadPoolExecutor với 5 workers
- Consume từ remote Kafka với SASL authentication
- Publish vào local Kafka cluster
- Comprehensive logging system
- Auto-create topics nếu chưa tồn tại
- Message counter với limit configurable (default: 100,000)

### 4.2. Kafka Consumer (kafka/consumer_app.py)
- Multi-threaded batch processing
- Read từ local Kafka cluster
- Bulk insert vào MongoDB (batch operations)
- Error handling và retry logic
- JSON parsing với validation

### 4.3. Spark ETL Job (spark/spark.py)
- **Extract**: PyMongo đọc từ MongoDB
- **Transform**: 
  - Parse JSON strings
  - Date/time transformations
  - Create surrogate keys
  - Aggregate metrics
- **Load**: Write vào PostgreSQL
  - 5 Dimension tables: dim_date, dim_product, dim_country, dim_referrer, dim_device
  - 1 Fact table: fact_product_views
- **Slowly Changing Dimension (SCD)**: Handle product/country changes

### 4.4. Data Model - Star Schema

**Dimension Tables:**
- `dim_date`: Date attributes (year, month, quarter, week_of_year...)
- `dim_product`: Product info với SCD Type 1
- `dim_country`: Store/country location data
- `dim_referrer`: Traffic source tracking
- `dim_device`: Device/browser information

**Fact Table:**
- `fact_product_views`: Aggregated metrics
  - Measures: view_count, unique_visitors, view_duration, avg_timestamp
  - Foreign keys đến tất cả dimensions
  - Timestamp tracking cho first/last view

## 5. FEATURES NỔI BẬT

✅ **High Availability**: Kafka cluster với replication factor 3  
✅ **Security**: SASL authentication cho Kafka  
✅ **Scalability**: Spark distributed processing với 2 workers  
✅ **Data Quality**: Schema validation, error handling  
✅ **Monitoring**: AKHQ UI, comprehensive logging  
✅ **Containerization**: 100% Docker-based, portable  
✅ **Performance**: Multi-threading, batch processing  

## 6. SKILLS THỂ HIỆN ĐƯỢC

### Data Engineering:
- Thiết kế và implement Real-time streaming pipeline
- ETL/ELT processes với Spark
- Data modeling (Star Schema, dimensional modeling)
- Performance optimization (partitioning, batching)

### Big Data Technologies:
- Kafka cluster setup và management
- Spark distributed computing
- MongoDB document database
- PostgreSQL data warehousing

### DevOps:
- Docker containerization
- Docker Compose orchestration
- Network configuration
- Service monitoring

### Software Engineering:
- Python OOP và multi-threading
- Error handling và logging
- Configuration management (.env, config files)
- Code organization và modularity

## 7. CÂU HỎI PHỎNG VẤN CÓ THỂ GẶP VÀ CÁCH TRẢ LỜI

**Q: Tại sao chọn Kafka thay vì RabbitMQ hay Redis?**
> A: Kafka phù hợp cho high-throughput streaming data, support replay messages, horizontal scaling tốt. Replication factor 3 đảm bảo data durability. Partitioning cho parallel processing.

**Q: Tại sao dùng cả MongoDB và PostgreSQL?**
> A: MongoDB lưu raw data (schema-less, fast writes). PostgreSQL cho analytics với Star Schema (structured data, complex queries, OLAP). Lambda architecture pattern.

**Q: Làm thế nào handle data quality issues?**
> A: JSON validation, null handling, data type checking, error logging, try-catch blocks, retry logic cho network failures.

**Q: Performance optimization đã làm gì?**
> A: Multi-threading (5 workers), batch processing MongoDB inserts, Spark partitioning (4 shuffle partitions), Kafka 3 partitions, indexed surrogate keys trong PostgreSQL.

**Q: Nếu scale lên 100x traffic thì làm thế nào?**
> A: Increase Kafka partitions, thêm Spark workers, MongoDB sharding, PostgreSQL read replicas, implement caching layer (Redis), message batching lớn hơn.

## 8. MỞ RỘNG DỰ ĐỊNH (FUTURE IMPROVEMENTS)

- [ ] Implement Apache Airflow để schedule Spark jobs
- [ ] Add data quality checks với Great Expectations
- [ ] Implement CDC (Change Data Capture) cho real-time updates
- [ ] Add data lineage tracking
- [ ] Kubernetes deployment thay vì Docker Compose
- [ ] Monitoring với Prometheus + Grafana

## 9. HƯỚNG DẪN CHẠY PROJECT

### Setup môi trường
```bash
# Tạo Docker network
docker network create streaming-network

# Start tất cả services
docker-compose up -d
```

### Tạo database PostgreSQL
```bash
# Copy SQL script vào container
docker cp spark/create_dim_fact_tables.sql postgres:/tmp/

# Chạy SQL script
docker exec postgres psql -U postgres -d postgres -f /tmp/create_dim_fact_tables.sql
```

### Chạy Spark Job
```bash
# Cài đặt dependencies (chỉ chạy 1 lần)
docker-compose exec -T spark bash -c "pip install pyspark==3.5.0 user-agents==2.2.0 psycopg2-binary==2.9.9 pymongo==4.6.3"

# Download PostgreSQL JDBC driver
docker-compose exec -T spark bash -c "cd /opt/bitnami/spark/jars && curl -sL -o postgresql-42.7.4.jar https://jdbc.postgresql.org/download/postgresql-42.7.4.jar"

# Copy Spark script
docker cp spark/spark.py project--1-spark-1:/tmp/spark.py

# Chạy Spark job
docker-compose exec -T spark bash -c "SPARK_LOCAL_IP=127.0.0.1 spark-submit --master local[1] /tmp/spark.py"
```

---

**Lưu ý khi phỏng vấn:** 
- Nhấn mạnh vào **end-to-end data pipeline** từ ingestion → storage → processing → serving
- Highlight **production-ready features**: authentication, logging, error handling, monitoring
- Đề cập đến **scalability** và **reliability** considerations
- Show business value: Enable real-time analytics cho business decisions
