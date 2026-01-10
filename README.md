# Real-Time Data Streaming & Analytics Pipeline

A comprehensive data engineering project implementing a real-time ETL pipeline using Kafka, MongoDB, Spark, and PostgreSQL for product view analytics.

## 🏗️ Architecture Overview

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   Remote    │      │   Kafka     │      │   MongoDB   │      │    Spark    │      │ PostgreSQL  │
│   Kafka     │─────▶│   Cluster   │─────▶│  (Raw Data) │─────▶│  Processing │─────▶│  (Data WH)  │
│  (Source)   │      │  (3 Nodes)  │      │             │      │             │      │             │
└─────────────┘      └─────────────┘      └─────────────┘      └─────────────┘      └─────────────┘
    Producer            Consumer                                  ETL Job              Star Schema
```

### Components

1. **Kafka Producer** - Consumes data from remote Kafka and publishes to local Kafka cluster
2. **Kafka Consumer** - Reads from local Kafka and stores in MongoDB
3. **MongoDB** - NoSQL database storing raw product view events
4. **Apache Spark** - Distributed data processing engine transforming data into dimensional model
5. **PostgreSQL** - Relational database storing processed data in Star Schema

## 📋 Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Setup & Installation](#setup--installation)
- [Running the Pipeline](#running-the-pipeline)
- [Data Model](#data-model)
- [Configuration](#configuration)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## ✨ Features

- **Real-time streaming**: Kafka-based event streaming with 3-node cluster
- **High availability**: Kafka cluster with replication factor 3
- **SASL authentication**: Secure Kafka communication with PLAIN mechanism
- **Multi-threaded processing**: Concurrent message processing with configurable worker threads
- **Data warehousing**: Star schema design with dimension and fact tables
- **Distributed processing**: Apache Spark for scalable ETL transformations
- **Docker containerization**: All services running in isolated containers
- **Comprehensive logging**: Detailed logging for monitoring and debugging

## 🔧 Prerequisites

- Docker & Docker Compose
- Python 3.12+
- 16GB+ RAM recommended for Spark workers
- Windows/Linux/macOS

## 📁 Project Structure

```
Project--1/
├── kafka/
│   ├── producer_app.py          # Kafka producer (remote → local)
│   ├── consumer_app.py          # Kafka consumer (local → MongoDB)
│   ├── requirements.txt         # Python dependencies
│   └── logs/                    # Application logs
├── spark/
│   ├── spark.py                 # Spark ETL job (MongoDB → PostgreSQL)
│   ├── create_dim_fact_tables.sql  # PostgreSQL schema
│   ├── requirements.txt         # Spark dependencies
│   └── util/
│       ├── config.py            # Configuration management
│       └── udf_manager.py       # Custom UDFs
├── config/
│   └── kafka/
│       ├── client.properties    # Kafka client config
│       └── kafka_server_jaas.conf  # SASL authentication
├── docker-compose.yml           # Docker services orchestration
├── python_check_mongo.py        # MongoDB data verification script
├── delete_mongo_data.py         # MongoDB cleanup utility
├── count_kafka_total.py         # Kafka message counter
├── setup.txt                    # Setup commands
└── README.md                    # This file
```

## 🚀 Setup & Installation

### 1. Create Docker Network

```bash
docker network create streaming-network
```

### 2. Start All Services

```bash
docker-compose up -d
```

This will start:
- 3 Kafka brokers (kafka-0, kafka-1, kafka-2)
- AKHQ (Kafka UI) on port 8180
- PostgreSQL on port 5433
- Adminer (DB UI) on port 8380
- MongoDB on port 27017
- Spark master on port 8080
- 2 Spark workers

### 3. Verify Services

```bash
docker-compose ps
```

All services should be in "Up" state.

### 4. Initialize PostgreSQL Schema

```bash
# Copy SQL script to PostgreSQL container
docker cp spark/create_dim_fact_tables.sql postgres:/tmp/

# Execute schema creation
docker exec postgres psql -U postgres -d postgres -f /tmp/create_dim_fact_tables.sql

# Verify tables created
docker exec postgres psql -U postgres -d postgres -c "\dt"
```

### 5. Setup Spark Dependencies (One-time)

```bash
# Install Python dependencies
docker-compose exec -T spark bash -c "pip install pymongo pandas psycopg2-binary"

# Download PostgreSQL JDBC driver
docker-compose exec -T spark bash -c "cd /opt/bitnami/spark/jars && curl -sL -o postgresql-42.7.4.jar https://jdbc.postgresql.org/download/postgresql-42.7.4.jar"
```

## 🎯 Running the Pipeline

### Step 1: Start Kafka Producer

The producer reads from remote Kafka and publishes to local cluster:

```bash
cd kafka
python producer_app.py
```

**Configuration** (via `.env` or environment variables):
- `SOURCE_BROKERS`: Remote Kafka brokers
- `DESTINATION_BROKERS`: Local Kafka brokers
- `MAX_MESSAGES`: Limit messages to process (default: 100,000)
- `MAX_WORKERS`: Thread pool size (default: 5)

### Step 2: Start Kafka Consumer

The consumer reads from local Kafka and stores in MongoDB:

```bash
cd kafka
python consumer_app.py
```

**Configuration**:
- `KAFKA_BROKERS`: Local Kafka brokers
- `MONGO_HOST`: MongoDB host (localhost or container name)
- `MONGO_DB`: Database name (default: kafka_data_db)
- `MONGO_COLLECTION`: Collection name (default: product_views_records)

### Step 3: Verify MongoDB Data

```bash
# Check document count
python python_check_mongo.py

# Or using Docker
docker-compose exec -T mongo mongosh --eval "db.product_views_records.countDocuments()"
```

### Step 4: Run Spark ETL Job

```bash
# Copy Spark script to container
docker cp spark/spark.py project--1-spark-1:/tmp/spark.py

# Execute Spark job
docker-compose exec -T spark bash -c "SPARK_LOCAL_IP=127.0.0.1 spark-submit --driver-java-options '-Divy.cache.dir=/tmp/ivy -Divy.home=/tmp/ivy' --master local[1] /tmp/spark.py"
```

**For concise output:**
```bash
docker-compose exec -T spark bash -c "SPARK_LOCAL_IP=127.0.0.1 spark-submit --driver-java-options '-Divy.cache.dir=/tmp/ivy -Divy.home=/tmp/ivy' --master local[1] /tmp/spark.py 2>&1 | tail -60"
```

### Step 5: Verify PostgreSQL Data

```bash
# Check row counts
docker exec postgres psql -U postgres -d postgres -c "SELECT 'dim_date' AS table_name, COUNT(*) FROM dim_date UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product UNION ALL SELECT 'dim_country', COUNT(*) FROM dim_country UNION ALL SELECT 'dim_referrer', COUNT(*) FROM dim_referrer UNION ALL SELECT 'fact_product_views', COUNT(*) FROM fact_product_views;"
```

## 📊 Data Model

### Star Schema Design

#### Dimension Tables

**dim_date** - Date dimension
```sql
- date_key (PK)
- date
- year, month, day
- day_of_week, week_of_year, quarter
```

**dim_product** - Product dimension
```sql
- product_key (PK)
- product_id (UNIQUE)
- product_name, category, price
- is_active, created_date
```

**dim_country** - Country/Store dimension
```sql
- country_key (PK)
- store_id (UNIQUE)
- country_name, country_code
- region, continent
```

**dim_referrer** - Referrer dimension
```sql
- referrer_key (PK)
- referrer_url
- referrer_domain
- referrer_type (Search Engine, Social Media, Direct, Other)
```

**dim_device** - Device dimension
```sql
- device_key (PK)
- device_id (UNIQUE)
- device_type, browser_info
```

#### Fact Table

**fact_product_views** - Product view facts
```sql
- date_key (FK)
- product_key (FK)
- country_key (FK)
- referrer_key (FK)
- device_key (FK)
- view_count
- unique_visitors
- first_view_time, last_view_time
- view_duration_seconds
- avg_view_timestamp
- processed_at
```

## ⚙️ Configuration

### Kafka Configuration

**SASL Authentication:**
```properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka" password="UnigapKafka@2024";
```

**Topic Configuration:**
- Partitions: 3
- Replication Factor: 3
- Auto-create: Enabled

### MongoDB Configuration

```env
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DB=kafka_data_db
MONGO_COLLECTION=product_views_records
```

### PostgreSQL Configuration

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_USER=postgres
POSTGRES_PASSWORD=UnigapPostgres@123
POSTGRES_DB=postgres
```

### Spark Configuration

```python
spark.sql.shuffle.partitions=4
spark.driver.memory=2g
SPARK_WORKER_MEMORY=8G
SPARK_WORKER_CORES=4
```

## 📈 Monitoring

### Web UIs

- **AKHQ (Kafka UI)**: http://localhost:8180
  - Username: `admin`
  - Password: `admin`
  
- **Spark UI**: http://localhost:8080

- **Adminer (PostgreSQL UI)**: http://localhost:8380
  - System: PostgreSQL
  - Server: postgres
  - Username: postgres
  - Password: UnigapPostgres@123

### Logs

```bash
# Kafka logs
docker-compose logs kafka-0

# MongoDB logs
docker-compose logs mongo

# PostgreSQL logs
docker-compose logs postgres

# Spark logs
docker-compose logs spark

# Application logs
tail -f kafka/logs/producer.log
tail -f kafka/logs/consumer.log
```

## 🔍 Troubleshooting

### Issue: Kafka Connection Refused

**Solution:**
```bash
# Check if Kafka is running
docker-compose ps kafka-0 kafka-1 kafka-2

# Restart Kafka cluster
docker-compose restart kafka-0 kafka-1 kafka-2
```

### Issue: MongoDB Empty Collection

**Solution:**
```bash
# Verify consumer is running
docker-compose logs mongo

# Check MongoDB from Spark container
docker-compose exec -T spark python -c "from pymongo import MongoClient; client = MongoClient('mongodb://host.docker.internal:27017/'); db = client['kafka_data_db']; print(f'Count: {db.product_views_records.count_documents({})}')"
```

### Issue: Spark Job Fails with PostgreSQL Driver Error

**Solution:**
```bash
# Re-download PostgreSQL JDBC driver
docker-compose exec -T spark bash -c "cd /opt/bitnami/spark/jars && curl -sL -o postgresql-42.7.4.jar https://jdbc.postgresql.org/download/postgresql-42.7.4.jar && ls -lh postgresql-42.7.4.jar"
```

### Issue: PostgreSQL Index Error (URL too long)

**Solution:**
```bash
# Drop problematic index
docker-compose exec -T postgres psql -U postgres -c "DROP INDEX IF EXISTS idx_dim_referrer_url; TRUNCATE TABLE public.dim_referrer CASCADE;"
```

## 🛠️ Utility Scripts

### Check MongoDB Data
```bash
python python_check_mongo.py
```

### Delete MongoDB Data
```bash
python delete_mongo_data.py
```

### Count Kafka Messages
```bash
python count_kafka_total.py
```

## 📝 Database Connection (DBeaver)

**PostgreSQL Connection:**
- Host: `localhost`
- Port: `5433`
- Database: `postgres`
- Username: `postgres`
- Password: `UnigapPostgres@123`

## 🎓 Learning Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [MongoDB Documentation](https://docs.mongodb.com/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

## 📄 License

This project is for educational and demonstration purposes.

## 👥 Authors

Data Engineering Team

## 🙏 Acknowledgments

- Apache Kafka for real-time streaming
- Apache Spark for distributed processing
- MongoDB for flexible data storage
- PostgreSQL for robust data warehousing
