# Real-time Product Analytics Pipeline

## 📋 Project Overview

A comprehensive real-time data pipeline that processes product view events from Kafka, stores them in MongoDB, and transforms them into a complete Star Schema analytics database in PostgreSQL using Apache Spark. The entire workflow is orchestrated by Apache Airflow with full error handling and monitoring.

## 🏗️ Architecture

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DATA PIPELINE ARCHITECTURE                          │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────┐
│  Remote Kafka    │
│  (Source Data)   │
│  Topic:          │
│  product_views   │
└────────┬─────────┘
         │
         │ (1) Producer reads remote topic
         ▼
┌──────────────────┐
│  Local Kafka     │
│  Cluster         │
│  (3 Brokers)     │
│                  │
│  Topic:          │
│  processed_      │
│  product_view_   │
│  test            │
└────────┬─────────┘
         │
         │ (2) Consumer reads local topic
         ▼
┌──────────────────┐
│   MongoDB 7.0    │
│                  │
│  Database:       │
│  kafka_data_db   │
│                  │
│  Collection:     │
│  product_views_  │
│  records         │
└────────┬─────────┘
         │
         │ (3) Spark transforms to Star Schema
         ▼
┌──────────────────┐
│  PostgreSQL      │
│  16.3            │
│  Star Schema     │
│                  │
│  Dimensions:     │
│  - dim_date      │
│  - dim_product   │
│  - dim_country   │
│  - dim_referrer  │
│  - dim_device    │
│                  │
│  Fact:           │
│  - fact_product_ │
│    views         │
└────────┬─────────┘
         │
         │ (4) Orchestrates entire workflow
         │
┌──────────────────┐
│  Apache Airflow  │
│  2.10.4          │
│                  │
│  DAG Tasks:      │
│  1. Producer     │
│  2. Consumer     │
│  3. Spark ETL    │
└──────────────────┘
```

## 🔄 Data Flow

### 1. **Producer Stage** (`kafka/producer_app.py`)

- Connects to remote Kafka cluster (147.185.221.24:33415)
- Reads from `product_views` topic
- Transforms and forwards to local Kafka cluster
- Topic: `processed_product_view_test`

### 2. **Consumer Stage** (`kafka/consumer_app.py`)

- Consumes from local Kafka topic
- Batch processing (configurable batch size)
- Stores raw events in MongoDB
- Collection: `product_views_records`

### 3. **ETL Stage** (`spark/airflow_spark_processor.py`)

- Spark reads from MongoDB with PyMongo (handles schemaless data)
- Data preprocessing: converts complex types, handles nulls, fixes timestamps
- Creates complete Star Schema:
  - **5 Dimension Tables**: date, product, country, referrer, device
  - **1 Fact Table**: product_views with foreign key relationships
- Uses DataFrame aliases to resolve JOIN conflicts
- Handles ISO timestamp format conversion

### 4. **Orchestration** (`airflow/dags/pipeline_v1`)

- Airflow DAG manages execution order
- Sequential task execution with dependencies
- Error handling and retry logic
- Monitoring and logging

## 📊 Data Schema

### MongoDB Document Structure

```json
{
  "_id": ObjectId("..."),
  "product_id": "P001",
  "device_id": "device-abc-123",
  "store_id": "US",
  "referrer_url": "https://google.com/search?q=product",
  "local_time": "2025-11-26T15:30:45.123456",
  "remote_ip": "192.168.1.100",
  "timestamp": "2025-11-26T15:30:45Z"
}
```

### PostgreSQL Star Schema

#### Dimension Tables

**`dim_date`** - Date dimension (4 records)

```sql
CREATE TABLE dim_date (
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
```

**`dim_product`** - Product dimension (3,084 records)

```sql
CREATE TABLE dim_product (
    product_key BIGSERIAL PRIMARY KEY,
    product_id VARCHAR(255) UNIQUE NOT NULL,
    product_name VARCHAR(500),
    category VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    created_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**`dim_country`** - Country/Store dimension (75 records)

```sql
CREATE TABLE dim_country (
    country_key BIGSERIAL PRIMARY KEY,
    store_id VARCHAR(255) UNIQUE NOT NULL,
    country_name VARCHAR(255),
    region VARCHAR(255),
    created_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**`dim_referrer`** - Referrer dimension (6,907 records)

```sql
CREATE TABLE dim_referrer (
    referrer_key BIGSERIAL PRIMARY KEY,
    referrer_url TEXT UNIQUE NOT NULL,
    referrer_domain VARCHAR(500),
    referrer_type VARCHAR(255),
    created_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**`dim_device`** - Device dimension (8,191 records)

```sql
CREATE TABLE dim_device (
    device_key BIGSERIAL PRIMARY KEY,
    device_id VARCHAR(255) UNIQUE NOT NULL,
    device_type VARCHAR(255),
    browser_info TEXT,
    created_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### Fact Table

**`fact_product_views`** - Main fact table (4 records)

```sql
CREATE TABLE fact_product_views (
    fact_key BIGSERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES dim_date(date_key),
    product_key BIGINT REFERENCES dim_product(product_key),
    country_key BIGINT REFERENCES dim_country(country_key),
    referrer_key BIGINT REFERENCES dim_referrer(referrer_key),
    device_key BIGINT REFERENCES dim_device(device_key),

    -- Measures
    view_count INTEGER NOT NULL DEFAULT 0,
    unique_visitors INTEGER NOT NULL DEFAULT 0,
    view_duration_seconds INTEGER,

    -- Timestamps
    first_view_time TIMESTAMP,
    last_view_time TIMESTAMP,
    avg_view_timestamp DOUBLE PRECISION,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Metadata
    data_source VARCHAR(255) DEFAULT 'kafka_mongodb',
    batch_id VARCHAR(255)
);
```

## 🛠️ Technology Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| **Apache Airflow** | 2.10.4 | Workflow orchestration |
| **Apache Kafka** | 3.x | Message streaming (3 brokers) |
| **Apache Spark** | 3.5.0 | Distributed data processing (Local mode) |
| **MongoDB** | 7.0 | Document storage for raw events |
| **PostgreSQL** | 16.3 | Star Schema analytics database |
| **Python** | 3.11 | Application development |
| **Docker** | Latest | Containerization |
| **Adminer** | 4.8.1 | Web-based database management |

## 📁 Project Structure

```text
Project--1/
├── airflow/                    # Airflow configuration & DAGs
├── config/
│   └── kafka/
│       └── kafka_server_jaas.conf  # Kafka authentication
├── dags/
│   └── pipeline_v1              # Main Airflow DAG
├── kafka/
│   ├── producer_app.py         # Kafka producer (remote → local)
│   ├── consumer_app.py         # Kafka consumer (local → MongoDB)
│   ├── requirements.txt        # Kafka dependencies
│   └── README.md               # Kafka setup guide
├── spark/
│   ├── airflow_spark_processor.py  # Main ETL job (MongoDB → PostgreSQL)
│   ├── create_postgres_tables.sql # Star Schema DDL
│   ├── database.py              # Database utilities
│   ├── schema.py                # Data schemas
│   ├── requirements.txt         # Spark dependencies
│   ├── run.py                   # Spark runner
│   ├── util/
│   │   ├── config.py            # Configuration management
│   │   └── udf_manager.py       # User-defined functions
│   └── *.md                     # Documentation files
├── logs/                        # Application logs
├── plugins/                     # Airflow plugins
├── docker-compose.yml           # Docker services definition
└── README.md                   # This file
```

## 🚀 Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Access to remote Kafka cluster credentials
- 8GB+ RAM recommended

### Installation

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd Project--1
   ```

2. **Configure environment**

   ```bash
   # Update Kafka credentials in config/kafka/kafka_server_jaas.conf
   # Update connection strings in spark/util/config.py if needed
   ```

3. **Start services**

   ```bash
   docker-compose up -d
   ```

4. **Verify services are running**

   ```bash
   docker ps
   ```

### Database Setup

1. **Create Star Schema tables**

   ```bash
   # Copy SQL file to container
   docker cp spark/create_postgres_tables.sql postgres:/tmp/

   # Execute SQL file
   docker exec postgres psql -U postgres -d postgres -f /tmp/create_postgres_tables.sql
   ```

1. **Verify table creation**

   ```bash
   docker exec postgres psql -U postgres -d postgres -c "\dt"
   ```

### Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | [http://localhost:8080](http://localhost:8080) | admin/admin |
| **Adminer** (Database GUI) | [http://localhost:8380](http://localhost:8380) | See PostgreSQL credentials |
| **Spark Master** | [http://localhost:8081](http://localhost:8081) | - |
| **Kafka UI (AKHQ)** | [http://localhost:8082](http://localhost:8082) | - |
| **PostgreSQL** | localhost:5433 | postgres/UnigapPostgres@123 |
| **MongoDB** | localhost:27017 | - |

### Running the Pipeline

1. **Access Airflow UI**
   - Navigate to [http://localhost:8080](http://localhost:8080)
   - Login: `admin` / `admin`

1. **Enable the DAG**
   - Find `pipeline_v1`
   - Toggle ON the DAG

1. **Trigger the DAG**
   - Click "Trigger DAG" button
   - Monitor execution in Graph View

1. **Verify results**

   ```sql
   -- Connect to PostgreSQL
   psql -h localhost -p 5433 -U postgres -d postgres

   -- Check Star Schema data
   SELECT 'dim_date' as table_name, COUNT(*) as count FROM dim_date
   UNION ALL
   SELECT 'dim_product', COUNT(*) FROM dim_product
   UNION ALL
   SELECT 'dim_country', COUNT(*) FROM dim_country
   UNION ALL
   SELECT 'dim_referrer', COUNT(*) FROM dim_referrer
   UNION ALL
   SELECT 'dim_device', COUNT(*) FROM dim_device
   UNION ALL
   SELECT 'fact_product_views', COUNT(*) FROM fact_product_views;

   -- Sample Star Schema query
   SELECT
       f.fact_key,
       d.date,
       p.product_name,
       c.country_name,
       r.referrer_domain,
       dv.device_type,
       f.view_count,
       f.unique_visitors
   FROM fact_product_views f
   JOIN dim_date d ON f.date_key = d.date_key
   JOIN dim_product p ON f.product_key = p.product_key
   JOIN dim_country c ON f.country_key = c.country_key
   JOIN dim_referrer r ON f.referrer_key = r.referrer_key
   JOIN dim_device dv ON f.device_key = dv.device_key
   ORDER BY f.view_count DESC
   LIMIT 10;
   ```

## 🔧 Configuration

### Kafka Configuration

- **Remote Kafka**: `kafka/producer_app.py`

  ```python
  REMOTE_KAFKA_BROKERS = "147.185.221.24:33415"
  REMOTE_TOPIC = "product_views"
  SASL_USERNAME = "your_username"
  SASL_PASSWORD = "your_password"
  ```

- **Local Kafka**: `docker-compose.yml`

  ```yaml
  # 3-node cluster with SASL authentication
  KAFKA_LISTENERS: CONTROLLER://:9093,INTERNAL://:29092,DOCKER_NETWORK://:9092,EXTERNAL://0.0.0.0:9094
  KAFKA_SASL_ENABLED_MECHANISMS: PLAIN
  ```

### MongoDB Configuration

```python
MONGODB_URI = "mongodb://mongo:27017/"
DATABASE_NAME = "kafka_data_db"
COLLECTION_NAME = "product_views_records"
```

### PostgreSQL Configuration

```python
POSTGRES_HOST = "postgres"  # Container name
POSTGRES_PORT = 5432        # Internal container port
POSTGRES_DB = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "UnigapPostgres@123"

# External access (from host/DBeaver):
EXTERNAL_HOST = "localhost"
EXTERNAL_PORT = 5433  # Changed from 5432 to avoid conflicts
```

### Spark Configuration

```python
# Local mode for reliability (changed from cluster mode)
SPARK_MASTER = "local[*]"
SPARK_EXECUTOR_MEMORY = "2g"
SPARK_EXECUTOR_CORES = 4

# Dependencies
PACKAGES = [
    "org.postgresql:postgresql:42.7.4",
    "org.mongodb.spark:mongo-spark-connector:10.4.0"  # Not used, using PyMongo instead
]
```

## 🐛 Troubleshooting

### Common Issues

1. **PostgreSQL Connection Failed**

   ```bash
   # Check if container is running
   docker ps --filter "name=postgres"

   # Test connection
   docker exec postgres psql -U postgres -d postgres -c "SELECT version();"

   # External connection test
   psql -h localhost -p 5433 -U postgres -d postgres
   ```

1. **DBeaver Connection Issues**
   - **Port**: Use `5433` (not 5432)
   - **Host**: `localhost` or `127.0.0.1`
   - **Password**: `UnigapPostgres@123`
   - **Test Connection** before saving

1. **Spark Job Fails**

   ```bash
   # Check Spark logs
   docker logs airflow-worker

   # Common issues:
   # - PyMongo not installed: pip install psycopg2-binary
   # - Table already exists: Check truncate logic
   # - Timestamp format: ISO format conversion
   ```

1. **Kafka Connection Issues**
   - Check `config/kafka/kafka_server_jaas.conf`
   - Verify remote Kafka credentials
   - Check network connectivity to `147.185.221.24:33415`

1. **MongoDB Data Not Found**

   ```bash
   # Check MongoDB data
   docker exec mongo mongosh --eval "db.product_views_records.countDocuments()"

   # Verify collection exists
   docker exec mongo mongosh kafka_data_db --eval "show collections"
   ```

1. **Dimension Tables Empty After Spark Job**
   - Check if data preprocessing handles null values
   - Verify JOIN operations use correct aliases
   - Check PostgreSQL foreign key constraints

### Log Locations

```bash
# Airflow logs
docker logs airflow-webserver
docker logs airflow-scheduler
docker logs airflow-worker

# Kafka logs
docker logs kafka-0
docker logs kafka-1
docker logs kafka-2

# Spark ETL logs (inside Airflow worker)
docker logs airflow-worker

# Database logs
docker logs postgres
docker logs mongo

# Application logs
tail -f logs/*.log
```

## 🔒 Security Considerations

- ✅ **SASL/PLAIN authentication** for remote Kafka
- ✅ **Password authentication** for PostgreSQL
- ✅ **Internal Docker network** only (no external MongoDB access)
- ✅ **Isolated containers** with proper user permissions
- ⚠️ **Change default passwords** in production
- ⚠️ **Enable SSL/TLS** for production deployments
- ⚠️ **Network segmentation** for production

## 📝 Development Notes

### Key Implementation Details

1. **Schemaless MongoDB Handling**
   - PyMongo instead of MongoDB Spark connector
   - Dynamic type conversion (dicts/lists → JSON strings)
   - Null value handling and data cleaning

2. **Star Schema Design**
   - Surrogate keys for all dimensions
   - Foreign key constraints maintained
   - Efficient JOIN operations for analytics

3. **Data Quality Assurance**
   - Preprocessing pipeline for inconsistent data
   - ISO timestamp format conversion
   - Duplicate handling and data validation

4. **Error Recovery**
   - Airflow retry mechanisms
   - Idempotent operations
   - Transaction-safe database writes

5. **Performance Optimizations**
   - Local Spark mode for reliability
   - Batch processing for efficiency
   - Indexed database tables

### ETL Process Flow

```text
Raw MongoDB Documents → Data Preprocessing → Dimension Creation → Fact Aggregation → PostgreSQL Star Schema
```

## 🙏 Acknowledgments

- Apache Airflow community for orchestration framework
- Apache Spark community for data processing capabilities
- MongoDB and PostgreSQL communities for database solutions
- Docker ecosystem for containerization
- Confluent for Kafka distribution

---

**Last Updated**: November 26, 2025   
**Architecture**: Complete Star Schema ETL Pipeline
