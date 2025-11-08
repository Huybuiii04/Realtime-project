# Airflow Project Setup

## Cấu trúc thư mục

```
Project--1/
├── airflow/
│   ├── Dockerfile          # Custom Airflow image với Java 17
│   └── requirements.txt    # Python packages cho Airflow
├── dags/                   # Airflow DAGs
├── logs/                   # Airflow logs
├── plugins/                # Airflow plugins
├── config/                 # Airflow config files
├── .env                    # Environment variables
└── docker-compose.yml      # Docker Compose configuration
```

## Cài đặt và chạy

### 1. Tạo network (chỉ chạy lần đầu)
```bash
docker network create streaming-network
```

### 2. Build Airflow image
```bash
docker-compose build
```

### 3. Khởi động Airflow
```bash
docker-compose up -d
```

### 4. Khởi động với Flower (monitoring tool)
```bash
docker-compose --profile flower up -d
```

### 5. Dừng services
```bash
docker-compose down
```

### 6. Xóa volumes (cẩn thận - sẽ mất dữ liệu)
```bash
docker-compose down -v
```

## Truy cập Airflow

- **Airflow Web UI**: http://localhost:18080
  - Username: `airflow`
  - Password: `airflow`

- **Flower (nếu chạy với profile flower)**: http://localhost:5555

## Services trong Docker Compose

### Kafka Services
- kafka-0, kafka-1, kafka-2: Kafka cluster (ports 9094, 9194, 9294)
- akhq: Kafka UI (port 8180)

### Database Services
- postgres: PostgreSQL cho ứng dụng (port 5432)
- airflow-postgres: PostgreSQL cho Airflow
- adminer: Database management UI (port 8380)

### Spark Services
- spark: Spark master (port 8080)
- spark-worker: Spark workers

### Airflow Services
- airflow-webserver: Airflow web interface (port 18080)
- airflow-scheduler: Airflow scheduler
- airflow-worker: Celery workers
- airflow-triggerer: Airflow triggerer
- airflow-init: Initialize Airflow (chạy 1 lần)
- redis: Message broker cho Celery
- flower: Celery monitoring (optional, port 5555)

## Cấu hình

Các biến môi trường được định nghĩa trong file `.env`:

```
AIRFLOW_IMAGE_NAME=unigap/airflow:2.10.4
AIRFLOW_UID=1000
DOCKER_GID=999
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```

## Custom Airflow Image

Dockerfile trong thư mục `airflow/` bao gồm:
- Apache Airflow 2.10.4
- Python 3.11
- OpenJDK 17 (để tích hợp với Spark)
- Các Python packages từ `requirements.txt`

## DAGs

Đặt các file DAG Python vào thư mục `dags/`. File DAG mẫu: `example_hello_world.py`

## Logs

Logs của Airflow được lưu trong thư mục `logs/`

## Troubleshooting

### Kiểm tra logs
```bash
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler
docker-compose logs airflow-worker
```

### Khởi động lại services
```bash
docker-compose restart airflow-webserver
docker-compose restart airflow-scheduler
```

### Xem trạng thái containers
```bash
docker-compose ps
```

### Vào container để debug
```bash
docker exec -it airflow-webserver bash
```

## Tích hợp với Kafka và Spark

Airflow có thể kết nối với:
- Kafka cluster qua network `streaming-network`
- Spark cluster qua network `streaming-network`
- PostgreSQL database

Để tạo connection trong Airflow:
1. Vào Admin > Connections
2. Thêm connection mới với thông tin từ docker-compose.yml



docker exec kafka-0 kafka-topics --bootstrap-server kafka-0:9092 --command-config /opt/bitnami/kafka/config/client.properties --delete --topic processed_product_view_test