# Project1_level2

## Kafka Topic Management

### Create Topic
docker exec kafka-0 kafka-topics --bootstrap-server kafka-0:29092 --create --topic processed_product_view_test --partitions 3 --replication-factor 3

### Delete Topic  
docker exec kafka-0 kafka-topics --bootstrap-server kafka-0:29092 --delete --topic processed_product_view_test

### Check Topic Messages
docker exec kafka-0 kafka-console-consumer --bootstrap-server kafka-0:29092 --topic processed_product_view_test --from-beginning --timeout-ms 5000

## Configuration
- MAX_WORKERS=5 (số threads xử lý)
- MAX_MESSAGES=10000 (giới hạn số messages xử lý)

## Usage
1. Chạy producer: `python producer_app.py`
2. Chạy consumer: `python consumer_app.py`

Producer sẽ dừng sau khi xử lý 10,000 messages từ remote Kafka.
Consumer sẽ dừng sau khi xử lý 10,000 messages từ local Kafka và lưu vào MongoDB.