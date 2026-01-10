# consumer_app.py
import os
import json
import logging
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

# --- Load .env ---
load_dotenv()

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------------- ENV ----------------
# Lưu ý: file .env nên trỏ tới KAFKA LOCAL
# - Nếu chạy Python trên Windows host:  localhost:9094,localhost:9194,localhost:9294
# - Nếu chạy Python trong container cùng network Kafka: kafka-0:29092,kafka-1:29092,kafka-2:29092
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-0:9092,kafka-1:9092,kafka-2:9092")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT").upper()
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "kafka")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "UnigapKafka@2024")

DESTINATION_TOPIC = os.getenv("DESTINATION_TOPIC", "destination_topic")
DESTINATION_CONSUMER_GROUP_ID = os.getenv("DESTINATION_CONSUMER_GROUP_ID", "local_product_view_mongo_group")

MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DB = os.getenv("MONGO_DB", "kafka_data_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "product_views_records")

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "100000"))

# Validate env nhanh
missing = []
for key in ["KAFKA_BROKERS", "DESTINATION_TOPIC"]:
    if not globals()[key]:
        missing.append(key)
if missing:
    logging.error(f"Thiếu biến môi trường: {', '.join(missing)}. Kiểm tra file .env!")
    raise SystemExit(1)

BROKER_LIST = [b.strip() for b in KAFKA_BROKERS.split(",") if b.strip()]

# ---------------- Helper Functions ----------------
def create_topic_if_not_exists(topic_name, num_partitions=3, replication_factor=3):
    """Tạo topic nếu chưa tồn tại."""
    try:
        admin_config = {
            'bootstrap_servers': BROKER_LIST,
            'security_protocol': KAFKA_SECURITY_PROTOCOL,
        }
        
        # Thêm SASL config nếu cần
        if KAFKA_SECURITY_PROTOCOL != 'PLAINTEXT':
            admin_config.update({
                'sasl_mechanism': KAFKA_SASL_MECHANISM,
                'sasl_plain_username': KAFKA_SASL_USERNAME,
                'sasl_plain_password': KAFKA_SASL_PASSWORD
            })
        
        admin_client = KafkaAdminClient(**admin_config)
        
        # Tạo topic mới
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        logging.info(f" Topic '{topic_name}' created successfully with {num_partitions} partitions and replication factor {replication_factor}")
        admin_client.close()
        
    except TopicAlreadyExistsError:
        logging.info(f"ℹ Topic '{topic_name}' already exists, skipping creation.")
    except Exception as e:
        logging.warning(f" Could not create topic '{topic_name}': {e}")

# ---------------- Kafka Consumer ----------------
def create_kafka_destination_consumer():
    """
    Tạo consumer đọc từ Kafka LOCAL.
    - Không đặt value_deserializer ở đây để tránh crash nếu dữ liệu không phải JSON.
    - Parse JSON ở bước xử lý message.
    """
    try:
        consumer_kwargs = {
            "bootstrap_servers": BROKER_LIST,
            "security_protocol": KAFKA_SECURITY_PROTOCOL,
            "auto_offset_reset": "earliest",
            "enable_auto_commit": True,
            "group_id": DESTINATION_CONSUMER_GROUP_ID,
            # Tinh chỉnh tiêu thụ (có thể đổi cho phù hợp)
            "max_poll_records": 100,
            "request_timeout_ms": 30000,
            "session_timeout_ms": 10000,
            "consumer_timeout_ms": 5000,  # Timeout sau 5s nếu không có message mới
        }

        # Chỉ gán SASL nếu không phải PLAINTEXT
        if KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
            consumer_kwargs.update({
                "sasl_mechanism": KAFKA_SASL_MECHANISM,
                "sasl_plain_username": KAFKA_SASL_USERNAME,
                "sasl_plain_password": KAFKA_SASL_PASSWORD,
            })

        consumer = KafkaConsumer(DESTINATION_TOPIC, **consumer_kwargs)
        logging.info(
            f" Kafka Destination Consumer OK | topic='{DESTINATION_TOPIC}' | group='{DESTINATION_CONSUMER_GROUP_ID}' | brokers={BROKER_LIST}"
        )
        return consumer
    except Exception as e:
        logging.error(f" Lỗi tạo Kafka Destination Consumer: {e}")
        return None

# ---------------- Mongo Client ----------------
def create_mongo_client():
    try:
        client = MongoClient(MONGO_HOST, MONGO_PORT)
        client.admin.command("ping")
        logging.info(f" Kết nối MongoDB OK | {MONGO_HOST}:{MONGO_PORT}")
        return client
    except ConnectionFailure as e:
        logging.error(f" Kết nối MongoDB thất bại: {e}")
        return None
    except Exception as e:
        logging.error(f" Lỗi MongoDB: {e}")
        return None

# ---------------- Processing ----------------
def process_message(raw_bytes, mongo_collection, message_offset):
    """
    Xử lý 1 message:
    - Thử decode UTF-8
    - Thử parse JSON; nếu không phải JSON thì lưu dạng {raw}
    """
    try:
        text = raw_bytes.decode("utf-8", errors="replace")

        try:
            doc = json.loads(text)
        except json.JSONDecodeError:
            doc = {"raw": text}

        result = mongo_collection.insert_one(doc)
        logging.info(f" Saved offset={message_offset} _id={result.inserted_id}")
    except Exception as mongo_err:
        logging.error(f" Lỗi lưu Mongo (offset {message_offset}): {mongo_err}")

# ---------------- Main loop ----------------
def run_consumer():
    # Tạo topic trước nếu chưa tồn tại
    logging.info(f"🔍 Checking if topic '{DESTINATION_TOPIC}' exists...")
    create_topic_if_not_exists(DESTINATION_TOPIC, num_partitions=3, replication_factor=3)
    
    consumer = create_kafka_destination_consumer()
    mongo_client = create_mongo_client()

    if not consumer or not mongo_client:
        logging.error(" Không khởi tạo được consumer hoặc MongoDB client. Thoát.")
        return

    mongo_collection = mongo_client[MONGO_DB][MONGO_COLLECTION]

    logging.info(
        f" Consume từ '{DESTINATION_TOPIC}' với {MAX_WORKERS} worker → Mongo ({MONGO_DB}.{MONGO_COLLECTION})"
    )
    logging.info(f" Max messages to process: {MAX_MESSAGES}")
    logging.info("Nhấn Ctrl+C để dừng...")

    message_count = 0
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            try:
                for message in consumer:
                    if message_count >= MAX_MESSAGES:
                        logging.info(f" Reached maximum messages limit: {MAX_MESSAGES}")
                        break
                        
                    message_offset = message.offset
                    logging.info(f" Nhận offset {message_offset} → đẩy vào thread pool")
                    executor.submit(process_message, message.value, mongo_collection, message_offset)
                    message_count += 1
                    
            except StopIteration:
                logging.info(f" Consumer timeout - no more messages available. Processed {message_count} messages.")
            except KeyboardInterrupt:
                logging.info(" Người dùng ngắt. Đang chờ các tác vụ dở dang...")
            except Exception as e:
                logging.error(f" Lỗi vòng lặp consumer: {e}")
        
        # ThreadPoolExecutor context exits here, ensuring all tasks complete
        logging.info(" Tất cả worker threads đã hoàn thành.")
        
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        try:
            mongo_client.close()
            logging.info(" Đã đóng MongoDB client.")
        except Exception:
            pass
        logging.info(f" Consumer dừng an toàn. Processed {message_count} messages.")

if __name__ == "__main__":
    run_consumer()
