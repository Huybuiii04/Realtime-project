# consumer_app.py
import os
import json
import logging
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.structs import TopicPartition, OffsetAndMetadata
from pymongo import MongoClient
from pymongo import UpdateOne
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv

# --- Load .env ---
load_dotenv()

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.getLogger("kafka").setLevel(logging.WARNING)

# ---------------- ENV ----------------
# Lưu ý: file .env nên trỏ tới KAFKA LOCAL
# - Nếu chạy Python trên Windows host:  localhost:9094,localhost:9194,localhost:9294
# - Nếu chạy Python trong container cùng network Kafka: kafka-0:29092,kafka-1:29092,kafka-2:29092
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9094,localhost:9194,localhost:9294")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT").upper()
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "kafka")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")

DESTINATION_TOPIC = os.getenv("DESTINATION_TOPIC", "destination_topic")
DESTINATION_CONSUMER_GROUP_ID = os.getenv("DESTINATION_CONSUMER_GROUP_ID", "local_product_view_mongo_group")

MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DB = os.getenv("MONGO_DB", "kafka_data_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "product_views_records")

MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "100000"))
CONSUMER_TIMEOUT_MS = int(os.getenv("CONSUMER_TIMEOUT_MS", "0"))
MONGO_BATCH_SIZE = int(os.getenv("MONGO_BATCH_SIZE", "5000"))

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
            "enable_auto_commit": False,
            "group_id": DESTINATION_CONSUMER_GROUP_ID,
            # Tinh chỉnh tiêu thụ (có thể đổi cho phù hợp)
            "max_poll_records": MONGO_BATCH_SIZE,
            "request_timeout_ms": 30000,
            "session_timeout_ms": 10000,
        }
        if CONSUMER_TIMEOUT_MS > 0:
            consumer_kwargs["consumer_timeout_ms"] = CONSUMER_TIMEOUT_MS

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
def decode_message(raw_bytes):
    text = raw_bytes.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw": text}


def insert_batch(mongo_collection, messages):
    operations = []
    for message in messages:
        doc = decode_message(message.value)
        doc["_id"] = f"{message.topic}:{message.partition}:{message.offset}"
        operations.append(UpdateOne({"_id": doc["_id"]}, {"$setOnInsert": doc}, upsert=True))

    if not operations:
        return 0

    result = mongo_collection.bulk_write(operations, ordered=False)
    return result.upserted_count + result.matched_count


def commit_batch(consumer, messages):
    offsets = {}
    for message in messages:
        tp = TopicPartition(message.topic, message.partition)
        next_offset = message.offset + 1
        current = offsets.get(tp)
        if current is None or next_offset > current.offset:
            offsets[tp] = OffsetAndMetadata(next_offset, None, -1)
    consumer.commit(offsets=offsets)

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
        f" Consume từ '{DESTINATION_TOPIC}' → Mongo ({MONGO_DB}.{MONGO_COLLECTION})"
    )
    logging.info(f" Max messages to process: {MAX_MESSAGES}")
    logging.info(f" Mongo batch size: {MONGO_BATCH_SIZE}")
    logging.info("Nhấn Ctrl+C để dừng...")

    message_count = 0
    failed = False
    try:
        try:
            while message_count < MAX_MESSAGES:
                records = consumer.poll(timeout_ms=1000, max_records=min(MONGO_BATCH_SIZE, MAX_MESSAGES - message_count))
                messages = [message for partition_records in records.values() for message in partition_records]
                if not messages:
                    continue

                logging.info(f" Nhận batch {len(messages)} messages → insert_many Mongo")

                try:
                    inserted = insert_batch(mongo_collection, messages)
                    commit_batch(consumer, messages)
                    message_count += inserted
                    logging.info(f" Saved and committed batch: {inserted} messages; total={message_count}")
                except Exception as e:
                    failed = True
                    logging.error(f" Lỗi xử lý batch: {e}")
                    break

        except StopIteration:
            logging.info(f" Consumer timeout - no more messages available. Processed {message_count} messages.")
        except KeyboardInterrupt:
            logging.info(" Người dùng ngắt.")
        except Exception as e:
            failed = True
            logging.error(f" Lỗi vòng lặp consumer: {e}")
        
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
        if failed:
            raise SystemExit(1)

if __name__ == "__main__":
    run_consumer()
