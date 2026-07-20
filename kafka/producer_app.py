import os
import json
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.structs import TopicPartition, OffsetAndMetadata
from dotenv import load_dotenv

# --- Load .env ---
load_dotenv()

# --- Logging Configuration ---
log_dir = "logs"
log_file = os.path.join(log_dir, "producer.log")

try:
    os.makedirs(log_dir, exist_ok=True)
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'))
    file_handler.setLevel(logging.INFO)
except Exception as e:
    # Nếu không thể tạo file log, chỉ dùng console
    print(f"⚠️  Could not create log file {log_file}: {e}")
    file_handler = None

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'))
console_handler.setLevel(logging.INFO)

# Setup logging with available handlers
handlers = [console_handler]
if file_handler:
    handlers.append(file_handler)
logging.basicConfig(level=logging.INFO, handlers=handlers)
logging.getLogger("kafka").setLevel(logging.WARNING)

# --- Kafka Remote (SOURCE) ---
SOURCE_BROKERS = os.getenv('SOURCE_BROKERS', '46.202.167.130:9094').split(',')
SOURCE_SECURITY_PROTOCOL = os.getenv('SOURCE_SECURITY_PROTOCOL', 'SASL_PLAINTEXT')
SOURCE_SASL_MECHANISM = os.getenv('SOURCE_SASL_MECHANISM', 'PLAIN')
SOURCE_SASL_USERNAME = os.getenv('SOURCE_SASL_USERNAME', 'kafka')
SOURCE_SASL_PASSWORD = os.getenv('SOURCE_SASL_PASSWORD', '')
SOURCE_TOPIC = os.getenv('SOURCE_TOPIC', 'product_view')
SOURCE_CONSUMER_GROUP_ID = os.getenv('SOURCE_CONSUMER_GROUP_ID', 'source_consumer_group')

# --- Kafka Local (DESTINATION) ---
DESTINATION_BROKERS = os.getenv('DESTINATION_BROKERS', 'localhost:9094,localhost:9194,localhost:9294').split(',')
DESTINATION_SECURITY_PROTOCOL = os.getenv('DESTINATION_SECURITY_PROTOCOL', 'SASL_PLAINTEXT')
DESTINATION_SASL_MECHANISM = os.getenv('DESTINATION_SASL_MECHANISM', 'PLAIN')
DESTINATION_SASL_USERNAME = os.getenv('DESTINATION_SASL_USERNAME', 'kafka')
DESTINATION_SASL_PASSWORD = os.getenv('DESTINATION_SASL_PASSWORD', '')
DESTINATION_TOPIC = os.getenv('DESTINATION_TOPIC', 'destination_topic')

# --- App Settings ---
MAX_MESSAGES = int(os.getenv('MAX_MESSAGES', 100000))
PRODUCER_BATCH_SIZE = int(os.getenv('PRODUCER_BATCH_SIZE', '1000'))


# --- Kafka Setup Functions ---
def create_kafka_source_consumer():
    """Tạo consumer kết nối tới Kafka Remote."""
    try:
        consumer = KafkaConsumer(
            SOURCE_TOPIC,
            bootstrap_servers=SOURCE_BROKERS,
            security_protocol=SOURCE_SECURITY_PROTOCOL,
            sasl_mechanism=SOURCE_SASL_MECHANISM,
            sasl_plain_username=SOURCE_SASL_USERNAME,
            sasl_plain_password=SOURCE_SASL_PASSWORD,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=SOURCE_CONSUMER_GROUP_ID,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logging.info(f" Source Consumer connected to remote Kafka topic '{SOURCE_TOPIC}'.")
        return consumer
    except Exception as e:
        logging.error(f" Error creating source Kafka consumer: {e}")
        return None


def create_topic_if_not_exists(topic_name, num_partitions=3, replication_factor=3):
    """Tạo topic nếu chưa tồn tại."""
    try:
        admin_config = {
            'bootstrap_servers': DESTINATION_BROKERS,
            'security_protocol': DESTINATION_SECURITY_PROTOCOL,
        }
        
        # Thêm SASL config nếu cần
        if DESTINATION_SECURITY_PROTOCOL == 'SASL_PLAINTEXT':
            admin_config.update({
                'sasl_mechanism': DESTINATION_SASL_MECHANISM,
                'sasl_plain_username': DESTINATION_SASL_USERNAME,
                'sasl_plain_password': DESTINATION_SASL_PASSWORD
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
        logging.info(f" Topic '{topic_name}' already exists, skipping creation.")
    except Exception as e:
        logging.warning(f" Could not create topic '{topic_name}': {e}")


def create_kafka_destination_producer():
    """Tạo producer kết nối tới Kafka Local."""
    try:
        producer_config = {
            'bootstrap_servers': DESTINATION_BROKERS,
            'security_protocol': DESTINATION_SECURITY_PROTOCOL,
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'acks': 'all',
            'retries': int(os.getenv('KAFKA_PRODUCER_RETRIES', '5')),
            'linger_ms': int(os.getenv('KAFKA_LINGER_MS', '20')),
            'batch_size': int(os.getenv('KAFKA_BATCH_SIZE_BYTES', '131072')),
            'compression_type': os.getenv('KAFKA_COMPRESSION_TYPE', 'gzip'),
        }
        
        # Thêm SASL config nếu cần
        if DESTINATION_SECURITY_PROTOCOL == 'SASL_PLAINTEXT':
            producer_config.update({
                'sasl_mechanism': DESTINATION_SASL_MECHANISM,
                'sasl_plain_username': DESTINATION_SASL_USERNAME,
                'sasl_plain_password': DESTINATION_SASL_PASSWORD
            })
        
        producer = KafkaProducer(**producer_config)
        logging.info(" Destination Producer connected to local Kafka cluster.")
        return producer
    except Exception as e:
        logging.error(f" Error creating destination Kafka producer: {e}")
        return None


def on_send_success(record_metadata):
    logging.debug(f"Message delivered to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")


def on_send_error(excp):
    logging.error(f"Message delivery failed: {excp}")


def flush_batch(producer, consumer, batch):
    if not batch:
        return 0

    futures = []
    for message in batch:
        futures.append(producer.send(DESTINATION_TOPIC, value=message.value))

    for future in futures:
        future.get(timeout=30)

    producer.flush()
    offsets = {}
    for message in batch:
        tp = TopicPartition(message.topic, message.partition)
        next_offset = message.offset + 1
        current = offsets.get(tp)
        if current is None or next_offset > current.offset:
            offsets[tp] = OffsetAndMetadata(next_offset, None, -1)
    consumer.commit(offsets=offsets)
    logging.info(f" Forwarded and committed batch: {len(batch)} messages")
    return len(batch)


def run_bridge():
    """Đọc từ Kafka remote → ghi sang Kafka local."""
    # Tạo topic trước nếu chưa tồn tại
    logging.info(f"🔍 Checking if topic '{DESTINATION_TOPIC}' exists...")
    create_topic_if_not_exists(DESTINATION_TOPIC, num_partitions=3, replication_factor=3)
    
    consumer = create_kafka_source_consumer()
    producer = create_kafka_destination_producer()

    if not consumer or not producer:
        logging.error(" Cannot initialize Kafka bridge. Check connection configs.")
        return

    logging.info(f" Start bridging data from remote topic '{SOURCE_TOPIC}' to local topic '{DESTINATION_TOPIC}'...")
    logging.info(f" Max messages to process: {MAX_MESSAGES}")

    message_count = 0
    failed = False
    batch = []
    
    try:
        for message in consumer:
            if message_count + len(batch) >= MAX_MESSAGES:
                logging.info(f" Reached maximum messages limit: {MAX_MESSAGES}")
                break

            batch.append(message)

            try:
                if len(batch) >= PRODUCER_BATCH_SIZE:
                    message_count += flush_batch(producer, consumer, batch)
                    batch = []
            except Exception as e:
                failed = True
                logging.error(f" Error processing batch: {e}")
                break

        if not failed and batch:
            message_count += flush_batch(producer, consumer, batch)

    except KeyboardInterrupt:
        logging.info(" Interrupted by user. Shutting down bridge...")
    except Exception as e:
        failed = True
        logging.error(f" Error during message bridge: {e}")
    
    # Bây giờ mới close producer
    try:
        producer.flush()
        logging.info(" Producer flushed successfully")
        producer.close()
        logging.info(" Producer closed successfully")
    except Exception as e:
        logging.error(f" Error closing producer: {e}")
    
    try:
        consumer.close()
        logging.info(" Consumer closed successfully")
    except Exception as e:
        logging.error(f" Error closing consumer: {e}")
    
    logging.info(f" Kafka bridge stopped. Processed {message_count} messages.")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    run_bridge()
