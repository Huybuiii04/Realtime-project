import os
import json
import time
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

# --- Load .env (hoặc custom ENV_FILE nếu được set) ---
env_file = os.getenv('ENV_FILE', '.env')  # Mặc định .env, nhưng có thể override
load_dotenv(env_file)
logging.info(f" Loaded environment from: {env_file}")

# --- Logging Configuration ---
log_dir = "logs"
log_file = os.path.join(log_dir, "producer.log")
os.makedirs(log_dir, exist_ok=True)

log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s')

file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)

logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])

# --- Kafka Remote (SOURCE) ---
SOURCE_BROKERS = os.getenv('SOURCE_BROKERS').split(',')
SOURCE_SECURITY_PROTOCOL = os.getenv('SOURCE_SECURITY_PROTOCOL')
SOURCE_SASL_MECHANISM = os.getenv('SOURCE_SASL_MECHANISM')
SOURCE_SASL_USERNAME = os.getenv('SOURCE_SASL_USERNAME')
SOURCE_SASL_PASSWORD = os.getenv('SOURCE_SASL_PASSWORD')
SOURCE_TOPIC = os.getenv('SOURCE_TOPIC')
SOURCE_CONSUMER_GROUP_ID = os.getenv('SOURCE_CONSUMER_GROUP_ID')

# --- Kafka Local (DESTINATION) ---
DESTINATION_BROKERS = os.getenv('DESTINATION_BROKERS').split(',')
DESTINATION_SECURITY_PROTOCOL = os.getenv('DESTINATION_SECURITY_PROTOCOL')
DESTINATION_SASL_MECHANISM = os.getenv('KAFKA_SASL_MECHANISM')
DESTINATION_SASL_USERNAME = os.getenv('KAFKA_SASL_USERNAME')
DESTINATION_SASL_PASSWORD = os.getenv('KAFKA_SASL_PASSWORD')
DESTINATION_TOPIC = os.getenv('DESTINATION_TOPIC')

# --- App Settings ---
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 5))
MAX_MESSAGES = int(os.getenv('MAX_MESSAGES', 10000))


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
            enable_auto_commit=True,
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
            'value_serializer': lambda v: json.dumps(v).encode('utf-8')
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


def process_and_produce_message(producer, destination_topic, message_value, message_offset):
    """Xử lý 1 message từ Kafka remote và ghi sang Kafka local."""
    try:
        processed_data = message_value
        logging.info(f" Forwarding message offset {message_offset} to local topic '{destination_topic}'")

        future = producer.send(destination_topic, value=processed_data)
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)
    except Exception as e:
        logging.error(f" Error processing message offset {message_offset}: {e}")


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

    logging.info(f" Start bridging data from remote topic '{SOURCE_TOPIC}' to local topic '{DESTINATION_TOPIC}' using {MAX_WORKERS} threads...")
    logging.info(f" Max messages to process: {MAX_MESSAGES}")

    message_count = 0
    futures = []  # Track submitted tasks
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        try:
            for message in consumer:
                if message_count >= MAX_MESSAGES:
                    logging.info(f" Reached maximum messages limit: {MAX_MESSAGES}")
                    break
                    
                data = message.value
                offset = message.offset
                logging.info(f" Received message from remote offset {offset}. Dispatching to local Kafka...")
                future = executor.submit(process_and_produce_message, producer, DESTINATION_TOPIC, data, offset)
                futures.append(future)
                message_count += 1

        except KeyboardInterrupt:
            logging.info(" Interrupted by user. Shutting down bridge...")
        except Exception as e:
            logging.error(f" Error during message bridge: {e}")
    
    # ThreadPoolExecutor context exit = tự động đợi tất cả threads hoàn thành
    logging.info(f" Waiting for {len(futures)} pending tasks to complete...")
    
    # Đợi và check kết quả
    completed = sum(1 for f in futures if f.done() and not f.exception())
    failed = sum(1 for f in futures if f.done() and f.exception())
    logging.info(f" Tasks completed: {completed}, failed: {failed}")
    
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


if __name__ == "__main__":
    run_bridge()
