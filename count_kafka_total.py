#!/usr/bin/env python3
"""
Script tính tổng dữ liệu trong Kafka topic
"""
import os
import logging
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.structs import TopicPartition
from dotenv import load_dotenv

# --- Setup Logging ---
logging.basicConfig(
    level=logging.WARNING,  # Chỉ show warnings và errors
    format="%(levelname)s: %(message)s"
)

# --- Load environment ---
load_dotenv()

# Kafka configuration
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "46.202.167.130:9094")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT").upper()
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "kafka")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")
TOPIC_TO_CHECK = os.getenv("TOPIC_TO_CHECK", "product_view")

BROKER_LIST = [b.strip() for b in KAFKA_BROKERS.split(",") if b.strip()]

def get_total_messages():
    """Tính tổng số messages trong topic"""
    try:
        consumer_config = {
            'bootstrap_servers': BROKER_LIST,
            'security_protocol': KAFKA_SECURITY_PROTOCOL,
            'group_id': 'total_count_group',
            'enable_auto_commit': False
        }
        
        if KAFKA_SECURITY_PROTOCOL != 'PLAINTEXT':
            consumer_config.update({
                'sasl_mechanism': KAFKA_SASL_MECHANISM,
                'sasl_plain_username': KAFKA_SASL_USERNAME,
                'sasl_plain_password': KAFKA_SASL_PASSWORD
            })
        
        consumer = KafkaConsumer(**consumer_config)
        
        # Lấy tất cả partitions của topic
        partitions = consumer.partitions_for_topic(TOPIC_TO_CHECK)
        
        if not partitions:
            print(f"❌ Topic '{TOPIC_TO_CHECK}' không tồn tại")
            consumer.close()
            return None
        
        # Tạo TopicPartition objects
        tp_list = [TopicPartition(TOPIC_TO_CHECK, p) for p in partitions]
        consumer.assign(tp_list)
        
        # Lấy beginning offset (offset đầu tiên)
        consumer.seek_to_beginning()
        beginning_offsets = consumer.position(tp_list[0])
        
        # Lấy end offset (offset cuối cùng)
        consumer.seek_to_end()
        end_offsets = {}
        total_messages = 0
        
        print("\n" + "="*70)
        print(f"THỐNG KÊ DỮ LIỆU KAFKA - TOPIC: {TOPIC_TO_CHECK}")
        print("="*70)
        print(f"\n📊 Thông tin chi tiết từng partition:\n")
        
        for partition in sorted(partitions):
            tp = TopicPartition(TOPIC_TO_CHECK, partition)
            consumer.seek_to_end(tp)
            end_offset = consumer.position(tp)
            end_offsets[partition] = end_offset
            
            # Messages = end_offset - 0 (vì offset bắt đầu từ 0)
            partition_messages = end_offset
            total_messages += partition_messages
            
            print(f"   Partition {partition}:")
            print(f"      • Số messages: {partition_messages:,}")
            print(f"      • Offset hiện tại: 0 → {end_offset - 1:,}")
        
        consumer.close()
        
        print(f"\n" + "-"*70)
        print(f"✅ TỔNG SỐ MESSAGES: {total_messages:,}")
        print("="*70 + "\n")
        
        return total_messages
        
    except Exception as e:
        print(f"ERROR: {e}")
        return None

if __name__ == '__main__':
    get_total_messages()
