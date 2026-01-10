#!/usr/bin/env python3
"""
Xóa toàn bộ data trong MongoDB collection
"""
import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT', '27017'))
MONGO_DB = os.getenv('MONGO_DB', 'kafka_data_db')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'product_views_records')

def delete_all_data():
    """Xóa toàn bộ data trong MongoDB collection"""
    try:
        print("\n" + "="*70)
        print("XÓA DỮ LIỆU MONGODB")
        print("="*70 + "\n")
        
        print(f"🔌 Kết nối: {MONGO_HOST}:{MONGO_PORT}")
        print(f"📦 Database: {MONGO_DB}")
        print(f"📋 Collection: {MONGO_COLLECTION}\n")
        
        # Kết nối MongoDB
        client = MongoClient(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", connectTimeoutMS=5000)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Kiểm tra số documents trước khi xóa
        count_before = collection.count_documents({})
        print(f"📊 Documents trước khi xóa: {count_before:,}")
        
        if count_before == 0:
            print("ℹ️  Collection đã trống, không cần xóa")
        else:
            print(f"\n⚠️  Đang xóa {count_before:,} documents...")
            result = collection.delete_many({})
            print(f"✅ Đã xóa {result.deleted_count:,} documents")
            
            # Verify
            count_after = collection.count_documents({})
            print(f"📊 Documents sau khi xóa: {count_after:,}")
        
        client.close()
        print("\n" + "="*70 + "\n")
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")

if __name__ == '__main__':
    delete_all_data()
