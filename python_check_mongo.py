from pymongo import MongoClient

# Sử dụng localhost khi chạy từ Windows host
client = MongoClient('mongodb://localhost:27017')
db = client['kafka_data_db']
collection = db['product_views_records']

# Kiểm tra số records
count = collection.count_documents({})
print(f"📊 Tổng records: {count}")

# Xem 3 records gần nhất
if count > 0:
    for doc in collection.find().limit(3):
        print(doc)
else:
    print("❌ Không có data")