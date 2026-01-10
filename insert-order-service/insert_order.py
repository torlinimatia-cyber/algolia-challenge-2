import json
import sys
import os
from pymongo import MongoClient
from datetime import datetime

def get_mongo_client():
    host = os.getenv('MONGO_HOST', 'localhost')
    port = os.getenv('MONGO_PORT', '27017')
    mongo_url = f'mongodb://{host}:{port}/'
    return MongoClient(mongo_url)

def insert_order(order_data):
    client = get_mongo_client()
    db_name = os.getenv('MONGO_DATABASE', 'etl_db')
    db = client[db_name]
    orders_collection = db['orders']
    
    if 'created_at' not in order_data:
        order_data['created_at'] = datetime.utcnow()
    if 'updated_at' not in order_data:
        order_data['updated_at'] = datetime.utcnow()
    
    result = orders_collection.insert_one(order_data)
    
    print(f"Order inserted successfully")
    print(f"Order ID: {order_data.get('order_id', 'N/A')}")
    print(f"MongoDB _id: {result.inserted_id}")
    print(f"User ID: {order_data.get('user_id', 'N/A')}")
    print(f"Amount: ${order_data.get('amount', 0)}")
    print(f"Status: {order_data.get('status', 'N/A')}")
    
    client.close()
    return result.inserted_id

def main():
    if len(sys.argv) < 2:
        print("Error: No order file provided")
        print("Usage: python insert_order.py <order.json>")
        sys.exit(1)
    
    order_file = sys.argv[1]
    
    if not os.path.exists(order_file):
        print(f"Error: File not found: {order_file}")
        sys.exit(1)
    
    try:
        with open(order_file, 'r') as f:
            order_data = json.load(f)
        
        insert_order(order_data)
        
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in file {order_file}")
        print(f"Details: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()