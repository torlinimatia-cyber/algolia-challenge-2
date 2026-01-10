import json
from datetime import datetime
from pymongo import MongoClient
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config import Config
import time
import sys
from bson import ObjectId

class CDCConsumer:
    def __init__(self):
        self.config = Config()
        self.mongo_client = None
        self.kafka_producer = None
        self.resume_token = None
        self.event_counter = 0
        
    def connect_mongodb(self):
        max_retries = 10
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                print(f"Connecting to MongoDB at {self.config.MONGO_HOST}:{self.config.MONGO_PORT}")
                self.mongo_client = MongoClient(
                    self.config.mongo_uri,
                    serverSelectionTimeoutMS=5000
                )
                self.mongo_client.admin.command('ping')
                print("Successfully connected to MongoDB")
                return True
            except Exception as e:
                retry_count += 1
                print(f"MongoDB connection attempt {retry_count}/{max_retries} failed: {e}")
                if retry_count < max_retries:
                    time.sleep(5)
                else:
                    print("Failed to connect to MongoDB after maximum retries")
                    return False
    
    def connect_kafka(self):
        max_retries = 10
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                print(f"Connecting to Kafka at {self.config.KAFKA_BOOTSTRAP_SERVERS}")
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                    acks='all',
                    retries=3
                )
                print("Successfully connected to Kafka")
                return True
            except KafkaError as e:
                retry_count += 1
                print(f"Kafka connection attempt {retry_count}/{max_retries} failed: {e}")
                if retry_count < max_retries:
                    time.sleep(5)
                else:
                    print("Failed to connect to Kafka after maximum retries")
                    return False
    
    def serialize_document(self, doc):
        if doc is None:
            return None
        
        serialized = {}
        for key, value in doc.items():
            if isinstance(value, (ObjectId, datetime)):
                serialized[key] = str(value)
            elif isinstance(value, dict):
                serialized[key] = self.serialize_document(value)
            elif isinstance(value, list):
                serialized[key] = [
                    self.serialize_document(item) if isinstance(item, dict) else str(item) if isinstance(item, (ObjectId, datetime)) else item
                    for item in value
                ]
            else:
                serialized[key] = value
        return serialized
    
    def transform_change_event(self, change):
        event = {
            'operation': change.get('operationType'),
            'timestamp': datetime.utcnow().isoformat(),
            'database': change.get('ns', {}).get('db'),
            'collection': change.get('ns', {}).get('coll'),
            'document_key': str(change.get('documentKey', {}).get('_id')),
        }
        
        if change.get('operationType') == 'insert':
            doc = change.get('fullDocument', {})
            event['data'] = self.serialize_document(doc)
        
        elif change.get('operationType') == 'update':
            updated_fields = change.get('updateDescription', {}).get('updatedFields', {})
            event['updated_fields'] = self.serialize_document(updated_fields)
            event['removed_fields'] = change.get('updateDescription', {}).get('removedFields', [])
        
        elif change.get('operationType') == 'delete':
            event['data'] = None
        
        return event
    
    def print_captured_data(self, event):
        """Print detailed information about captured data"""
        self.event_counter += 1
        
        print("\n" + "="*80)
        print(f"CAPTURED EVENT #{self.event_counter}")
        print("="*80)
        print(f"Operation Type: {event['operation'].upper()}")
        print(f"Database: {event['database']}")
        print(f"Collection: {event['collection']}")
        print(f"Document ID: {event['document_key']}")
        print(f"Timestamp: {event['timestamp']}")
        
        if event['operation'] == 'insert':
            print("\n--- ORIGINAL DOCUMENT (from MongoDB) ---")
            print(json.dumps(event['data'], indent=2, default=str))
            
            # Extract key fields for quick view
            if 'data' in event and event['data']:
                data = event['data']
                print("\n--- KEY FIELDS ---")
                if 'order_id' in data:
                    print(f"  Order ID: {data.get('order_id')}")
                if 'user_id' in data:
                    print(f"  User ID: {data.get('user_id')}")
                if 'amount' in data:
                    print(f"  Amount: ${data.get('amount')}")
                if 'status' in data:
                    print(f"  Status: {data.get('status')}")
                if 'products' in data:
                    print(f"  Products: {len(data.get('products', []))} items")
                    for idx, product in enumerate(data.get('products', []), 1):
                        print(f"    {idx}. {product.get('product_name')} (qty: {product.get('quantity')}, price: ${product.get('price')})")
        
        elif event['operation'] == 'update':
            print("\n--- UPDATED FIELDS ---")
            print(json.dumps(event.get('updated_fields', {}), indent=2, default=str))
            if event.get('removed_fields'):
                print("\n--- REMOVED FIELDS ---")
                print(json.dumps(event['removed_fields'], indent=2, default=str))
        
        elif event['operation'] == 'delete':
            print("\n--- DELETED DOCUMENT ---")
            print(f"Document with ID {event['document_key']} was deleted")
        
        print("="*80 + "\n")
    
    def publish_to_kafka(self, event):
        try:
            self.print_captured_data(event)
            
            future = self.kafka_producer.send(self.config.KAFKA_TOPIC, value=event)
            record_metadata = future.get(timeout=10)
            
            print(f"PUBLISHED TO KAFKA")
            print(f"   Topic: {record_metadata.topic}")
            print(f"   Partition: {record_metadata.partition}")
            print(f"   Offset: {record_metadata.offset}")
            print(f"   Message Size: {len(json.dumps(event, default=str).encode('utf-8'))} bytes")
            print(f"   Destination: {self.config.KAFKA_BOOTSTRAP_SERVERS}")
            print()
            
            return True
        except KafkaError as e:
            print(f"FAILED to publish to Kafka: {e}")
            return False
    
    def watch_changes(self):
        db = self.mongo_client[self.config.MONGO_DATABASE]
        collection = db[self.config.MONGO_COLLECTION]
        
        pipeline = [
            {'$match': {'operationType': {'$in': ['insert', 'update', 'delete']}}}
        ]
        
        print("\n" + "="*80)
        print("CDC CONSUMER STARTED")
        print("="*80)
        print(f"Watching: {self.config.MONGO_DATABASE}.{self.config.MONGO_COLLECTION}")
        print(f"MongoDB: {self.config.MONGO_HOST}:{self.config.MONGO_PORT}")
        print(f"Kafka Topic: {self.config.KAFKA_TOPIC}")
        print(f"Kafka Brokers: {self.config.KAFKA_BOOTSTRAP_SERVERS}")
        print("="*80)
        print("\nWaiting for changes...\n")
        
        try:
            with collection.watch(pipeline, resume_after=self.resume_token) as stream:
                for change in stream:
                    self.resume_token = stream.resume_token
                    
                    event = self.transform_change_event(change)
                    self.publish_to_kafka(event)
                    
        except Exception as e:
            print(f"Error in change stream: {e}")
            raise
    
    def run(self):
        if not self.connect_mongodb():
            sys.exit(1)
        
        if not self.connect_kafka():
            sys.exit(1)
        
        try:
            self.watch_changes()
        except KeyboardInterrupt:
            print("\n\n" + "="*80)
            print(f"CDC Consumer Shutting Down")
            print(f"   Total Events Processed: {self.event_counter}")
            print("="*80)
        except Exception as e:
            print(f"Fatal error: {e}")
            sys.exit(1)
        finally:
            if self.kafka_producer:
                self.kafka_producer.flush()
                self.kafka_producer.close()
            if self.mongo_client:
                self.mongo_client.close()
            print("CDC consumer stopped\n")

if __name__ == "__main__":
    consumer = CDCConsumer()
    consumer.run()