import os

class Config:
    MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
    MONGO_PORT = int(os.getenv('MONGO_PORT', '27017'))
    MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'etl_db')
    MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'orders')
    
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'orders-cdc')
    
    @property
    def mongo_uri(self):
        return f"mongodb://{self.MONGO_HOST}:{self.MONGO_PORT}/"