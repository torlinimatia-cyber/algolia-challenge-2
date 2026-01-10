import os

class Config:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'orders-cdc')
    
    CHECKPOINT_LOCATION = os.getenv('CHECKPOINT_LOCATION', '/tmp/spark-checkpoints')
    OUTPUT_PATH = os.getenv('OUTPUT_PATH', '/output/orders')
    
    SPARK_APP_NAME = os.getenv('SPARK_APP_NAME', 'OrdersCDCProcessor')