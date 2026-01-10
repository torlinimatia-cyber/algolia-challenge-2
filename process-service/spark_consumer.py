from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType
from config import Config
import sys
import time

class SparkCDCProcessor:
    def __init__(self):
        self.config = Config()
        self.spark = None
        self.batch_counter = 0
        
    def create_spark_session(self):
        print("Creating Spark session...")
        self.spark = SparkSession.builder \
            .appName(self.config.SPARK_APP_NAME) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.sql.streaming.checkpointLocation", self.config.CHECKPOINT_LOCATION) \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        print("Spark session created successfully")
        return self.spark
    
    def define_schema(self):
        item_schema = StructType([
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True)
        ])
        
        data_schema = StructType([
            StructField("_id", StringType(), True),
            StructField("order_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("items", ArrayType(item_schema), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True)
        ])
        
        cdc_schema = StructType([
            StructField("operation", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("database", StringType(), True),
            StructField("collection", StringType(), True),
            StructField("document_key", StringType(), True),
            StructField("data", data_schema, True)
        ])
        
        return cdc_schema
    
    def read_from_kafka(self):
        print(f"Reading from Kafka topic: {self.config.KAFKA_TOPIC}")
        print(f"Kafka brokers: {self.config.KAFKA_BOOTSTRAP_SERVERS}")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", self.config.KAFKA_TOPIC) \
            .option("startingOffsets", "earliest") \
            .load()
        
        return df
    
    def transform_data(self, df):
        print("Applying transformations...")
        
        cdc_schema = self.define_schema()
        
        parsed_df = df.select(
            from_json(col("value").cast("string"), cdc_schema).alias("cdc_event")
        )
        
        flattened_df = parsed_df.select(
            col("cdc_event.operation").alias("operation"),
            col("cdc_event.timestamp").alias("cdc_timestamp"),
            col("cdc_event.data._id").alias("mongodb_id"),
            col("cdc_event.data.order_id").alias("order_id"),
            col("cdc_event.data.user_id").alias("user_id"),
            col("cdc_event.data.amount").alias("amount"),
            col("cdc_event.data.status").alias("status"),
            col("cdc_event.data.items").alias("items"),
            to_timestamp(col("cdc_event.data.created_at")).alias("created_at"),
            to_timestamp(col("cdc_event.data.updated_at")).alias("updated_at"),
            current_timestamp().alias("processed_at")
        )
        
        filtered_df = flattened_df.filter(col("operation") == "insert")
        
        exploded_df = filtered_df.select(
            col("mongodb_id"),
            col("order_id"),
            col("user_id"),
            col("amount"),
            col("status"),
            explode(col("items")).alias("item"),
            col("created_at"),
            col("updated_at"),
            col("processed_at")
        )
        
        final_df = exploded_df.select(
            col("mongodb_id"),
            col("order_id"),
            col("user_id"),
            col("amount"),
            col("status"),
            col("item.product_id").alias("product_id"),
            col("item.product_name").alias("product_name"),
            col("item.quantity").alias("quantity"),
            col("item.price").alias("price"),
            (col("item.quantity") * col("item.price")).alias("line_total"),
            col("created_at"),
            col("updated_at"),
            col("processed_at")
        )
        
        return final_df
    
    def print_batch_data(self, batch_df, batch_id):
        """Print detailed information about processed batch"""
        self.batch_counter += 1
        
        print("\n" + "="*80)
        print(f"PROCESSING BATCH #{self.batch_counter} (Batch ID: {batch_id})")
        print("="*80)
        
        rows = batch_df.collect()
        row_count = len(rows)
        
        print(f"Batch Size: {row_count} rows")
        print(f"Processing Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if row_count > 0:
            print("\n--- INPUT DATA (from Kafka) ---")
            print(f"Source: Kafka topic '{self.config.KAFKA_TOPIC}'")
            print(f"Records received: {row_count}")
            
            print("\n--- OUTPUT SCHEMA ---")
            batch_df.printSchema()
            
            print("\n--- TRANSFORMED DATA (first 5 rows) ---")
            for idx, row in enumerate(rows[:5], 1):
                print(f"\nRow {idx}:")
                print(f"  MongoDB ID: {row['mongodb_id']}")
                print(f"  Order ID: {row['order_id']}")
                print(f"  User ID: {row['user_id']}")
                print(f"  Product: {row['product_name']} (ID: {row['product_id']})")
                print(f"  Quantity: {row['quantity']}")
                print(f"  Price: ${row['price']:.2f}")
                print(f"  Line Total: ${row['line_total']:.2f}")
                print(f"  Order Amount: ${row['amount']:.2f}")
                print(f"  Status: {row['status']}")
                print(f"  Created: {row['created_at']}")
                print(f"  Processed: {row['processed_at']}")
            
            if row_count > 5:
                print(f"\n  ... and {row_count - 5} more rows")
            
            print("\n--- BATCH STATISTICS ---")
            total_amount = sum(row['line_total'] for row in rows)
            unique_orders = len(set(row['order_id'] for row in rows))
            unique_products = len(set(row['product_id'] for row in rows))
            
            print(f"  Unique Orders: {unique_orders}")
            print(f"  Unique Products: {unique_products}")
            print(f"  Total Line Amount: ${total_amount:.2f}")
            print(f"  Average Line Total: ${total_amount/row_count:.2f}")
            
            print("\n--- TRANSFORMATIONS APPLIED ---")
            print("  1. Parsed JSON from Kafka message")
            print("  2. Flattened CDC event structure")
            print("  3. Filtered for 'insert' operations only")
            print("  4. Exploded items array (denormalized)")
            print("  5. Calculated line_total (quantity * price)")
            print("  6. Added processed_at timestamp")
            
            print(f"\n--- OUTPUT DESTINATION ---")
            print(f"  Format: Parquet")
            print(f"  Path: {self.config.OUTPUT_PATH}")
            print(f"  Mode: Append")
        else:
            print("No data in this batch")
        
        print("="*80 + "\n")
    
    def write_to_warehouse(self, df):
        print(f"Writing to warehouse at: {self.config.OUTPUT_PATH}")
        
        def process_batch(batch_df, batch_id):
            if batch_df.count() > 0:
                self.print_batch_data(batch_df, batch_id)
                
                batch_df.write \
                    .mode("append") \
                    .parquet(self.config.OUTPUT_PATH)
                
                print(f"BATCH #{self.batch_counter} WRITTEN TO PARQUET")
                print(f"  Location: {self.config.OUTPUT_PATH}")
                print(f"  Rows written: {batch_df.count()}\n")
        
        query = df \
            .writeStream \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", self.config.CHECKPOINT_LOCATION) \
            .trigger(processingTime="10 seconds") \
            .start()
        
        return query
    
    def run(self):
        try:
            print("\n" + "="*80)
            print("SPARK PROCESSOR STARTED")
            print("="*80)
            print(f"Application: {self.config.SPARK_APP_NAME}")
            print(f"Kafka Source: {self.config.KAFKA_BOOTSTRAP_SERVERS}")
            print(f"Kafka Topic: {self.config.KAFKA_TOPIC}")
            print(f"Output Path: {self.config.OUTPUT_PATH}")
            print(f"Checkpoint: {self.config.CHECKPOINT_LOCATION}")
            print("="*80 + "\n")
            
            self.create_spark_session()
            
            kafka_df = self.read_from_kafka()
            
            transformed_df = self.transform_data(kafka_df)
            
            query = self.write_to_warehouse(transformed_df)
            
            print("Spark streaming job started successfully")
            print("Waiting for data from Kafka...\n")
            
            query.awaitTermination()
            
        except KeyboardInterrupt:
            print("\n\n" + "="*80)
            print("Spark Processor Shutting Down")
            print(f"   Total Batches Processed: {self.batch_counter}")
            print("="*80)
        except Exception as e:
            print(f"Fatal error: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        finally:
            if self.spark:
                self.spark.stop()
            print("Spark processor stopped\n")

if __name__ == "__main__":
    processor = SparkCDCProcessor()
    processor.run()