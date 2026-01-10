from google.cloud import bigquery
from google.cloud import storage
import os
import time
import sys
import glob
from config import Config

class BigQueryLoader:
    def __init__(self):
        self.config = Config()
        
        if not self.config.GCP_PROJECT_ID:
            raise ValueError("GCP_PROJECT_ID environment variable is required")
        if not self.config.GCS_BUCKET:
            raise ValueError("GCS_BUCKET environment variable is required")
        
        self.bq_client = bigquery.Client(project=self.config.GCP_PROJECT_ID)
        self.storage_client = storage.Client(project=self.config.GCP_PROJECT_ID)
        
        self.processed_files = set()
        
    def create_tables(self):
        print("Creating BigQuery tables if they don't exist...")
        
        orders_fact_schema = [
            bigquery.SchemaField("mongodb_id", "STRING"),
            bigquery.SchemaField("order_id", "INTEGER"),
            bigquery.SchemaField("user_id", "INTEGER"),
            bigquery.SchemaField("amount", "FLOAT"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("product_id", "INTEGER"),
            bigquery.SchemaField("product_name", "STRING"),
            bigquery.SchemaField("quantity", "INTEGER"),
            bigquery.SchemaField("price", "FLOAT"),
            bigquery.SchemaField("line_total", "FLOAT"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
            bigquery.SchemaField("processed_at", "TIMESTAMP"),
        ]
        
        table_id = f"{self.config.GCP_PROJECT_ID}.{self.config.BQ_DATASET}.orders_fact"
        table = bigquery.Table(table_id, schema=orders_fact_schema)
        
        try:
            table = self.bq_client.create_table(table)
            print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
        except Exception as e:
            if "Already Exists" in str(e):
                print(f"Table {table_id} already exists")
            else:
                print(f"Error creating table: {e}")
                raise
    
    def upload_parquet_to_gcs(self, local_path, gcs_path):
        print(f"Uploading {local_path} to gs://{self.config.GCS_BUCKET}/{gcs_path}")
        
        try:
            bucket = self.storage_client.bucket(self.config.GCS_BUCKET)
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            print(f"Upload complete: gs://{self.config.GCS_BUCKET}/{gcs_path}")
            return True
        except Exception as e:
            print(f"Error uploading to GCS: {e}")
            return False
    
    def load_from_gcs_to_bigquery(self, gcs_uri):
        print(f"Loading from {gcs_uri} to BigQuery...")
        
        table_id = f"{self.config.GCP_PROJECT_ID}.{self.config.BQ_DATASET}.orders_fact"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        
        try:
            load_job = self.bq_client.load_table_from_uri(
                gcs_uri,
                table_id,
                job_config=job_config
            )
            
            load_job.result()
            
            destination_table = self.bq_client.get_table(table_id)
            print(f"Loaded {load_job.output_rows} rows into {table_id}")
            print(f"Total rows in table: {destination_table.num_rows}")
            return True
        except Exception as e:
            print(f"Error loading to BigQuery: {e}")
            return False
    
    def get_new_parquet_files(self):
        parquet_files = glob.glob(f"{self.config.LOCAL_PARQUET_DIR}/*.parquet")
        new_files = [f for f in parquet_files if f not in self.processed_files]
        return new_files
    
    def process_file(self, parquet_file):
        filename = os.path.basename(parquet_file)
        gcs_path = f"orders/{filename}"
        
        if self.upload_parquet_to_gcs(parquet_file, gcs_path):
            gcs_uri = f"gs://{self.config.GCS_BUCKET}/{gcs_path}"
            
            if self.load_from_gcs_to_bigquery(gcs_uri):
                self.processed_files.add(parquet_file)
                return True
        
        return False
    
    def run_summary_query(self):
        print("\nRunning summary aggregation query...")
        
        query = f"""
        CREATE OR REPLACE TABLE `{self.config.GCP_PROJECT_ID}.{self.config.BQ_DATASET}.orders_summary` AS
        SELECT
            user_id,
            COUNT(DISTINCT order_id) as total_orders,
            SUM(line_total) as total_amount,
            SUM(quantity) as total_items,
            MAX(created_at) as last_order_date,
            CURRENT_TIMESTAMP() as computed_at
        FROM `{self.config.GCP_PROJECT_ID}.{self.config.BQ_DATASET}.orders_fact`
        GROUP BY user_id
        ORDER BY total_amount DESC
        """
        
        try:
            query_job = self.bq_client.query(query)
            query_job.result()
            
            print("Summary table created successfully")
            
            results = self.bq_client.query(f"""
                SELECT * FROM `{self.config.GCP_PROJECT_ID}.{self.config.BQ_DATASET}.orders_summary`
                LIMIT 10
            """).result()
            
            print("\nTop 10 users by total amount:")
            for row in results:
                print(f"User {row.user_id}: {row.total_orders} orders, ${row.total_amount:.2f}")
        except Exception as e:
            print(f"Error running summary query: {e}")
    
    def monitor_and_load(self):
        print(f"Starting continuous monitoring of {self.config.LOCAL_PARQUET_DIR}")
        print(f"Checking for new files every {self.config.CHECK_INTERVAL} seconds")
        print("Press Ctrl+C to stop\n")
        
        try:
            while True:
                new_files = self.get_new_parquet_files()
                
                if new_files:
                    print(f"\nFound {len(new_files)} new file(s)")
                    
                    for parquet_file in new_files:
                        try:
                            print(f"\nProcessing: {parquet_file}")
                            self.process_file(parquet_file)
                        except Exception as e:
                            print(f"Error processing {parquet_file}: {e}")
                else:
                    print(".", end="", flush=True)
                
                time.sleep(self.config.CHECK_INTERVAL)
                
        except KeyboardInterrupt:
            print("\n\nStopping monitor...")
            self.run_summary_query()
    
    def load_all_existing(self):
        print("Loading all existing Parquet files...")
        
        parquet_files = glob.glob(f"{self.config.LOCAL_PARQUET_DIR}/*.parquet")
        
        if not parquet_files:
            print("No Parquet files found")
            return
        
        print(f"Found {len(parquet_files)} Parquet file(s)")
        
        success_count = 0
        for parquet_file in parquet_files:
            if self.process_file(parquet_file):
                success_count += 1
        
        print(f"\nSuccessfully processed {success_count}/{len(parquet_files)} files")
        
        if success_count > 0:
            self.run_summary_query()

def main():
    loader = BigQueryLoader()
    
    loader.create_tables()
    
    if loader.config.LOADER_MODE == 'once':
        loader.load_all_existing()
    elif loader.config.LOADER_MODE == 'monitor':
        loader.monitor_and_load()
    else:
        print(f"Unknown mode: {loader.config.LOADER_MODE}")
        sys.exit(1)

if __name__ == "__main__":
    main()