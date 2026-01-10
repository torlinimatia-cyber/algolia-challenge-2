import os

class Config:
    GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
    BQ_DATASET = os.getenv('BQ_DATASET', 'etl_warehouse')
    GCS_BUCKET = os.getenv('GCS_BUCKET')
    
    LOCAL_PARQUET_DIR = os.getenv('LOCAL_PARQUET_DIR', '/output/orders')
    LOADER_MODE = os.getenv('LOADER_MODE', 'monitor')
    CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', '30'))