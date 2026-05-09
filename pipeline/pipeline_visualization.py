import pandas as pd
import sqlalchemy as sa
import os
from dotenv import load_dotenv
from minioclient.minio_client import MinioClient
import logging
import requests
import json
import io

load_dotenv()

logger = logging.getLogger(__name__)

# PostgreSQL connection
def get_postgres_engine():
    url = f"postgresql://{os.getenv('POSTGRES_USER', 'airflow')}:{os.getenv('POSTGRES_PASSWORD', 'airflow')}@{os.getenv('POSTGRES_HOST', 'postgres')}/{os.getenv('POSTGRES_DB', 'fintech_warehouse')}"
    return sa.create_engine(url)

# Load data from MinIO to PostgreSQL
def load_data_to_postgres():
    minio = MinioClient()
    engine = get_postgres_engine()

    files = [
        "events_flat.parquet",
        "dau_matrics.parquet",
        "revenue_by_city.parquet",
        "revenue_by_month.parquet",
        "revenue_by_segment.parquet",
        "user_lifecycle.parquet"
    ]

    for file in files:
        try:
            data = minio.download_file("fintech-data-warehouse", file)
            df = pd.read_parquet(io.BytesIO(data))
            table_name = file.replace(".parquet", "").replace("_", "")
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            logger.info(f"Loaded {file} to table {table_name}")
        except Exception as e:
            logger.error(f"Error loading {file}: {e}")

# Main function
def run_visualization_pipeline():
    load_data_to_postgres()

if __name__ == "__main__":
    run_visualization_pipeline()