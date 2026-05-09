from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio
import json


def read_from_minio(**context):
    """Read file from MinIO passed via DAG conf."""
    conf = context["dag_run"].conf or {}
    bucket = conf.get("bucket", "default-bucket")
    object_name = conf.get("object_name", "")

    client = Minio(
        endpoint="minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )

    response = client.get_object(bucket, object_name)
    data = response.read().decode("utf-8")
    print(f"[READ] Read {len(data)} bytes from {bucket}/{object_name}")

    # Push data to XCom for next task
    context["ti"].xcom_push(key="raw_data", value=data)
    context["ti"].xcom_push(key="object_name", value=object_name)
    context["ti"].xcom_push(key="bucket", value=bucket)


def process_data(**context):
    """Process the raw data (transform/clean logic goes here)."""
    ti = context["ti"]
    raw_data = ti.xcom_pull(key="raw_data", task_ids="read_from_minio")
    object_name = ti.xcom_pull(key="object_name", task_ids="read_from_minio")

    # Example: count lines and words
    lines = raw_data.splitlines()
    word_count = sum(len(line.split()) for line in lines)

    result = {
        "object_name": object_name,
        "line_count": len(lines),
        "word_count": word_count,
        "processed": True,
    }

    print(f"[PROCESS] Result: {json.dumps(result, indent=2)}")
    ti.xcom_push(key="processed_result", value=result)


def save_result_to_minio(**context):
    """Save processed result back to MinIO as a JSON file."""
    import io

    ti = context["ti"]
    result = ti.xcom_pull(key="processed_result", task_ids="process_data")
    bucket = ti.xcom_pull(key="bucket", task_ids="read_from_minio")

    client = Minio(
        endpoint="minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )

    output_name = f"results/{result['object_name']}_result.json"
    output_data = json.dumps(result, indent=2).encode("utf-8")

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    client.put_object(
        bucket_name=bucket,
        object_name=output_name,
        data=io.BytesIO(output_data),
        length=len(output_data),
        content_type="application/json",
    )

    print(f"[SAVE] Result saved to {bucket}/{output_name}")


with DAG(
    dag_id="minio_pipeline",
    description="Read file from MinIO → Process → Save result back to MinIO",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Only triggered manually or via API
    catchup=False,
    tags=["pipeline", "minio"],
) as dag:

    task_read = PythonOperator(
        task_id="read_from_minio",
        python_callable=read_from_minio,
    )

    task_process = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

    task_save = PythonOperator(
        task_id="save_result_to_minio",
        python_callable=save_result_to_minio,
    )

    # Define task order
    task_read >> task_process >> task_save