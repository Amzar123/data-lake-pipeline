from pathlib import Path
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from minioclient.minio_client import MinioClient
from airflow.airflow_client import AirflowClient
from pipeline.pipeline_ingest import RAW_DATA_DIR, upload_raw_directory
from pipeline.pipeline_visualization import run_visualization_pipeline
import uvicorn

app = FastAPI(title="Pipeline Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

minio = MinioClient()
airflow = AirflowClient()


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.post("/upload/{bucket_name}")
async def upload_file(bucket_name: str, file: UploadFile = File(...)):
    """Upload a file to MinIO and trigger an Airflow DAG."""
    try:
        # Upload file to MinIO
        object_name = file.filename
        file_data = await file.read()
        minio.upload_file(bucket_name, object_name, file_data, file.content_type)

        # Trigger Airflow DAG after upload
        dag_run = airflow.trigger_dag(
            dag_id="minio_pipeline",
            conf={"bucket": bucket_name, "object_name": object_name},
        )

        return {
            "message": "File uploaded and pipeline triggered",
            "bucket": bucket_name,
            "object_name": object_name,
            "dag_run_id": dag_run.get("dag_run_id"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/files/{bucket_name}")
def list_files(bucket_name: str):
    """List all files in a MinIO bucket."""
    try:
        files = minio.list_files(bucket_name)
        return {"bucket": bucket_name, "files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pipeline/upload/raw/{bucket_name}")
def upload_raw_data(bucket_name: str, source_dir: Optional[str] = None):
    """Upload all local raw files from data/raw to MinIO via API."""
    try:
        target_dir = Path(source_dir).expanduser().resolve() if source_dir else RAW_DATA_DIR
        count = upload_raw_directory(bucket_name, target_dir)
        return {
            "message": "Raw data uploaded to MinIO",
            "bucket": bucket_name,
            "source_dir": str(target_dir),
            "uploaded_files": count,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/files/{bucket_name}/{object_name}")
def delete_file(bucket_name: str, object_name: str):
    """Delete a file from MinIO."""
    try:
        minio.delete_file(bucket_name, object_name)
        return {"message": f"{object_name} deleted from {bucket_name}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pipeline/preprocess")
def preprocess_raw_to_clean(
    raw_bucket: str = "fintech-data-lake",
    output_bucket: str = "fintech-data-warehouse",
):
    """Preprocess all raw data from MinIO bucket into clean, analysis-ready format in output bucket."""
    try:
        summary = preprocess_minio_data(raw_bucket, output_bucket)
        return {
            "message": "Raw data preprocessed successfully",
            "summary": summary,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pipeline/visualize")
def run_visualization():
    """Load preprocessed data to PostgreSQL and setup Superset visualizations."""
    try:
        run_visualization_pipeline()
        return {
            "message": "Visualization pipeline completed"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dags")
def list_dags():
    """List all available Airflow DAGs."""
    try:
        return airflow.list_dags()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/dags/{dag_id}/trigger")
def trigger_dag(dag_id: str, conf: dict = {}):
    """Manually trigger an Airflow DAG."""
    try:
        return airflow.trigger_dag(dag_id=dag_id, conf=conf)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dags/{dag_id}/runs")
def get_dag_runs(dag_id: str):
    """Get all runs for a specific DAG."""
    try:
        return airflow.get_dag_runs(dag_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)