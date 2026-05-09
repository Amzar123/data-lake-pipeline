import pandas as pd
import io
from pathlib import Path
from typing import Any, Dict, List, Optional
import logging
from minioclient.minio_client import MinioClient

logger = logging.getLogger(__name__)

RAW_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
PREPROCESSED_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "preprocessed"

# Default MinIO buckets
DEFAULT_RAW_BUCKET = "fintech-data-lake"
DEFAULT_OUTPUT_BUCKET = "fintech-data-warehouse"


def ensure_output_dir() -> None:
    """Create preprocessed data directory if it doesn't exist."""
    PREPROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)


def collect_raw_parquet_files(source_dir: Path = RAW_DATA_DIR) -> List[Path]:
    """Collect all parquet files from raw data directory."""
    if not source_dir.exists():
        raise FileNotFoundError(f"Raw data directory not found: {source_dir}")
    return sorted([p for p in source_dir.rglob("*.parquet")])


def list_minio_parquet_files(minio_client: MinioClient, bucket: str) -> List[str]:
    """List all parquet files in a MinIO bucket recursively (including nested folders)."""
    try:
        # Use direct minio client with recursive=True to traverse nested structure
        objects = minio_client.client.list_objects(bucket, recursive=True)
        parquet_files = sorted([obj.object_name for obj in objects if obj.object_name.endswith(".parquet")])
        return parquet_files
    except Exception as e:
        logger.error(f"Error listing objects in {bucket}: {str(e)}")
        raise


def _to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy")
    return buffer.getvalue()


def _normalize_raw_events(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.lower()

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    if "session_id" not in df.columns:
        df["session_id"] = df.get("session_id", pd.NA)

    required = ["event_id", "user_id", "timestamp"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    df = df.dropna(subset=required)
    df = df.drop_duplicates(subset=["event_id"])

    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply data cleaning transformations.
    - Remove duplicate rows
    - Handle missing values
    - Remove leading/trailing whitespace from string columns
    """
    # Remove duplicates
    df = df.drop_duplicates()

    # Clean string columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip() if df[col].dtype == "object" else df[col]

    # Handle missing values - forward fill for time series, then drop remaining
    df = df.fillna(method="ffill", limit=5)
    df = df.dropna()

    return df


def normalize_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply data normalization transformations.
    - Convert column names to lowercase
    - Standardize numeric columns (optional)
    - Ensure consistent data types
    """
    # Lowercase column names
    df.columns = df.columns.str.lower()

    # Convert numeric columns to consistent types
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    for col in numeric_cols:
        if df[col].dtype == "float64" and df[col].isna().sum() == 0:
            # Check if it's actually integer data
            if (df[col] == df[col].astype(int)).all():
                df[col] = df[col].astype("int64")

    return df


def preprocess_raw_file(raw_file: Path, output_dir: Path = PREPROCESSED_DATA_DIR) -> int:
    """
    Read, clean, and save a single parquet file (local).
    Returns the number of rows in the processed data.
    """
    ensure_output_dir()

    # Read raw parquet
    df = pd.read_parquet(raw_file)
    original_rows = len(df)

    # Apply transformations
    df = clean_data(df)
    df = normalize_data(df)

    processed_rows = len(df)

    # Build output path preserving directory structure
    rel_path = raw_file.relative_to(RAW_DATA_DIR)
    output_file = output_dir / rel_path

    # Create parent directories
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Save as parquet
    df.to_parquet(output_file, index=False, compression="snappy")

    logger.info(
        f"Preprocessed {raw_file.name}: {original_rows} rows → {processed_rows} rows | {output_file}"
    )

    return processed_rows


def preprocess_minio_file(
    minio_client: MinioClient,
    raw_bucket: str,
    object_name: str,
) -> pd.DataFrame:
    """
    Download a raw parquet file from MinIO and return a cleaned normalized DataFrame.
    """
    raw_data = minio_client.download_file(raw_bucket, object_name)
    df = pd.read_parquet(io.BytesIO(raw_data))
    df = _normalize_raw_events(df)
    df = clean_data(df)
    df = normalize_data(df)
    return df


def _build_events_flat(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(by=["timestamp", "user_id"]).reset_index(drop=True)


def _build_dau_metrics(df: pd.DataFrame) -> pd.DataFrame:
    daily = df.copy()
    daily["date"] = daily["timestamp"].dt.date
    daily["month"] = daily["timestamp"].dt.month

    by_day = (
        daily.groupby("date")
        .agg(
            daily_active_users=("user_id", "nunique"),
            daily_sessions=("session_id", "nunique"),
            daily_events=("event_id", "size"),
        )
        .reset_index()
    )
    by_day["events_per_user"] = by_day["daily_events"] / by_day["daily_active_users"]
    by_day["month"] = by_day["date"].apply(lambda d: d.month)

    monthly_active = (
        daily.groupby(daily["timestamp"].dt.month)["user_id"].nunique().rename("monthly_active_users")
    )
    by_day = by_day.merge(monthly_active, left_on="month", right_index=True)
    by_day["dau_mau_ratio"] = by_day["daily_active_users"] / by_day["monthly_active_users"]
    return by_day[["date", "daily_active_users", "daily_sessions", "daily_events", "events_per_user", "month", "monthly_active_users", "dau_mau_ratio"]]


def _build_revenue_by_city(df: pd.DataFrame) -> pd.DataFrame:
    transactions = df[df["amount"].notna()].copy()
    if transactions.empty:
        return pd.DataFrame(columns=["location_city", "users", "total_revenue", "avg_transaction"])

    by_city = (
        transactions.groupby("location_city")
        .agg(
            users=("user_id", "nunique"),
            total_revenue=("amount", "sum"),
            avg_transaction=("amount", "mean"),
        )
        .reset_index()
    )
    return by_city


def _build_revenue_by_month(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["month", "active_users", "total_revenue", "avg_transaction"])

    transactions = df[df["amount"].notna()].copy()
    monthly_active = (
        df.assign(month=df["timestamp"].dt.month)
        .groupby("month")["user_id"].nunique()
        .rename("active_users")
    )
    revenue = (
        transactions.assign(month=transactions["timestamp"].dt.month)
        .groupby("month")
        .agg(total_revenue=("amount", "sum"), avg_transaction=("amount", "mean"))
    )
    result = monthly_active.to_frame().join(revenue).reset_index()
    return result


def _build_revenue_by_segment(df: pd.DataFrame) -> pd.DataFrame:
    transactions = df[df["amount"].notna()].copy()
    if transactions.empty:
        return pd.DataFrame(columns=["user_segment", "users", "transactions", "total_revenue", "avg_transaction", "revenue_per_user"])

    users_by_segment = df.groupby("user_segment")["user_id"].nunique().rename("users")
    txn = (
        transactions.groupby("user_segment")
        .agg(
            transactions=("event_id", "count"),
            total_revenue=("amount", "sum"),
            avg_transaction=("amount", "mean"),
        )
    )
    result = users_by_segment.to_frame().join(txn).reset_index()
    result["revenue_per_user"] = result["total_revenue"] / result["users"].replace(0, 1)
    return result


def _build_user_lifecycle(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "user_id",
                "user_segment",
                "location_city",
                "device_type",
                "user_tenure_days",
                "total_events",
                "total_sessions",
                "active_days",
                "total_logins",
                "total_transactions",
                "total_amount_naira",
                "engagement_score",
                "last_active_date",
                "first_active_date",
                "days_since_active",
                "lifecycle_stage",
            ]
        )

    users = []
    now = df["timestamp"].max().date()

    for user_id, group in df.groupby("user_id"):
        segment = group["user_segment"].dropna().iloc[0] if "user_segment" in group.columns and not group["user_segment"].dropna().empty else None
        city = group["location_city"].dropna().iloc[0] if "location_city" in group.columns and not group["location_city"].dropna().empty else None
        device = group["device_type"].dropna().iloc[0] if "device_type" in group.columns and not group["device_type"].dropna().empty else None
        tenure = group["user_tenure_days"].dropna().iloc[0] if "user_tenure_days" in group.columns and not group["user_tenure_days"].dropna().empty else None

        first_active = group["timestamp"].dt.date.min()
        last_active = group["timestamp"].dt.date.max()
        days_since_active = (now - last_active).days
        total_events = len(group)
        total_sessions = group["session_id"].nunique()
        active_days = group["timestamp"].dt.date.nunique()
        total_logins = int((group["event_type"] == "login").sum()) if "event_type" in group.columns else 0
        total_transactions = int(group["amount"].notna().sum())
        total_amount_naira = float(group["amount"].fillna(0).sum())
        engagement_score = round((total_events + total_sessions + total_logins + total_transactions) / max(active_days, 1), 1)

        if days_since_active <= 7:
            lifecycle_stage = "Active"
        elif days_since_active <= 30:
            lifecycle_stage = "At-Risk"
        else:
            lifecycle_stage = "Churned"

        if total_events < 10 and days_since_active <= 7:
            lifecycle_stage = "New"

        users.append(
            {
                "user_id": user_id,
                "user_segment": segment,
                "location_city": city,
                "device_type": device,
                "user_tenure_days": int(tenure) if tenure is not None else None,
                "total_events": total_events,
                "total_sessions": total_sessions,
                "active_days": active_days,
                "total_logins": total_logins,
                "total_transactions": total_transactions,
                "total_amount_naira": total_amount_naira,
                "engagement_score": engagement_score,
                "last_active_date": last_active,
                "first_active_date": first_active,
                "days_since_active": days_since_active,
                "lifecycle_stage": lifecycle_stage,
            }
        )

    return pd.DataFrame(users)


def preprocess_all_raw_data(
    source_dir: Path = RAW_DATA_DIR, output_dir: Path = PREPROCESSED_DATA_DIR
) -> Dict[str, int]:
    """
    Preprocess all raw parquet files (local).
    Returns a summary dict with file counts and row statistics.
    """
    ensure_output_dir()

    raw_files = collect_raw_parquet_files(source_dir)

    if not raw_files:
        logger.warning(f"No parquet files found in {source_dir}")
        return {"files_processed": 0, "total_rows": 0, "files": {}}

    summary = {
        "files_processed": 0,
        "total_rows": 0,
        "files": {},
    }

    for raw_file in raw_files:
        try:
            row_count = preprocess_raw_file(raw_file, output_dir)
            rel_path = str(raw_file.relative_to(source_dir))
            summary["files"][rel_path] = row_count
            summary["total_rows"] += row_count
            summary["files_processed"] += 1
        except Exception as e:
            logger.error(f"Error processing {raw_file}: {str(e)}")
            summary["files"][str(raw_file)] = f"ERROR: {str(e)}"

    return summary


def preprocess_minio_data(
    raw_bucket: str = DEFAULT_RAW_BUCKET,
    output_bucket: str = DEFAULT_OUTPUT_BUCKET,
) -> Dict[str, Any]:
    """
    Preprocess all parquet files from MinIO raw bucket to output bucket.
    Returns a summary dict with file counts and row statistics.
    """
    minio = MinioClient()

    # Ensure output bucket exists
    minio.ensure_bucket(output_bucket)

    try:
        parquet_files = list_minio_parquet_files(minio, raw_bucket)
    except Exception as e:
        logger.error(f"Error listing files in {raw_bucket}: {str(e)}")
        return {"error": str(e), "files_processed": 0}

    if not parquet_files:
        logger.warning(f"No parquet files found in bucket {raw_bucket}")
        return {"files_processed": 0, "total_rows": 0, "files": {}}

    frames = []
    for object_name in parquet_files:
        try:
            df = preprocess_minio_file(minio, raw_bucket, object_name)
            frames.append(df)
        except Exception as e:
            logger.error(f"Error processing {object_name}: {str(e)}")

    if not frames:
        return {"files_processed": 0, "total_rows": 0, "files": {}}

    all_events = pd.concat(frames, ignore_index=True)
    all_events = all_events.drop_duplicates(subset=["event_id"])

    outputs = {
        "events_flat.parquet": _build_events_flat(all_events),
        "dau_matrics.parquet": _build_dau_metrics(all_events),
        "revenue_by_city.parquet": _build_revenue_by_city(all_events),
        "revenue_by_month.parquet": _build_revenue_by_month(all_events),
        "revenue_by_segment.parquet": _build_revenue_by_segment(all_events),
        "user_lifecycle.parquet": _build_user_lifecycle(all_events),
    }

    summary = {
        "files_processed": len(parquet_files),
        "raw_bucket": raw_bucket,
        "output_bucket": output_bucket,
        "outputs": {},
    }

    for filename, df in outputs.items():
        try:
            data = _to_parquet_bytes(df)
            minio.upload_file(output_bucket, filename, data, "application/octet-stream")
            summary["outputs"][filename] = {
                "rows": len(df),
                "columns": df.shape[1],
            }
        except Exception as e:
            logger.error(f"Error uploading {filename}: {str(e)}")
            summary["outputs"][filename] = {"error": str(e)}

    return summary

