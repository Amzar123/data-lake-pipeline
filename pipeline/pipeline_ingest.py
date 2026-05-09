import argparse
import mimetypes
import sys
from pathlib import Path
from typing import List

try:
    from minioclient.minio_client import MinioClient
except ImportError:
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from minioclient.minio_client import MinioClient

RAW_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"


def collect_raw_files(source_dir: Path) -> List[Path]:
    """Return all regular files under the raw data directory."""
    if not source_dir.exists():
        raise FileNotFoundError(f"Raw data directory not found: {source_dir}")
    return [path for path in source_dir.rglob("*") if path.is_file()]


def build_object_name(source_path: Path, root_dir: Path) -> str:
    """Build a MinIO object name preserving the directory structure under the raw root."""
    return str(source_path.relative_to(root_dir)).replace("\\", "/")


def upload_raw_directory(bucket_name: str, source_dir: Path = RAW_DATA_DIR) -> int:
    """Upload all files from the raw data directory into the given MinIO bucket."""
    minio = MinioClient()
    raw_files = collect_raw_files(source_dir)

    if not raw_files:
        print(f"No files found in raw data directory: {source_dir}")
        return 0

    uploaded_count = 0
    for raw_file in raw_files:
        object_name = build_object_name(raw_file, source_dir)
        content_type, _ = mimetypes.guess_type(raw_file.name)
        content_type = content_type or "application/octet-stream"

        with raw_file.open("rb") as file_handle:
            data = file_handle.read()

        minio.upload_file(bucket_name, object_name, data, content_type)
        print(f"Uploaded {raw_file} → {bucket_name}/{object_name} ({content_type})")
        uploaded_count += 1

    return uploaded_count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload all files from data/raw to MinIO."
    )
    parser.add_argument(
        "bucket",
        help="The MinIO bucket name to upload raw files into.",
    )
    parser.add_argument(
        "--source-dir",
        default=str(RAW_DATA_DIR),
        help="Local raw data directory to upload (default: data/raw).",
    )

    args = parser.parse_args()
    source_dir = Path(args.source_dir).expanduser().resolve()

    count = upload_raw_directory(args.bucket, source_dir)
    print(f"Finished uploading {count} file(s) to bucket '{args.bucket}'")


if __name__ == "__main__":
    main()
