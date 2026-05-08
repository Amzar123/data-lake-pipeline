import io
from minio import Minio
from minio.error import S3Error
from config import settings


class MinioClient:
    def __init__(self):
        self.client = Minio(
            endpoint=settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=False,
        )

    def ensure_bucket(self, bucket_name: str):
        """Create bucket if it doesn't exist."""
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)

    def upload_file(
        self,
        bucket_name: str,
        object_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ):
        """Upload bytes data to MinIO."""
        self.ensure_bucket(bucket_name)
        self.client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=io.BytesIO(data),
            length=len(data),
            content_type=content_type,
        )

    def list_files(self, bucket_name: str) -> list:
        """List all objects in a bucket."""
        self.ensure_bucket(bucket_name)
        objects = self.client.list_objects(bucket_name)
        return [
            {
                "name": obj.object_name,
                "size": obj.size,
                "last_modified": str(obj.last_modified),
            }
            for obj in objects
        ]

    def delete_file(self, bucket_name: str, object_name: str):
        """Delete an object from a bucket."""
        self.client.remove_object(bucket_name, object_name)

    def download_file(self, bucket_name: str, object_name: str) -> bytes:
        """Download an object and return as bytes."""
        response = self.client.get_object(bucket_name, object_name)
        return response.read()