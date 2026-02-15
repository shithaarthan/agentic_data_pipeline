"""
MinIO Client for S3-compatible object storage.
Provides interface for lakehouse storage operations.
"""

import os
from typing import Optional, BinaryIO
from minio import Minio
from minio.error import S3Error
from loguru import logger

# Default configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# Lakehouse bucket
LAKEHOUSE_BUCKET = "lakehouse"


class MinIOClient:
    """MinIO client for lakehouse storage."""
    
    def __init__(
        self,
        endpoint: str = MINIO_ENDPOINT,
        access_key: str = MINIO_ACCESS_KEY,
        secret_key: str = MINIO_SECRET_KEY,
        secure: bool = MINIO_SECURE
    ):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.bucket = LAKEHOUSE_BUCKET
        self._ensure_bucket()
    
    def _ensure_bucket(self):
        """Create lakehouse bucket if it doesn't exist."""
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                logger.info(f"Created bucket: {self.bucket}")
        except S3Error as e:
            logger.error(f"Error creating bucket: {e}")
            raise
    
    def upload_file(self, local_path: str, object_name: str) -> bool:
        """Upload a file to the lakehouse bucket."""
        try:
            self.client.fput_object(self.bucket, object_name, local_path)
            logger.debug(f"Uploaded: {object_name}")
            return True
        except S3Error as e:
            logger.error(f"Upload failed: {e}")
            return False
    
    def upload_data(self, data: BinaryIO, object_name: str, length: int) -> bool:
        """Upload binary data to the lakehouse bucket."""
        try:
            self.client.put_object(self.bucket, object_name, data, length)
            logger.debug(f"Uploaded: {object_name}")
            return True
        except S3Error as e:
            logger.error(f"Upload failed: {e}")
            return False
    
    def download_file(self, object_name: str, local_path: str) -> bool:
        """Download a file from the lakehouse bucket."""
        try:
            self.client.fget_object(self.bucket, object_name, local_path)
            logger.debug(f"Downloaded: {object_name}")
            return True
        except S3Error as e:
            logger.error(f"Download failed: {e}")
            return False
    
    def list_objects(self, prefix: str = "", recursive: bool = True) -> list:
        """List objects in the lakehouse bucket."""
        try:
            objects = self.client.list_objects(
                self.bucket, 
                prefix=prefix, 
                recursive=recursive
            )
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"List failed: {e}")
            return []
    
    def delete_object(self, object_name: str) -> bool:
        """Delete an object from the lakehouse bucket."""
        try:
            self.client.remove_object(self.bucket, object_name)
            logger.debug(f"Deleted: {object_name}")
            return True
        except S3Error as e:
            logger.error(f"Delete failed: {e}")
            return False
    
    def get_presigned_url(self, object_name: str, expires_hours: int = 1) -> Optional[str]:
        """Get a presigned URL for an object."""
        from datetime import timedelta
        try:
            url = self.client.presigned_get_object(
                self.bucket,
                object_name,
                expires=timedelta(hours=expires_hours)
            )
            return url
        except S3Error as e:
            logger.error(f"Presign failed: {e}")
            return None


# Singleton instance
_client: Optional[MinIOClient] = None


def get_minio_client() -> MinIOClient:
    """Get or create MinIO client singleton."""
    global _client
    if _client is None:
        _client = MinIOClient()
    return _client


if __name__ == "__main__":
    # Test connection
    client = get_minio_client()
    print(f"Connected to MinIO at {MINIO_ENDPOINT}")
    print(f"Bucket: {LAKEHOUSE_BUCKET}")
    print(f"Objects: {client.list_objects()}")
