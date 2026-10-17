import io
import os
import logging
from typing import Optional
from minio import Minio
from minio.error import S3Error
from app.config import settings

logger = logging.getLogger(__name__)


class StorageService:
    """
    Storage Service abstraction wrapping MinIO / S3 object storage
    with local directory fallback for local execution environments.
    """

    def __init__(self):
        self.bucket_name = settings.MINIO_BUCKET
        self.use_local_fallback = False
        self.local_storage_dir = os.path.join(os.getcwd(), "data", "storage")
        
        try:
            self.client = Minio(
                endpoint=settings.MINIO_ENDPOINT,
                access_key=settings.MINIO_ACCESS_KEY,
                secret_key=settings.MINIO_SECRET_KEY,
                secure=settings.MINIO_SECURE,
            )
            self._ensure_bucket_exists()
        except Exception as e:
            logger.warning(
                f"MinIO storage server not available at {settings.MINIO_ENDPOINT}: {e}. "
                f"Activating local filesystem storage fallback at {self.local_storage_dir}"
            )
            self.use_local_fallback = True
            os.makedirs(self.local_storage_dir, exist_ok=True)

    def _ensure_bucket_exists(self):
        """Ensure default MinIO bucket exists."""
        if not self.use_local_fallback:
            try:
                if not self.client.bucket_exists(self.bucket_name):
                    self.client.make_bucket(self.bucket_name)
                    logger.info(f"Created MinIO bucket: {self.bucket_name}")
            except Exception as e:
                logger.warning(f"Could not check/create MinIO bucket: {e}. Switching to local storage fallback.")
                self.use_local_fallback = True
                os.makedirs(self.local_storage_dir, exist_ok=True)

    def upload_object(self, object_key: str, data: bytes, content_type: str = "application/octet-stream") -> bool:
        """Upload raw file object bytes to storage."""
        if self.use_local_fallback:
            target_path = os.path.join(self.local_storage_dir, object_key.replace("/", "_"))
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            with open(target_path, "wb") as f:
                f.write(data)
            logger.info(f"[Local Storage] Uploaded object: {object_key} ({len(data)} bytes)")
            return True
        else:
            try:
                data_stream = io.BytesIO(data)
                self.client.put_object(
                    bucket_name=self.bucket_name,
                    object_name=object_key,
                    data=data_stream,
                    length=len(data),
                    content_type=content_type
                )
                logger.info(f"[MinIO Storage] Uploaded object: {object_key} ({len(data)} bytes)")
                return True
            except S3Error as e:
                logger.error(f"MinIO upload error for key {object_key}: {e}")
                raise e

    def download_object(self, object_key: str) -> bytes:
        """Retrieve raw file object bytes from storage."""
        if self.use_local_fallback:
            target_path = os.path.join(self.local_storage_dir, object_key.replace("/", "_"))
            if not os.path.exists(target_path):
                raise FileNotFoundError(f"Object {object_key} not found in local storage.")
            with open(target_path, "rb") as f:
                return f.read()
        else:
            try:
                response = self.client.get_object(self.bucket_name, object_key)
                return response.read()
            finally:
                if 'response' in locals() and response:
                    response.close()
                    response.release_conn()

    def delete_object(self, object_key: str) -> bool:
        """Remove file object from storage (compensating transaction / hard delete)."""
        if self.use_local_fallback:
            target_path = os.path.join(self.local_storage_dir, object_key.replace("/", "_"))
            if os.path.exists(target_path):
                os.remove(target_path)
                logger.info(f"[Local Storage] Deleted object: {object_key}")
            return True
        else:
            try:
                self.client.remove_object(self.bucket_name, object_key)
                logger.info(f"[MinIO Storage] Deleted object: {object_key}")
                return True
            except S3Error as e:
                logger.error(f"MinIO delete error for key {object_key}: {e}")
                return False


storage_service = StorageService()
