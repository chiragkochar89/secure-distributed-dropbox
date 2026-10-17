import io
import os
import logging
import asyncio
from typing import Dict, Optional
from minio import Minio

from app.config import settings
from app.storage.minio_client import StorageService, storage_service

logger = logging.getLogger(__name__)


class ObjectReplicationService:
    """
    Manages cross-node binary object payload replication between MinIO storage instances.
    Provides async peer replication with version-aware keys, retries, and local storage fallback.
    """

    def __init__(self, local_storage: Optional[StorageService] = None):
        self.local_storage = local_storage or storage_service

    @property
    def peer_endpoints(self) -> Dict[str, str]:
        """Dynamically parse peer endpoints from settings."""
        endpoints = {}
        if not settings.MINIO_PEER_ENDPOINTS:
            return endpoints

        parts = settings.MINIO_PEER_ENDPOINTS.split(",")
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if "=" in part:
                node_id, endpoint = part.split("=", 1)
                endpoints[node_id.strip()] = endpoint.strip()
            elif ":" in part:
                node_id = part.split(":")[0]
                endpoints[node_id.strip()] = part.strip()
        return endpoints

    async def replicate_object_to_peer(
        self,
        peer_id: str,
        peer_endpoint: str,
        object_key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        max_retries: int = 3
    ) -> bool:
        """
        Replicates raw object bytes to a specific peer's MinIO storage instance.
        Implements exponential backoff retry logic.
        """
        bucket_name = f"{peer_id}-bucket"

        for attempt in range(1, max_retries + 1):
            try:
                if self.local_storage.use_local_fallback:
                    # Local fallback mode (for unit testing without live MinIO docker)
                    peer_dir = os.path.join(os.getcwd(), "data", "storage", peer_id)
                    target_path = os.path.join(peer_dir, object_key.replace("/", "_"))
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)
                    with open(target_path, "wb") as f:
                        f.write(data)
                    logger.info(f"[Local Replication] Replicated {object_key} ({len(data)} bytes) to peer [{peer_id}]")
                    return True
                else:
                    peer_client = Minio(
                        endpoint=peer_endpoint,
                        access_key=settings.MINIO_ACCESS_KEY,
                        secret_key=settings.MINIO_SECRET_KEY,
                        secure=settings.MINIO_SECURE,
                    )
                    if not peer_client.bucket_exists(bucket_name):
                        peer_client.make_bucket(bucket_name)

                    data_stream = io.BytesIO(data)
                    peer_client.put_object(
                        bucket_name=bucket_name,
                        object_name=object_key,
                        data=data_stream,
                        length=len(data),
                        content_type=content_type
                    )
                    logger.info(f"[MinIO Replication] Replicated {object_key} ({len(data)} bytes) to peer [{peer_id}] at {peer_endpoint}")
                    return True

            except Exception as e:
                logger.warning(
                    f"Replication attempt {attempt}/{max_retries} failed for peer [{peer_id}] at {peer_endpoint}: {e}"
                )
                if attempt < max_retries:
                    await asyncio.sleep(0.05 * (2 ** (attempt - 1)))

        logger.error(f"Failed to replicate object {object_key} to peer [{peer_id}] after {max_retries} attempts.")
        return False

    async def replicate_object_to_all_peers(
        self,
        object_key: str,
        data: bytes,
        content_type: str = "application/octet-stream"
    ) -> Dict[str, bool]:
        """
        Asynchronously replicates object payload to all configured peer nodes.
        Execution runs concurrently for all peers and does not block Raft consensus.
        """
        results = {}
        endpoints = self.peer_endpoints
        if not endpoints:
            logger.info("No peer MinIO endpoints configured for replication.")
            return results

        tasks = []
        peer_ids = []
        for peer_id, endpoint in endpoints.items():
            tasks.append(self.replicate_object_to_peer(peer_id, endpoint, object_key, data, content_type))
            peer_ids.append(peer_id)

        statuses = await asyncio.gather(*tasks, return_exceptions=True)

        for peer_id, status in zip(peer_ids, statuses):
            results[peer_id] = status is True

        return results

    async def delete_object_from_peer(
        self,
        peer_id: str,
        peer_endpoint: str,
        object_key: str,
        max_retries: int = 3
    ) -> bool:
        """
        Deletes object payload from a specific peer's storage instance.
        Implements exponential backoff retry logic.
        """
        bucket_name = f"{peer_id}-bucket"
        for attempt in range(1, max_retries + 1):
            try:
                if self.local_storage.use_local_fallback:
                    peer_dir = os.path.join(os.getcwd(), "data", "storage", peer_id)
                    target_path = os.path.join(peer_dir, object_key.replace("/", "_"))
                    if os.path.exists(target_path):
                        os.remove(target_path)
                    logger.info(f"[Local Deletion] Deleted {object_key} from peer [{peer_id}]")
                    return True
                else:
                    peer_client = Minio(
                        endpoint=peer_endpoint,
                        access_key=settings.MINIO_ACCESS_KEY,
                        secret_key=settings.MINIO_SECRET_KEY,
                        secure=settings.MINIO_SECURE,
                    )
                    if peer_client.bucket_exists(bucket_name):
                        peer_client.remove_object(bucket_name, object_key)
                    logger.info(f"[MinIO Deletion] Deleted {object_key} from peer [{peer_id}] at {peer_endpoint}")
                    return True
            except Exception as e:
                logger.warning(
                    f"Delete attempt {attempt}/{max_retries} failed for peer [{peer_id}] at {peer_endpoint}: {e}"
                )
                if attempt < max_retries:
                    await asyncio.sleep(0.05 * (2 ** (attempt - 1)))

        logger.error(f"Failed to delete object {object_key} on peer [{peer_id}] after {max_retries} attempts.")
        return False

    async def delete_object_from_all_peers(self, object_key: str) -> Dict[str, bool]:
        """
        Asynchronously deletes object payload from all configured peer nodes.
        Execution runs concurrently for all peers.
        """
        results = {}
        endpoints = self.peer_endpoints
        if not endpoints:
            return results

        tasks = []
        peer_ids = []
        for peer_id, endpoint in endpoints.items():
            tasks.append(self.delete_object_from_peer(peer_id, endpoint, object_key))
            peer_ids.append(peer_id)

        statuses = await asyncio.gather(*tasks, return_exceptions=True)
        for peer_id, status in zip(peer_ids, statuses):
            results[peer_id] = status is True

        return results


replication_service = ObjectReplicationService()

