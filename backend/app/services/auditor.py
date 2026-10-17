import os
import asyncio
import logging
from typing import Dict, List, Optional
from sqlalchemy.orm import Session

from app.config import settings
from app.db.session import SessionLocal
from app.db.models.file import FileMetadata, FileVersion, PendingObjectCleanup
from app.core.crypto import compute_sha256
from app.core.audit import log_audit_event
from app.storage.minio_client import storage_service
from app.storage.replication_service import replication_service
from app.raft.node import RaftNode, NodeRole

logger = logging.getLogger("secure-raft-dropbox.auditor")


class ReplicaAuditor:
    """
    Leader-bound Active Replica Auditor & Self-Healing Engine.
    Periodically verifies SHA-256 hashes of object payloads across cluster nodes
    against committed PostgreSQL file metadata, and repairs missing/corrupted replicas.
    """

    def __init__(
        self,
        node: Optional[RaftNode] = None,
        audit_interval_seconds: float = 60.0,
        batch_size: int = 50,
        max_concurrent_repairs: int = 3
    ):
        self.node = node
        self.audit_interval_seconds = audit_interval_seconds
        self.batch_size = batch_size
        self.max_concurrent_repairs = max_concurrent_repairs
        self._audit_task: Optional[asyncio.Task] = None
        self._audit_lock = asyncio.Lock()
        self.is_running = False

    def start(self):
        """Start the background auditing task if node is active leader."""
        if self.is_running:
            return
        self.is_running = True
        if self._audit_task and not self._audit_task.done():
            self._audit_task.cancel()
        self._audit_task = asyncio.create_task(self._audit_loop())
        logger.info(f"ReplicaAuditor started (Interval: {self.audit_interval_seconds}s, Batch: {self.batch_size})")

    async def stop(self):
        """Stop and cancel the background auditing task immediately."""
        self.is_running = False
        if self._audit_task and not self._audit_task.done():
            self._audit_task.cancel()
            try:
                await self._audit_task
            except asyncio.CancelledError:
                pass
        logger.info("ReplicaAuditor stopped.")

    async def _audit_loop(self):
        """Background auditing loop running periodically on leader node."""
        # Wait initial audit interval before first periodic sweep to allow leader to establish authority and heartbeats
        try:
            await asyncio.sleep(self.audit_interval_seconds)
        except asyncio.CancelledError:
            return

        while self.is_running:
            try:
                # Check leadership before running audit cycle
                if self.node and self.node.role != NodeRole.LEADER:
                    logger.info("Node is no longer leader. ReplicaAuditor stopping loop.")
                    self.is_running = False
                    break

                await self.run_audit_cycle()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error during audit cycle: {e}", exc_info=True)

            try:
                await asyncio.sleep(self.audit_interval_seconds)
            except asyncio.CancelledError:
                break

    async def run_audit_cycle(self) -> int:
        """
        Executes a single audit cycle.
        Guarantees single execution using self._audit_lock (prevents overlapping cycles).
        Returns number of file versions audited.
        """
        if self._audit_lock.locked():
            logger.warning("Previous audit cycle still in progress. Skipping overlapping cycle.")
            return 0

        async with self._audit_lock:
            # Drain durable pending object cleanups first
            try:
                await self.process_pending_cleanups()
            except Exception as cln_err:
                logger.error(f"Error during process_pending_cleanups: {cln_err}")

            db: Session = SessionLocal()
            try:
                # 1. Fetch active committed file versions from PostgreSQL (bounded batch)
                versions = (
                    db.query(FileVersion)
                    .join(FileMetadata, FileVersion.file_id == FileMetadata.id)
                    .filter(FileMetadata.is_deleted == False)
                    .limit(self.batch_size)
                    .all()
                )

                if not versions:
                    return 0

                logger.info(f"Starting audit cycle for {len(versions)} file versions...")

                sem = asyncio.Semaphore(self.max_concurrent_repairs)
                repair_tasks = [self._audit_and_heal_version(db, v, sem) for v in versions]
                await asyncio.gather(*repair_tasks, return_exceptions=True)

                return len(versions)
            finally:
                db.close()

    async def _audit_and_heal_version(self, db: Session, version: FileVersion, semaphore: asyncio.Semaphore):
        """Audits all cluster nodes for a specific file version and repairs deficient replicas."""
        async with semaphore:
            expected_sha256 = version.sha256_hash
            object_key = version.object_key
            
            node_payloads: Dict[str, Optional[bytes]] = {}
            deficient_nodes: List[str] = []

            # 1. Audit local node storage
            local_node_id = self.node.node_id if self.node else settings.NODE_ID
            try:
                local_bytes = await asyncio.to_thread(storage_service.download_object, object_key)
                if compute_sha256(local_bytes) == expected_sha256:
                    node_payloads[local_node_id] = local_bytes
                else:
                    logger.warning(f"SHA-256 mismatch for object {object_key} on local node [{local_node_id}]")
                    deficient_nodes.append(local_node_id)
            except Exception as e:
                logger.warning(f"Object {object_key} missing on local node [{local_node_id}]: {e}")
                deficient_nodes.append(local_node_id)

            # 2. Audit peer node storage
            peer_endpoints = replication_service.peer_endpoints
            for peer_id, endpoint in peer_endpoints.items():
                def _fetch_peer(p_id=peer_id, ep=endpoint):
                    if storage_service.use_local_fallback:
                        peer_path = os.path.join(os.getcwd(), "data", "storage", p_id, object_key.replace("/", "_"))
                        if os.path.exists(peer_path):
                            with open(peer_path, "rb") as f:
                                return f.read()
                        return None
                    else:
                        from minio import Minio
                        peer_client = Minio(
                            endpoint=ep,
                            access_key=settings.MINIO_ACCESS_KEY,
                            secret_key=settings.MINIO_SECRET_KEY,
                            secure=settings.MINIO_SECURE,
                        )
                        bucket_name = f"{p_id}-bucket"
                        resp = peer_client.get_object(bucket_name, object_key)
                        try:
                            return resp.read()
                        finally:
                            resp.close()
                            resp.release_conn()

                try:
                    peer_bytes = await asyncio.to_thread(_fetch_peer)
                    if peer_bytes and compute_sha256(peer_bytes) == expected_sha256:
                        node_payloads[peer_id] = peer_bytes
                    else:
                        deficient_nodes.append(peer_id)
                except Exception as peer_err:
                    logger.debug(f"Object {object_key} missing or error on peer [{peer_id}]: {peer_err}")
                    deficient_nodes.append(peer_id)

            if not deficient_nodes:
                # All replicas intact and valid!
                return

            logger.info(f"Object {object_key} deficient on nodes: {deficient_nodes}. Searching for healthy repair source...")

            # 3. Find an independently verified healthy repair source
            healthy_payload: Optional[bytes] = None
            healthy_source_id: Optional[str] = None

            for source_id, candidate_bytes in node_payloads.items():
                if candidate_bytes and compute_sha256(candidate_bytes) == expected_sha256:
                    healthy_payload = candidate_bytes
                    healthy_source_id = source_id
                    break

            if not healthy_payload:
                logger.error(f"CRITICAL: No healthy repair source found with valid SHA-256 for object {object_key}!")
                return

            # 4. Repair each deficient node
            for target_node_id in deficient_nodes:
                try:
                    if target_node_id == local_node_id:
                        storage_service.upload_object(object_key, healthy_payload)
                        repaired_bytes = storage_service.download_object(object_key)
                    else:
                        default_docker_fallback = f"minio-{target_node_id.split('-')[-1]}:9000" if target_node_id.startswith("node-") else "minio-1:9000"
                        peer_endpoint = peer_endpoints.get(target_node_id, default_docker_fallback)
                        await replication_service.replicate_object_to_peer(
                            peer_id=target_node_id,
                            peer_endpoint=peer_endpoint,
                            object_key=object_key,
                            data=healthy_payload
                        )
                        # Read back repaired bytes to verify post-repair hash
                        if storage_service.use_local_fallback:
                            repaired_path = os.path.join(os.getcwd(), "data", "storage", target_node_id, object_key.replace("/", "_"))
                            with open(repaired_path, "rb") as f:
                                repaired_bytes = f.read()
                        else:
                            repaired_bytes = healthy_payload

                    # Post-repair SHA-256 verification
                    if compute_sha256(repaired_bytes) == expected_sha256:
                        logger.info(f"REPLICA_HEALED: Object {object_key} successfully repaired on node [{target_node_id}] from healthy source [{healthy_source_id}]")
                        try:
                            log_audit_event(
                                db=db,
                                event_type="REPLICA_HEALED",
                                user_id=None,
                                resource_id=version.file_id,
                                details={
                                    "object_key": object_key,
                                    "target_node": target_node_id,
                                    "source_node": healthy_source_id,
                                    "sha256": expected_sha256
                                }
                            )
                        except Exception as audit_err:
                            try:
                                db.rollback()
                            except Exception:
                                pass
                            logger.warning(
                                f"Failed to persist REPLICA_HEALED audit log for object {object_key} on node [{target_node_id}]: {audit_err}"
                            )
                    else:
                        logger.error(f"Post-repair SHA-256 check failed for object {object_key} on node [{target_node_id}]")

                except Exception as repair_err:
                    logger.error(f"Failed to repair object {object_key} on node [{target_node_id}]: {repair_err}")

    async def process_pending_cleanups(self) -> int:
        """
        Durable Object Cleanup Processor:
        Queries outstanding 'PENDING' cleanups from PostgreSQL,
        deletes the object from local storage and all peer MinIO instances,
        and marks them 'COMPLETED' upon full cleanup.
        """
        db = SessionLocal()
        cleaned_count = 0
        try:
            pending = db.query(PendingObjectCleanup).filter(
                PendingObjectCleanup.status.in_(["PENDING", "FAILED"])
            ).limit(self.batch_size).all()

            for item in pending:
                obj_key = item.object_key
                item.retry_count += 1
                try:
                    # 1. Delete from local storage
                    local_ok = await asyncio.to_thread(storage_service.delete_object, obj_key)

                    # 2. Delete from all peer MinIO instances
                    peer_results = await replication_service.delete_object_from_all_peers(obj_key)
                    all_peers_ok = all(peer_results.values()) if peer_results else True

                    if local_ok and all_peers_ok:
                        item.status = "COMPLETED"
                        item.last_error = None
                        cleaned_count += 1
                        logger.info(f"Durable object cleanup completed for {obj_key}")
                    else:
                        item.status = "FAILED" if item.retry_count >= 5 else "PENDING"
                        failed_nodes = [p for p, ok in peer_results.items() if not ok]
                        item.last_error = f"Failed to delete on nodes: {failed_nodes}"
                        logger.warning(f"Durable object cleanup partial/failed for {obj_key}: {item.last_error}")
                except Exception as err:
                    item.status = "FAILED" if item.retry_count >= 5 else "PENDING"
                    item.last_error = str(err)
                    logger.warning(f"Error during durable cleanup of {obj_key}: {err}")

            db.commit()
            return cleaned_count
        except Exception as e:
            try:
                db.rollback()
            except Exception:
                pass
            logger.error(f"Error processing pending object cleanups: {e}")
            return 0
        finally:
            db.close()


replica_auditor = ReplicaAuditor()

