import os
import shutil
import uuid
import pytest
import asyncio
from unittest.mock import patch, MagicMock

from app.config import settings
from app.db.session import SessionLocal, init_db
from app.db.models.file import FileMetadata, FileVersion
from app.core.crypto import compute_sha256
from app.storage.minio_client import storage_service
from app.storage.replication_service import replication_service
from app.services.auditor import ReplicaAuditor
from app.raft.node import RaftNode, NodeRole
from app.raft.state_machine import state_machine_executor
from app.services.commands import FileOperationCommand, FileOperationType


@pytest.fixture(autouse=True)
def clean_env():
    """Ensure clean database and storage environment before each test."""
    init_db()
    storage_dir = os.path.join(os.getcwd(), "data", "storage")
    raft_dir = os.path.join(os.getcwd(), "data", "raft")
    for d in [storage_dir, raft_dir]:
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d, exist_ok=True)
    orig_fallback = storage_service.use_local_fallback
    storage_service.use_local_fallback = True

    yield

    storage_service.use_local_fallback = orig_fallback
    for d in [storage_dir, raft_dir]:
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)


@pytest.mark.asyncio
async def test_configurable_snapshot_threshold():
    """Verify SNAPSHOT_THRESHOLD configuration loads as 10 and controls automatic snapshot triggering."""
    assert settings.SNAPSHOT_THRESHOLD == 10

    # Test custom setting value (e.g., 3)
    orig_threshold = settings.SNAPSHOT_THRESHOLD
    try:
        settings.SNAPSHOT_THRESHOLD = 3

        node = RaftNode(node_id="node-p2-snap")
        node.persistent_state.current_term = 1
        node.role = NodeRole.LEADER

        # Append 3 entries to trigger threshold of 3
        for i in range(1, 4):
            cmd = FileOperationCommand(
                operation=FileOperationType.UPLOAD,
                file_id=f"p2-file-{i}",
                owner_id="p2-owner",
                filename=f"p2_file_{i}.txt",
                sha256=f"hash_{i}",
                object_key=f"users/p2-owner/p2-file-{i}/v1",
                size_bytes=50,
                version=1
            )
            await node.append_log_entry(cmd)

        node.volatile_state.commit_index = 3
        applied = await state_machine_executor.apply_committed_entries(node)
        assert applied == 3
        assert node.last_included_index == 3
    finally:
        settings.SNAPSHOT_THRESHOLD = orig_threshold


@pytest.mark.asyncio
async def test_replica_auditor_docker_hostname_fallback():
    """Verify ReplicaAuditor fallback endpoint uses Docker service hostnames (minio-X:9000)."""
    node = RaftNode(node_id="node-1")
    node.role = NodeRole.LEADER
    auditor = ReplicaAuditor(node=node)

    captured_endpoints = {}
    orig_replicate = replication_service.replicate_object_to_peer

    async def tracking_replicate(peer_id, peer_endpoint, object_key, data, **kwargs):
        captured_endpoints[peer_id] = peer_endpoint
        return await orig_replicate(peer_id, peer_endpoint, object_key, data, **kwargs)

    with patch.object(settings, "MINIO_PEER_ENDPOINTS", "node-3=minio-3:9000"):
        with patch.object(replication_service, "replicate_object_to_peer", side_effect=tracking_replicate):
            db = SessionLocal()
            file_id = str(uuid.uuid4())
            object_key = f"users/u-p2/{file_id}/v1"
            valid_bytes = b"Valid Payload Content for P2 Test"
            valid_hash = compute_sha256(valid_bytes)

            file_meta = FileMetadata(
                id=file_id,
                owner_id="u-p2",
                original_filename="p2.txt",
                sanitized_name="p2.txt",
                content_type="text/plain",
                size_bytes=len(valid_bytes),
                current_sha256=valid_hash,
                current_version=1,
                is_deleted=False
            )
            file_ver = FileVersion(
                file_id=file_id,
                version_number=1,
                object_key=object_key,
                size_bytes=len(valid_bytes),
                sha256_hash=valid_hash
            )
            db.add(file_meta)
            db.add(file_ver)
            db.commit()

            # Upload to local node storage
            storage_service.upload_object(object_key, valid_bytes)

            sem = asyncio.Semaphore(1)
            await auditor._audit_and_heal_version(db, file_ver, sem)

            db.close()

    assert "node-3" in captured_endpoints
    assert captured_endpoints["node-3"] == "minio-3:9000"


@pytest.mark.asyncio
async def test_auditor_database_rollback_on_audit_log_failure():
    """Verify DB audit persistence failure triggers db.rollback() without failing object repair."""
    node = RaftNode(node_id="node-1")
    node.role = NodeRole.LEADER
    auditor = ReplicaAuditor(node=node)

    db = SessionLocal()
    file_id = str(uuid.uuid4())
    object_key = f"users/u-p2-rb/{file_id}/v1"
    valid_bytes = b"Payload for Rollback Test"
    valid_hash = compute_sha256(valid_bytes)

    file_meta = FileMetadata(
        id=file_id,
        owner_id="u-p2-rb",
        original_filename="rb.txt",
        sanitized_name="rb.txt",
        content_type="text/plain",
        size_bytes=len(valid_bytes),
        current_sha256=valid_hash,
        current_version=1,
        is_deleted=False
    )
    file_ver = FileVersion(
        file_id=file_id,
        version_number=1,
        object_key=object_key,
        size_bytes=len(valid_bytes),
        sha256_hash=valid_hash
    )
    db.add(file_meta)
    db.add(file_ver)
    db.commit()

    storage_service.upload_object(object_key, valid_bytes)

    def failing_log_audit(*args, **kwargs):
        raise RuntimeError("Database connection dropped during audit logging")

    rollback_called = False
    orig_rollback = db.rollback

    def tracking_rollback():
        nonlocal rollback_called
        rollback_called = True
        orig_rollback()

    db.rollback = tracking_rollback

    sem = asyncio.Semaphore(1)
    with patch("app.services.auditor.log_audit_event", side_effect=failing_log_audit):
        await auditor._audit_and_heal_version(db, file_ver, sem)

    db.close()

    assert rollback_called is True
