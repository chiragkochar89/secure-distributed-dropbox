import os
import shutil
import uuid
import pytest
import asyncio
from unittest.mock import patch, AsyncMock

from app.config import settings
from app.db.session import SessionLocal, init_db
from app.db.models.file import FileMetadata, FileVersion, PendingObjectCleanup
from app.core.crypto import compute_sha256
from app.storage.minio_client import storage_service
from app.storage.replication_service import ObjectReplicationService, replication_service
from app.services.auditor import ReplicaAuditor
from app.raft.node import RaftNode, NodeRole


@pytest.fixture(autouse=True)
def clean_test_storage():
    init_db()
    storage_dir = os.path.join(os.getcwd(), "data", "storage")
    if os.path.exists(storage_dir):
        shutil.rmtree(storage_dir, ignore_errors=True)
    os.makedirs(storage_dir, exist_ok=True)
    orig_fallback = storage_service.use_local_fallback
    storage_service.use_local_fallback = True

    yield

    storage_service.use_local_fallback = orig_fallback
    if os.path.exists(storage_dir):
        shutil.rmtree(storage_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_multi_replica_sha256_byte_parity():
    """Phase 5 Test 1: Upload object, replicate across all 3 replicas, verify byte parity and SHA-256."""
    repl_service = ObjectReplicationService()
    settings.MINIO_PEER_ENDPOINTS = "node-2=localhost:9002,node-3=localhost:9003"

    file_id = f"f-parity-{uuid.uuid4().hex[:8]}"
    object_key = f"users/u1/{file_id}/v1"
    content = b"Distributed Raft Dropbox Deterministic Payload Verification Content 2026!"
    expected_sha256 = compute_sha256(content)

    # 1. Primary write
    storage_service.upload_object(object_key, content)
    primary_bytes = storage_service.download_object(object_key)
    assert primary_bytes == content
    assert compute_sha256(primary_bytes) == expected_sha256

    # 2. Replicate to peer stores
    results = await repl_service.replicate_object_to_all_peers(object_key, content)
    assert results.get("node-2") is True
    assert results.get("node-3") is True

    # 3. Verify byte-for-byte exactness across all 3 node stores
    node2_path = os.path.join(os.getcwd(), "data", "storage", "node-2", object_key.replace("/", "_"))
    node3_path = os.path.join(os.getcwd(), "data", "storage", "node-3", object_key.replace("/", "_"))

    with open(node2_path, "rb") as f:
        node2_bytes = f.read()
    with open(node3_path, "rb") as f:
        node3_bytes = f.read()

    assert node2_bytes == content
    assert node3_bytes == content
    assert compute_sha256(node2_bytes) == expected_sha256
    assert compute_sha256(node3_bytes) == expected_sha256


@pytest.mark.asyncio
async def test_replica_corruption_and_self_healing():
    """Phase 5 Test 2: Induce corruption on Node 2 and deletion on Node 3; verify auditor repairs both."""
    repl_service = ObjectReplicationService()
    settings.MINIO_PEER_ENDPOINTS = "node-2=localhost:9002,node-3=localhost:9003"

    file_id = f"f-heal-{uuid.uuid4().hex[:8]}"
    object_key = f"users/u1/{file_id}/v1"
    authoritative_content = b"CRITICAL_AUTHORITATIVE_DATA_BLOCK_RAFT_CLUSTER"
    expected_hash = compute_sha256(authoritative_content)

    db = SessionLocal()
    meta = FileMetadata(
        id=file_id,
        owner_id="u1",
        original_filename="heal_test.bin",
        sanitized_name="heal_test.bin",
        content_type="application/octet-stream",
        size_bytes=len(authoritative_content),
        current_sha256=expected_hash,
        current_version=1,
        is_deleted=False
    )
    ver = FileVersion(
        id=str(uuid.uuid4()),
        file_id=file_id,
        version_number=1,
        object_key=object_key,
        size_bytes=len(authoritative_content),
        sha256_hash=expected_hash
    )
    db.add(meta)
    db.add(ver)
    db.commit()
    db.close()

    # Step 1: Upload valid data to Node 1 (primary) and replicate initially
    storage_service.upload_object(object_key, authoritative_content)
    await repl_service.replicate_object_to_all_peers(object_key, authoritative_content)

    node2_path = os.path.join(os.getcwd(), "data", "storage", "node-2", object_key.replace("/", "_"))
    node3_path = os.path.join(os.getcwd(), "data", "storage", "node-3", object_key.replace("/", "_"))

    # Step 2: INDUCE CORRUPTION:
    # Corrupt node-2
    with open(node2_path, "wb") as f:
        f.write(b"TAMPERED_MALICIOUS_BITROT_PAYLOAD")
    # Delete node-3 entirely
    if os.path.exists(node3_path):
        os.remove(node3_path)

    assert os.path.exists(node3_path) is False
    with open(node2_path, "rb") as f:
        assert compute_sha256(f.read()) != expected_hash

    # Step 3: Run auditor repair
    node = RaftNode(node_id="node-1")
    node.role = NodeRole.LEADER
    auditor = ReplicaAuditor(node=node)

    # Auditing repairs missing / corrupt replicas from the authoritative primary
    audited = await auditor.run_audit_cycle()
    assert audited >= 1

    # Step 4: Verify healing results
    assert os.path.exists(node3_path) is True
    with open(node2_path, "rb") as f:
        assert f.read() == authoritative_content
    with open(node3_path, "rb") as f:
        assert f.read() == authoritative_content
    assert compute_sha256(open(node2_path, "rb").read()) == expected_hash
    assert compute_sha256(open(node3_path, "rb").read()) == expected_hash


@pytest.mark.asyncio
async def test_soft_delete_and_durable_cleanup_drain():
    """Phase 5 Test 3: Soft-delete metadata creates durable cleanup, auditor deletes objects across all replicas."""
    settings.MINIO_PEER_ENDPOINTS = "node-2=localhost:9002,node-3=localhost:9003"
    repl_service = ObjectReplicationService()

    file_id = f"f-del-{uuid.uuid4().hex[:8]}"
    object_key = f"users/u1/{file_id}/v1"
    content = b"Data to be securely deleted"

    # Upload to primary and peers
    storage_service.upload_object(object_key, content)
    await repl_service.replicate_object_to_all_peers(object_key, content)

    node1_path = os.path.join(storage_service.local_storage_dir, object_key.replace("/", "_"))
    node2_path = os.path.join(os.getcwd(), "data", "storage", "node-2", object_key.replace("/", "_"))
    node3_path = os.path.join(os.getcwd(), "data", "storage", "node-3", object_key.replace("/", "_"))
    assert os.path.exists(node1_path) is True
    assert os.path.exists(node2_path) is True
    assert os.path.exists(node3_path) is True

    # Commit soft-delete and pending object cleanup in PostgreSQL
    db = SessionLocal()
    meta = FileMetadata(
        id=file_id,
        owner_id="u1",
        original_filename="delete_me.txt",
        sanitized_name="delete_me.txt",
        content_type="text/plain",
        size_bytes=len(content),
        current_sha256=compute_sha256(content),
        current_version=1,
        is_deleted=True  # Soft deleted
    )
    cleanup = PendingObjectCleanup(
        file_id=file_id,
        object_key=object_key,
        status="PENDING",
        retry_count=0
    )
    db.add(meta)
    db.add(cleanup)
    db.commit()
    cleanup_id = cleanup.id
    db.close()

    # Run auditor cleanup cycle
    node = RaftNode(node_id="node-1")
    node.role = NodeRole.LEADER
    auditor = ReplicaAuditor(node=node)

    await auditor.process_pending_cleanups()

    # Verify object deleted from primary and all peer buckets
    assert os.path.exists(node1_path) is False
    assert os.path.exists(node2_path) is False
    assert os.path.exists(node3_path) is False

    # Verify cleanup record marked COMPLETED
    db = SessionLocal()
    c = db.query(PendingObjectCleanup).filter_by(id=cleanup_id).first()
    assert c.status == "COMPLETED"
    db.close()
