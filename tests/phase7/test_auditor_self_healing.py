import os
import shutil
import uuid
import pytest
import asyncio
from unittest.mock import patch

from app.config import settings
from app.db.session import SessionLocal, init_db
from app.db.models.file import FileMetadata, FileVersion
from app.db.models.audit import AuditLog
from app.core.crypto import compute_sha256
from app.storage.minio_client import storage_service
from app.storage.replication_service import replication_service
from app.services.auditor import ReplicaAuditor, replica_auditor
from app.raft.node import RaftNode, NodeRole
from app.raft.engine import RaftElectionEngine


@pytest.fixture(autouse=True)
def clean_env():
    """Ensure clean database and storage environment before each test."""
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
async def test_replica_corruption_detection():
    """Test 1: Detect object payload corruption when storage bytes hash differs from PostgreSQL metadata."""
    db = SessionLocal()
    file_id = str(uuid.uuid4())
    object_key = f"users/u-audit-1/{file_id}/v1"
    valid_data = b"Authoritative Committed Payload Content"
    corrupted_data = b"Corrupted Modified Payload Content"
    expected_hash = compute_sha256(valid_data)

    # Insert committed file metadata and version into PostgreSQL
    file_meta = FileMetadata(
        id=file_id,
        owner_id="u-audit-1",
        original_filename="doc.txt",
        sanitized_name="doc.txt",
        content_type="text/plain",
        size_bytes=len(valid_data),
        current_sha256=expected_hash,
        current_version=1,
        is_deleted=False
    )
    file_ver = FileVersion(
        id=str(uuid.uuid4()),
        file_id=file_id,
        version_number=1,
        object_key=object_key,
        size_bytes=len(valid_data),
        sha256_hash=expected_hash
    )
    db.add(file_meta)
    db.add(file_ver)
    db.commit()
    db.close()

    # Store CORRUPTED payload locally
    storage_service.upload_object(object_key, corrupted_data)
    assert compute_sha256(storage_service.download_object(object_key)) != expected_hash

    # Run audit cycle
    node = RaftNode(node_id="node-1")
    node.role = NodeRole.LEADER
    auditor = ReplicaAuditor(node=node, audit_interval_seconds=60.0)
    
    with patch("app.services.auditor.logger.warning") as mock_warn:
        audited_count = await auditor.run_audit_cycle()
        assert audited_count >= 1
        # Check that corruption warning was logged
        warning_logged = any("SHA-256 mismatch" in call.args[0] for call in mock_warn.call_args_list)
        assert warning_logged is True


@pytest.mark.asyncio
async def test_auditor_stops_on_leadership_loss():
    """Test 2: Auditor loop stops immediately when leader node steps down to follower."""
    node = RaftNode(node_id="node-1")
    node.role = NodeRole.LEADER
    
    engine = RaftElectionEngine(node=node)
    await engine.start_heartbeat_loop()
    assert replica_auditor.is_running is True

    # Leadership lost -> step down
    await engine.stop_heartbeat_loop()
    assert replica_auditor.is_running is False


@pytest.mark.asyncio
async def test_new_leader_starts_auditor():
    """Test 3: Auditor starts automatically when node is promoted to leader."""
    node = RaftNode(node_id="node-1")
    node.role = NodeRole.LEADER
    
    engine = RaftElectionEngine(node=node)
    assert replica_auditor.is_running is False

    await engine.start_heartbeat_loop()
    assert replica_auditor.is_running is True

    await engine.stop_heartbeat_loop()
    assert replica_auditor.is_running is False


@pytest.mark.asyncio
async def test_corrupted_source_not_used_for_repair():
    """Test 4: Auditor verifies candidate source hash and rejects corrupted repair sources."""
    db = SessionLocal()
    file_id = str(uuid.uuid4())
    object_key = f"users/u-audit-4/{file_id}/v1"
    valid_data = b"Valid Payload Bytes"
    corrupted_data = b"Corrupted Source Bytes"
    expected_hash = compute_sha256(valid_data)

    # Insert database records
    file_meta = FileMetadata(
        id=file_id,
        owner_id="u-audit-4",
        original_filename="file4.txt",
        sanitized_name="file4.txt",
        content_type="text/plain",
        size_bytes=len(valid_data),
        current_sha256=expected_hash,
        current_version=1,
        is_deleted=False
    )
    file_ver = FileVersion(
        id=str(uuid.uuid4()),
        file_id=file_id,
        version_number=1,
        object_key=object_key,
        size_bytes=len(valid_data),
        sha256_hash=expected_hash
    )
    db.add(file_meta)
    db.add(file_ver)
    db.commit()
    db.close()

    # Node-1 (leader) has CORRUPTED payload
    storage_service.upload_object(object_key, corrupted_data)
    
    # Node-2 (peer) has HEALTHY payload
    settings.MINIO_PEER_ENDPOINTS = "node-2=localhost:9002"
    node2_dir = os.path.join(os.getcwd(), "data", "storage", "node-2")
    os.makedirs(node2_dir, exist_ok=True)
    with open(os.path.join(node2_dir, object_key.replace("/", "_")), "wb") as f:
        f.write(valid_data)

    node = RaftNode(node_id="node-1")
    node.role = NodeRole.LEADER
    auditor = ReplicaAuditor(node=node, audit_interval_seconds=60.0)

    # Execute audit cycle
    await auditor.run_audit_cycle()

    # Node-1 should be repaired from Node-2 (healthy source), NOT keeping its corrupted payload!
    repaired_local = storage_service.download_object(object_key)
    assert compute_sha256(repaired_local) == expected_hash
    assert repaired_local == valid_data


@pytest.mark.asyncio
async def test_active_replica_self_healing():
    """Test 5: Deficient replica is repaired from healthy source, verified post-repair, and REPLICA_HEALED audit log recorded."""
    db = SessionLocal()
    file_id = str(uuid.uuid4())
    object_key = f"users/u-audit-5/{file_id}/v1"
    valid_data = b"Self Healing Test Payload Content"
    expected_hash = compute_sha256(valid_data)

    # Insert database records
    file_meta = FileMetadata(
        id=file_id,
        owner_id="u-audit-5",
        original_filename="file5.txt",
        sanitized_name="file5.txt",
        content_type="text/plain",
        size_bytes=len(valid_data),
        current_sha256=expected_hash,
        current_version=1,
        is_deleted=False
    )
    file_ver = FileVersion(
        id=str(uuid.uuid4()),
        file_id=file_id,
        version_number=1,
        object_key=object_key,
        size_bytes=len(valid_data),
        sha256_hash=expected_hash
    )
    db.add(file_meta)
    db.add(file_ver)
    db.commit()

    # Node-1 (leader) has HEALTHY payload
    storage_service.upload_object(object_key, valid_data)

    # Node-2 (peer) is MISSING payload
    settings.MINIO_PEER_ENDPOINTS = "node-2=localhost:9002"

    node = RaftNode(node_id="node-1")
    node.role = NodeRole.LEADER
    auditor = ReplicaAuditor(node=node, audit_interval_seconds=60.0)

    # Execute audit cycle
    await auditor.run_audit_cycle()

    # Verify Node-2 peer storage has been repaired
    node2_path = os.path.join(os.getcwd(), "data", "storage", "node-2", object_key.replace("/", "_"))
    assert os.path.exists(node2_path)
    with open(node2_path, "rb") as f:
        assert f.read() == valid_data

    # Verify REPLICA_HEALED audit log recorded in PostgreSQL
    audit_entry = (
        db.query(AuditLog)
        .filter(AuditLog.event_type == "REPLICA_HEALED", AuditLog.resource_id == file_id)
        .first()
    )
    assert audit_entry is not None
    assert audit_entry.user_id is None
    import json
    details = json.loads(audit_entry.details_json) if audit_entry.details_json else {}
    assert details.get("target_node") == "node-2"
    assert details.get("sha256") == expected_hash

    db.close()


@pytest.mark.asyncio
async def test_audit_log_persistence_failure_does_not_fail_repair():
    """Test 6: Audit log persistence failure does NOT cause a successful object repair to fail."""
    db = SessionLocal()
    file_id = str(uuid.uuid4())
    object_key = f"users/u-audit-6/{file_id}/v1"
    valid_data = b"Payload with DB Audit Error Failure Test"
    expected_hash = compute_sha256(valid_data)

    file_meta = FileMetadata(
        id=file_id,
        owner_id="u-audit-6",
        original_filename="file6.txt",
        sanitized_name="file6.txt",
        content_type="text/plain",
        size_bytes=len(valid_data),
        current_sha256=expected_hash,
        current_version=1,
        is_deleted=False
    )
    file_ver = FileVersion(
        id=str(uuid.uuid4()),
        file_id=file_id,
        version_number=1,
        object_key=object_key,
        size_bytes=len(valid_data),
        sha256_hash=expected_hash
    )
    db.add(file_meta)
    db.add(file_ver)
    db.commit()
    db.close()

    storage_service.upload_object(object_key, valid_data)
    settings.MINIO_PEER_ENDPOINTS = "node-2=localhost:9002"

    node = RaftNode(node_id="node-1")
    node.role = NodeRole.LEADER
    auditor = ReplicaAuditor(node=node, audit_interval_seconds=60.0)

    with patch("app.services.auditor.log_audit_event", side_effect=Exception("Database Connection Timeout")) as mock_audit_log, \
         patch("app.services.auditor.logger.warning") as mock_warn:
        
        await auditor.run_audit_cycle()
        
        # Verify log_audit_event was called
        assert mock_audit_log.called is True
        
        # Verify object repair succeeded despite audit persistence failure
        node2_path = os.path.join(os.getcwd(), "data", "storage", "node-2", object_key.replace("/", "_"))
        assert os.path.exists(node2_path)
        with open(node2_path, "rb") as f:
            assert f.read() == valid_data

        # Verify warning log specifically mentions audit log failure
        warning_logged = any("Failed to persist REPLICA_HEALED audit log" in call.args[0] for call in mock_warn.call_args_list)
        assert warning_logged is True

