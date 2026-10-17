import os
import shutil
import pytest
import asyncio
from unittest.mock import AsyncMock, patch

from app.config import settings
from app.db.session import SessionLocal
from app.db.models.file import FileMetadata, FileVersion, AppliedCommand
from app.db.models.user import User
from app.services.commands import FileOperationCommand, FileOperationType
from app.storage.minio_client import StorageService, storage_service
from app.storage.replication_service import ObjectReplicationService, replication_service
from app.raft.node import RaftNode, NodeRole
from app.raft.state import PersistentRaftState, VolatileRaftState
from app.raft.state_machine import StateMachineExecutor


@pytest.fixture(autouse=True)
def clean_storage_env():
    """Clean up data/storage test directories before and after each test."""
    orig_fallback = storage_service.use_local_fallback
    storage_service.use_local_fallback = True
    storage_dir = os.path.join(os.getcwd(), "data", "storage")
    if os.path.exists(storage_dir):
        shutil.rmtree(storage_dir, ignore_errors=True)
    os.makedirs(storage_dir, exist_ok=True)

    yield

    storage_service.use_local_fallback = orig_fallback
    if os.path.exists(storage_dir):
        shutil.rmtree(storage_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_object_payload_peer_replication():
    """Test 1: Upload object to leader and verify replication to peer MinIO/storage stores."""
    repl_service = ObjectReplicationService()
    
    # Configure peer endpoints
    settings.MINIO_PEER_ENDPOINTS = "node-2=localhost:9002,node-3=localhost:9003"
    
    object_key = "users/test-user-1/file-100/v1"
    data = b"Hello, Secure Raft Dropbox Peer Replication!"
    
    # Replicate payload to peers
    results = await repl_service.replicate_object_to_all_peers(object_key, data)
    
    assert "node-2" in results
    assert "node-3" in results
    assert results["node-2"] is True
    assert results["node-3"] is True

    # Verify peer storage files exist in fallback storage directories
    node2_path = os.path.join(os.getcwd(), "data", "storage", "node-2", object_key.replace("/", "_"))
    node3_path = os.path.join(os.getcwd(), "data", "storage", "node-3", object_key.replace("/", "_"))
    
    assert os.path.exists(node2_path)
    assert os.path.exists(node3_path)
    
    with open(node2_path, "rb") as f:
        assert f.read() == data
    with open(node3_path, "rb") as f:
        assert f.read() == data


@pytest.mark.asyncio
async def test_replication_after_commit_only():
    """Test 2: Verify object replication triggers ONLY after Raft log reaches commitIndex."""
    node = RaftNode(node_id="node-1")
    node.persistent_state.current_term = 1
    node.role = NodeRole.LEADER
    executor = StateMachineExecutor()
    
    object_key = "users/test-user-2/file-200/v1"
    payload_data = b"Uncommitted vs Committed Payload Test"
    
    # Store payload locally on leader
    storage_service.upload_object(object_key, payload_data)
    
    # Append UPLOAD command to leader log (Index 1)
    cmd = FileOperationCommand(
        operation=FileOperationType.UPLOAD,
        file_id="file-200",
        owner_id="user-2",
        filename="test.txt",
        sha256="dummyhash200",
        object_key=object_key,
        size_bytes=len(payload_data),
        version=1
    )
    await node.append_log_entry(cmd)

    # UNCOMMITTED STATE: commit_index remains 0, last_applied remains 0
    assert node.volatile_state.commit_index == 0
    applied = await executor.apply_committed_entries(node)
    assert applied == 0  # No entries applied
    
    # Verify peer stores do NOT have object yet
    node2_path = os.path.join(os.getcwd(), "data", "storage", "node-2", object_key.replace("/", "_"))
    assert not os.path.exists(node2_path)

    # COMMITTED STATE: Advance commit_index to 1
    node.volatile_state.commit_index = 1
    
    with patch.object(replication_service, "replicate_object_to_all_peers", wraps=replication_service.replicate_object_to_all_peers) as mock_repl:
        applied = await executor.apply_committed_entries(node)
        assert applied == 1
        assert node.volatile_state.last_applied == 1
        
        # Allow background task to complete
        await asyncio.sleep(0.1)
        mock_repl.assert_called_once_with(object_key, payload_data, "application/octet-stream")


@pytest.mark.asyncio
async def test_replication_version_isolation():
    """Test 3: Verify uploading v1 and v2 creates distinct immutable keys without overwriting v1."""
    repl_service = ObjectReplicationService()
    settings.MINIO_PEER_ENDPOINTS = "node-2=localhost:9002"
    
    v1_key = "users/user-3/file-300/v1"
    v1_data = b"Version 1 Payload Content"
    
    v2_key = "users/user-3/file-300/v2"
    v2_data = b"Version 2 Updated Payload Content"

    # Replicate v1
    res_v1 = await repl_service.replicate_object_to_all_peers(v1_key, v1_data)
    assert res_v1["node-2"] is True

    # Replicate v2
    res_v2 = await repl_service.replicate_object_to_all_peers(v2_key, v2_data)
    assert res_v2["node-2"] is True

    # Verify both versions exist independently in peer storage
    v1_path = os.path.join(os.getcwd(), "data", "storage", "node-2", v1_key.replace("/", "_"))
    v2_path = os.path.join(os.getcwd(), "data", "storage", "node-2", v2_key.replace("/", "_"))

    assert os.path.exists(v1_path)
    assert os.path.exists(v2_path)

    with open(v1_path, "rb") as f:
        assert f.read() == v1_data
    with open(v2_path, "rb") as f:
        assert f.read() == v2_data


@pytest.mark.asyncio
async def test_replication_retry_after_peer_recovery():
    """Test 4: Verify retry backoff logic handles transient peer failures."""
    from unittest.mock import MagicMock
    repl_service = ObjectReplicationService()
    
    object_key = "users/user-4/file-400/v1"
    data = b"Retry Mechanism Test Bytes"

    attempt_counter = 0

    def flaky_replicate(*args, **kwargs):
        nonlocal attempt_counter
        attempt_counter += 1
        if attempt_counter < 3:
            raise ConnectionError("Simulated peer network timeout")
        return MagicMock()

    # Patch peer call to simulate failure on attempts 1 and 2, success on 3
    with patch.object(repl_service.local_storage, "use_local_fallback", False):
        with patch("app.storage.replication_service.Minio") as mock_minio_cls:
            mock_client = MagicMock()
            mock_client.bucket_exists.return_value = True
            mock_client.put_object.side_effect = flaky_replicate
            mock_minio_cls.return_value = mock_client

            success = await repl_service.replicate_object_to_peer(
                peer_id="node-2",
                peer_endpoint="localhost:9002",
                object_key=object_key,
                data=data,
                max_retries=3
            )

            assert success is True
            assert attempt_counter == 3
