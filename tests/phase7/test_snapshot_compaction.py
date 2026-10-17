import os
import shutil
import uuid
import pytest
import asyncio
from unittest.mock import patch

from app.db.session import SessionLocal, init_db
from app.db.models.file import FileMetadata, FileVersion, AppliedCommand
from app.services.commands import FileOperationCommand, FileOperationType
from app.raft.node import RaftNode, NodeRole
from app.raft.state import RaftLogEntry
from app.raft.state_machine import state_machine_executor
from app.raft.snapshot import RaftSnapshot, SnapshotManager, dump_postgres_state_machine, restore_postgres_state_machine
from app.core.crypto import compute_sha256


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

    yield

    for d in [storage_dir, raft_dir]:
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)


@pytest.mark.asyncio
async def test_snapshot_threshold_trigger():
    """Test 1: Verify last_applied - last_included_index >= threshold triggers snapshot creation."""
    node = RaftNode(node_id="node-1")
    node.persistent_state.current_term = 1
    node.role = NodeRole.LEADER

    # Append 10 UPLOAD commands to log
    for i in range(1, 11):
        file_id = f"file-snap-{i}"
        cmd = FileOperationCommand(
            operation=FileOperationType.UPLOAD,
            file_id=file_id,
            owner_id="user-snap",
            filename=f"file_{i}.txt",
            sha256=f"hash_{i}",
            object_key=f"users/user-snap/{file_id}/v1",
            size_bytes=100,
            version=1
        )
        await node.append_log_entry(cmd)

    # Commit all 10 entries and apply to state machine (automatically triggers snapshot at threshold 10)
    node.volatile_state.commit_index = 10
    applied = await state_machine_executor.apply_committed_entries(node)
    assert applied == 10
    assert node.volatile_state.last_applied == 10
    assert node.last_included_index == 10
    assert len(node.persistent_state.log) == 0  # Compacted all entries 1..10


@pytest.mark.asyncio
async def test_snapshot_checkpoint_barrier_consistency():
    """Test 2: Verify Checkpoint Barrier guarantees snapshot represents EXACT state at index N."""
    node = RaftNode(node_id="node-1")
    node.persistent_state.current_term = 1
    node.role = NodeRole.LEADER

    for i in range(1, 6):
        cmd = FileOperationCommand(
            operation=FileOperationType.UPLOAD,
            file_id=f"file-cb-{i}",
            owner_id="user-cb",
            filename=f"cb_{i}.txt",
            sha256=f"hash_cb_{i}",
            object_key=f"u/f_{i}/v1",
            version=1
        )
        await node.append_log_entry(cmd)

    node.volatile_state.commit_index = 5
    await state_machine_executor.apply_committed_entries(node)
    assert node.volatile_state.last_applied == 5

    # Verify barrier ensures dump represents exactly 5 applied commands
    snapshot = await state_machine_executor.maybe_create_snapshot(node, snapshot_threshold=5)
    assert snapshot is not None
    assert snapshot.last_included_index == 5

    applied_cmds = snapshot.metadata_state.get("applied_commands", [])
    assert len(applied_cmds) == 5
    indexes = [ac["raft_index"] for ac in applied_cmds]
    assert indexes == [1, 2, 3, 4, 5]


@pytest.mark.asyncio
async def test_snapshot_non_blocking_raft_lock():
    """Test 3: Verify RaftNode lock is NOT held during PostgreSQL snapshot dump and disk I/O."""
    node = RaftNode(node_id="node-1")
    node.persistent_state.current_term = 1

    for i in range(1, 6):
        cmd = FileOperationCommand(
            operation=FileOperationType.UPLOAD,
            file_id=f"file-nb-{i}",
            owner_id="user-nb",
            filename=f"nb_{i}.txt",
            sha256=f"hash_nb_{i}",
            object_key=f"u/f_{i}/v1",
            version=1
        )
        await node.append_log_entry(cmd)

    node.volatile_state.commit_index = 5
    await state_machine_executor.apply_committed_entries(node)

    # During dump, verify node.lock is NOT locked
    lock_was_unlocked = False
    with patch("app.raft.snapshot.dump_postgres_state_machine", wraps=dump_postgres_state_machine) as mock_dump:
        def check_lock_state():
            nonlocal lock_was_unlocked
            lock_was_unlocked = not node.lock.locked()
            return dump_postgres_state_machine()

        mock_dump.side_effect = check_lock_state
        snapshot = await state_machine_executor.maybe_create_snapshot(node, snapshot_threshold=5)
        assert snapshot is not None
        assert lock_was_unlocked is True


@pytest.mark.asyncio
async def test_snapshot_preserves_suffix():
    """Test 4: Verify log compaction truncates entries <= last_included_index while preserving entries > last_included_index."""
    node = RaftNode(node_id="node-1")
    node.persistent_state.current_term = 1

    # Append 12 entries
    for i in range(1, 13):
        cmd = FileOperationCommand(
            operation=FileOperationType.UPLOAD,
            file_id=f"file-suf-{i}",
            owner_id="user-suf",
            filename=f"suf_{i}.txt",
            sha256=f"hash_suf_{i}",
            object_key=f"u/f_{i}/v1",
            version=1
        )
        await node.append_log_entry(cmd)

    # Apply up to index 10 (automatically triggers snapshot creation at index 10)
    node.volatile_state.commit_index = 10
    await state_machine_executor.apply_committed_entries(node)
    assert node.volatile_state.last_applied == 10
    assert node.last_included_index == 10

    # Log should preserve suffix entries 11 and 12
    remaining_entries = node.persistent_state.log
    assert len(remaining_entries) == 2
    assert remaining_entries[0].index == 11
    assert remaining_entries[1].index == 12

    # Abstraction checks
    assert node.get_log_entry(10) is None  # Compacted
    assert node.get_log_entry(11) is not None  # Preserved
    assert node.get_log_entry(11).command.file_id == "file-suf-11"
    assert node.get_log_entry(12).command.file_id == "file-suf-12"
    assert node.last_log_index == 12


@pytest.mark.asyncio
async def test_snapshot_checksum_validation():
    """Test 5: Verify snapshot loading fails if state_machine_checksum does not match payload content."""
    state_data = dump_postgres_state_machine()
    canonical_bytes = json_dumps_utf8(state_data)
    valid_checksum = compute_sha256(canonical_bytes)

    snapshot = RaftSnapshot(
        last_included_index=5,
        last_included_term=1,
        state_machine_checksum=valid_checksum,
        metadata_state=state_data
    )

    # Valid deserialization succeeds
    valid_json = snapshot.to_json()
    loaded_snap = RaftSnapshot.from_json(valid_json)
    assert loaded_snap.last_included_index == 5

    # Corrupt checksum in JSON
    import json
    snap_dict = json.loads(valid_json)
    snap_dict["state_machine_checksum"] = "bad_corrupted_checksum_hash"
    corrupted_json = json.dumps(snap_dict)

    with pytest.raises(ValueError, match="Snapshot checksum mismatch"):
        RaftSnapshot.from_json(corrupted_json)


@pytest.mark.asyncio
async def test_partial_snapshot_does_not_replace_valid_snapshot():
    """Test 6: Verify crash during snapshot write leaves existing valid snapshot.json intact."""
    node_id = "node-1"
    
    # Save valid snapshot 1 at index 5
    state1 = dump_postgres_state_machine()
    checksum1 = compute_sha256(json_dumps_utf8(state1))
    snap1 = RaftSnapshot(
        last_included_index=5,
        last_included_term=1,
        state_machine_checksum=checksum1,
        metadata_state=state1
    )
    path1 = SnapshotManager.save_snapshot_atomically(node_id, snap1)
    assert os.path.exists(path1)

    # Simulate partial temporary file write (interrupted before atomic replace)
    target_dir = SnapshotManager.get_snapshot_dir(node_id)
    temp_path = os.path.join(target_dir, "snapshot.json.tmp")
    with open(temp_path, "w") as f:
        f.write("{partial incomplete json content...")

    # Load active snapshot -> must load valid snap1 (index 5)
    loaded = SnapshotManager.load_snapshot(node_id)
    assert loaded is not None
    assert loaded.last_included_index == 5
    assert loaded.state_machine_checksum == checksum1


def json_dumps_utf8(data):
    import json
    return json.dumps(data, sort_keys=True).encode("utf-8")
