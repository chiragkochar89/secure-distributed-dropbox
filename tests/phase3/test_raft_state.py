import os
import shutil
import pytest
import tempfile
from pathlib import Path

from app.raft.state import PersistentRaftState, VolatileRaftState, LeaderVolatileState, RaftLogEntry
from app.raft.persistence import RaftStorageEngine, RaftCorruptedStateError, RaftPersistenceError
from app.raft.node import RaftNode, NodeRole
from app.services.commands import FileOperationCommand, FileOperationType


@pytest.fixture
def temp_node_dir():
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_term_monotonicity(temp_node_dir):
    """Verify monotonic term update rule (5 -> 6 allowed, 6 -> 6 unchanged, 6 -> 5 ignored)."""
    node = RaftNode(node_id="test-node-1", base_dir=temp_node_dir)
    await node.initialize()

    # 1. 0 -> 5
    await node.transition_to_follower(term=5, voted_for="node-2")
    assert node.persistent_state.current_term == 5
    assert node.persistent_state.voted_for == "node-2"

    # 2. 5 -> 6 (Higher term)
    updated = await node.update_term_if_higher(6)
    assert updated is True
    assert node.persistent_state.current_term == 6
    assert node.persistent_state.voted_for is None  # Cleared on higher term!

    # 3. 6 -> 6 (Same term)
    updated_same = await node.update_term_if_higher(6)
    assert updated_same is False
    assert node.persistent_state.current_term == 6

    # 4. 6 -> 5 (Lower term rejected)
    await node.transition_to_follower(term=5, voted_for="node-3")
    assert node.persistent_state.current_term == 6  # Term remains 6!


@pytest.mark.asyncio
async def test_same_term_follower_transition_preserves_voted_for(temp_node_dir):
    """Verify that same-term follower transition preserves existing voted_for value."""
    node = RaftNode(node_id="test-node-2", base_dir=temp_node_dir)
    await node.initialize()

    # Set term=5, voted_for="node-2"
    await node.transition_to_follower(term=5, voted_for="node-2")
    assert node.persistent_state.voted_for == "node-2"

    # Transition to follower in SAME term (new_term=5) without providing voted_for arg
    await node.transition_to_follower(term=5, voted_for=None)
    assert node.persistent_state.current_term == 5
    assert node.persistent_state.voted_for == "node-2"  # Preserved!


@pytest.mark.asyncio
async def test_higher_term_transition_clears_voted_for(temp_node_dir):
    """Verify higher term transition sets current_term=6, voted_for=None, role=FOLLOWER."""
    node = RaftNode(node_id="test-node-3", base_dir=temp_node_dir)
    await node.initialize()

    await node.transition_to_follower(term=5, voted_for="node-2")
    assert node.persistent_state.voted_for == "node-2"

    # Higher term transition
    await node.transition_to_follower(term=6, voted_for=None)
    assert node.persistent_state.current_term == 6
    assert node.persistent_state.voted_for is None  # Cleared!
    assert node.role == NodeRole.FOLLOWER


@pytest.mark.asyncio
async def test_strongly_typed_raft_log_and_indexing(temp_node_dir):
    """Verify strongly typed RaftLogEntry model and 1-based log indexing convention."""
    node = RaftNode(node_id="test-node-4", base_dir=temp_node_dir)
    await node.initialize()

    # Empty log indexing convention
    assert node.persistent_state.last_log_index == 0
    assert node.persistent_state.last_log_term == 0

    cmd = FileOperationCommand(
        operation=FileOperationType.UPLOAD,
        file_id="f-100",
        owner_id="user-1",
        filename="report.pdf",
        sha256="abc123hash",
        object_key="users/user-1/f-100/v1",
        size_bytes=2048,
        content_type="application/pdf"
    )

    entry1 = RaftLogEntry(index=1, term=1, command=cmd)
    node.persistent_state.log.append(entry1)
    await node.storage.save_state(node.persistent_state)

    assert node.persistent_state.last_log_index == 1
    assert node.persistent_state.last_log_term == 1

    # CRITICAL INVARIANT: Raw file bytes MUST NOT be in log entry dict
    entry_dict = entry1.to_dict()
    assert "bytes" not in entry_dict["command"]
    assert "file_content" not in entry_dict["command"]


@pytest.mark.asyncio
async def test_atomic_persistence_and_crash_reload(temp_node_dir):
    """Verify atomic persistence engine flushes state cleanly and survives node crashes."""
    node1 = RaftNode(node_id="test-node-5", base_dir=temp_node_dir)
    await node1.initialize()

    await node1.transition_to_candidate()
    assert node1.persistent_state.current_term == 1
    assert node1.persistent_state.voted_for == "test-node-5"

    cmd = FileOperationCommand(
        operation=FileOperationType.RENAME,
        file_id="f-200",
        owner_id="user-1",
        filename="new_name.txt",
        sha256="def456hash",
        object_key="users/user-1/f-200/v1",
        size_bytes=512,
        content_type="text/plain"
    )
    entry = RaftLogEntry(index=1, term=1, command=cmd)
    node1.persistent_state.log.append(entry)
    await node1.storage.save_state(node1.persistent_state)

    # Simulate Node 1 crash & restart with Node 2 reading SAME storage directory
    node2 = RaftNode(node_id="test-node-5", base_dir=temp_node_dir)
    await node2.initialize()

    assert node2.persistent_state.current_term == 1
    assert node2.persistent_state.voted_for == "test-node-5"
    assert node2.persistent_state.last_log_index == 1
    assert node2.persistent_state.log[0].command.filename == "new_name.txt"


@pytest.mark.asyncio
async def test_corrupted_persistent_state_fails_safely(temp_node_dir):
    """Verify that a corrupted state.json causes safe startup failure (RaftCorruptedStateError)."""
    node = RaftNode(node_id="corrupt-node", base_dir=temp_node_dir)
    state_file = Path(temp_node_dir) / "corrupt-node" / "state.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Write invalid JSON content
    state_file.write_text("{invalid_json_corrupted: true", encoding="utf-8")

    with pytest.raises(RaftCorruptedStateError):
        await node.initialize()


@pytest.mark.asyncio
async def test_state_isolation_rules(temp_node_dir):
    """Verify separation between persistent state, volatile state, and leader-only volatile state."""
    node = RaftNode(node_id="leader-node", base_dir=temp_node_dir)
    await node.initialize()

    await node.transition_to_candidate()
    peers = ["peer-2:50052", "peer-3:50053"]
    await node.transition_to_leader(peers)

    assert node.role == NodeRole.LEADER
    assert node.leader_state is not None
    assert node.leader_state.next_index["peer-2:50052"] == 1
    assert node.leader_state.match_index["peer-2:50052"] == 0

    # Step down to follower -> leader_state must be cleared!
    await node.transition_to_follower(term=2)
    assert node.role == NodeRole.FOLLOWER
    assert node.leader_state is None
