import os
import shutil
import tempfile
import uuid
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

from app.db.session import SessionLocal, init_db
from app.db.models.file import FileMetadata, FileVersion, AppliedCommand
from app.db.models.user import User
from app.services.commands import FileOperationCommand, FileOperationType
from app.proto import raft_pb2
from app.raft.node import RaftNode, NodeRole
from app.raft.state import RaftLogEntry
from app.raft.rpc_server import RaftConsensusServicer, GRPCServerManager
from app.raft.rpc_client import RaftPeerClientPool
from app.raft.engine import RaftElectionEngine
from app.raft.snapshot import RaftSnapshot, SnapshotManager, dump_postgres_state_machine
from app.core.crypto import compute_sha256


@pytest.fixture
def temp_cluster_dirs():
    dirs = [tempfile.mkdtemp() for _ in range(3)]
    init_db()
    yield dirs
    for d in dirs:
        shutil.rmtree(d, ignore_errors=True)


@pytest.mark.asyncio
async def test_install_snapshot_sequential_unary_chunks(temp_cluster_dirs):
    """Test 1: Leader streams multi-chunk snapshot over sequential 64 KB unary RPC calls with valid offsets."""
    import socket
    ports = []
    sockets = []
    for _ in range(2):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('127.0.0.1', 0))
        ports.append(s.getsockname()[1])
        sockets.append(s)
    for s in sockets:
        s.close()
    addrs = [f"127.0.0.1:{p}" for p in ports]

    node1 = RaftNode(node_id="node-1", base_dir=temp_cluster_dirs[0])
    await node1.initialize()
    node1.role = NodeRole.LEADER
    node1.persistent_state.current_term = 1
    if node1.leader_state is None:
        from app.raft.state import LeaderVolatileState
        node1.leader_state = LeaderVolatileState(next_index={}, match_index={})

    node2 = RaftNode(node_id="node-2", base_dir=temp_cluster_dirs[1])
    await node2.initialize()
    node2.role = NodeRole.FOLLOWER
    node2.persistent_state.current_term = 1

    # Populate PostgreSQL state on leader
    db = SessionLocal()
    u = User(id="u-is-1", email="u1@snap.local", username="u1", password_hash="hash")
    fm = FileMetadata(id="f-is-1", owner_id="u-is-1", original_filename="doc.pdf", sanitized_name="doc.pdf", content_type="app/pdf", size_bytes=50000, current_sha256="sha50000", current_version=1, is_deleted=False)
    fv = FileVersion(id="fv-1", file_id="f-is-1", version_number=1, object_key="u/f/v1", size_bytes=50000, sha256_hash="sha50000")
    db.add(u)
    db.add(fm)
    db.add(fv)
    db.commit()
    db.close()

    # Create snapshot on Leader at index 10
    state_data = dump_postgres_state_machine()
    checksum = compute_sha256(dump_bytes(state_data))
    snapshot = RaftSnapshot(
        last_included_index=10,
        last_included_term=1,
        state_machine_checksum=checksum,
        metadata_state=state_data
    )
    SnapshotManager.save_snapshot_atomically(node1.node_id, snapshot, temp_cluster_dirs[0])
    node1.compact_log(10, 1)

    # Start follower gRPC server
    pool1 = RaftPeerClientPool(peers=[addrs[1]])
    eng1 = RaftElectionEngine(node=node1, peer_pool=pool1)
    
    eng2 = RaftElectionEngine(node=node2)
    srv2 = GRPCServerManager(port=ports[1], node=node2, election_engine=eng2)
    await srv2.start()

    try:
        # Dispatch InstallSnapshot from Leader to Follower
        success = await eng1._send_install_snapshot(addrs[1])
        assert success is True
        assert node1.leader_state.match_index[addrs[1]] == 10
        assert node1.leader_state.next_index[addrs[1]] == 11

        # Verify Follower activated snapshot at index 10
        assert node2.last_included_index == 10
        assert node2.volatile_state.commit_index == 10
        assert node2.volatile_state.last_applied == 10
    finally:
        await srv2.stop()
        await pool1.close_all()


@pytest.mark.asyncio
async def test_install_snapshot_checksum_failure(temp_cluster_dirs):
    """Test 2: Follower rejects snapshot if state_machine_checksum does not match payload."""
    node2 = RaftNode(node_id="node-2", base_dir=temp_cluster_dirs[1])
    await node2.initialize()
    node2.persistent_state.current_term = 1

    servicer = RaftConsensusServicer(node=node2)

    # Create corrupted snapshot JSON with bad checksum
    state_data = dump_postgres_state_machine()
    snap_bad = RaftSnapshot(
        last_included_index=5,
        last_included_term=1,
        state_machine_checksum="bad_corrupted_checksum",
        metadata_state=state_data
    )
    # Bypass model_validate checksum check to test server verification
    json_bad = snap_bad.model_dump_json(indent=2).encode("utf-8")

    req = raft_pb2.InstallSnapshotRequest(
        term=1,
        leader_id="node-1",
        last_included_index=5,
        last_included_term=1,
        offset=0,
        data=json_bad,
        done=True
    )

    resp = await servicer.InstallSnapshot(req, MagicMock())
    assert resp.term == 1

    # Follower should NOT have activated invalid snapshot (remains at index 0)
    assert node2.last_included_index == 0


@pytest.mark.asyncio
async def test_install_snapshot_matching_suffix_preserved(temp_cluster_dirs):
    """Test 3: Follower preserves matching log entries > last_included_index after snapshot installation."""
    node2 = RaftNode(node_id="node-2", base_dir=temp_cluster_dirs[1])
    await node2.initialize()
    node2.persistent_state.current_term = 1

    # Append entries 1..12 to follower log
    for i in range(1, 13):
        cmd = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id=f"f-{i}", owner_id="u", filename=f"{i}.txt", sha256="h", object_key="k", version=1)
        entry = RaftLogEntry(index=i, term=1, command=cmd)
        node2.persistent_state.log.append(entry)

    servicer = RaftConsensusServicer(node=node2)

    state_data = dump_postgres_state_machine()
    checksum = compute_sha256(dump_bytes(state_data))
    snap = RaftSnapshot(last_included_index=10, last_included_term=1, state_machine_checksum=checksum, metadata_state=state_data)
    json_bytes = snap.to_json().encode("utf-8")

    req = raft_pb2.InstallSnapshotRequest(term=1, leader_id="node-1", last_included_index=10, last_included_term=1, offset=0, data=json_bytes, done=True)
    await servicer.InstallSnapshot(req, MagicMock())

    # Verify follower compacts log up to index 10 and PRESERVES entries 11 and 12
    assert node2.last_included_index == 10
    remaining = node2.persistent_state.log
    assert len(remaining) == 2
    assert remaining[0].index == 11
    assert remaining[1].index == 12


@pytest.mark.asyncio
async def test_follower_state_machine_restore_from_snapshot(temp_cluster_dirs):
    """Test 4: Follower PostgreSQL metadata state machine matches leader after InstallSnapshot."""
    node2 = RaftNode(node_id="node-2", base_dir=temp_cluster_dirs[1])
    await node2.initialize()

    servicer = RaftConsensusServicer(node=node2)

    # Populate state data representing leader state
    user_id = str(uuid.uuid4())
    file_id = str(uuid.uuid4())
    state_data = {
        "users": [{"id": user_id, "email": "restored@test.local", "username": "restored", "password_hash": "hash", "role": "USER", "is_active": True}],
        "file_metadata": [{"id": file_id, "owner_id": user_id, "original_filename": "restored.txt", "sanitized_name": "restored.txt", "content_type": "text/plain", "size_bytes": 1234, "current_sha256": "sha1234", "current_version": 1, "is_deleted": False}],
        "file_versions": [{"id": str(uuid.uuid4()), "file_id": file_id, "version_number": 1, "object_key": "u/f/v1", "size_bytes": 1234, "sha256_hash": "sha1234"}],
        "audit_logs": [],
        "applied_commands": [{"raft_index": 5, "term": 1, "command_type": "UPLOAD", "file_id": file_id}]
    }
    checksum = compute_sha256(dump_bytes(state_data))
    snap = RaftSnapshot(last_included_index=5, last_included_term=1, state_machine_checksum=checksum, metadata_state=state_data)

    req = raft_pb2.InstallSnapshotRequest(term=1, leader_id="node-1", last_included_index=5, last_included_term=1, offset=0, data=snap.to_json().encode("utf-8"), done=True)
    await servicer.InstallSnapshot(req, MagicMock())

    # Verify follower PostgreSQL DB contains restored user and file metadata
    db = SessionLocal()
    db_user = db.query(User).filter(User.id == user_id).first()
    db_file = db.query(FileMetadata).filter(FileMetadata.id == file_id).first()
    assert db_user is not None
    assert db_user.username == "restored"
    assert db_file is not None
    assert db_file.sanitized_name == "restored.txt"
    db.close()


@pytest.mark.asyncio
async def test_leader_next_index_updated_after_snapshot(temp_cluster_dirs):
    """Test 5: Leader updates match_index = last_included_index and next_index = last_included_index + 1 after snapshot success."""
    import socket
    ports = []
    sockets = []
    for _ in range(2):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('127.0.0.1', 0))
        ports.append(s.getsockname()[1])
        sockets.append(s)
    for s in sockets:
        s.close()
    addrs = [f"127.0.0.1:{p}" for p in ports]

    node1 = RaftNode(node_id="node-1", base_dir=temp_cluster_dirs[0])
    await node1.initialize()
    node1.role = NodeRole.LEADER
    node1.persistent_state.current_term = 2
    if node1.leader_state is None:
        from app.raft.state import LeaderVolatileState
        node1.leader_state = LeaderVolatileState(next_index={}, match_index={})
    node1.leader_state.next_index[addrs[1]] = 1
    node1.leader_state.match_index[addrs[1]] = 0

    node2 = RaftNode(node_id="node-2", base_dir=temp_cluster_dirs[1])
    await node2.initialize()
    node2.persistent_state.current_term = 2

    # Save leader snapshot at index 15
    state_data = dump_postgres_state_machine()
    checksum = compute_sha256(dump_bytes(state_data))
    snap = RaftSnapshot(last_included_index=15, last_included_term=2, state_machine_checksum=checksum, metadata_state=state_data)
    SnapshotManager.save_snapshot_atomically(node1.node_id, snap, temp_cluster_dirs[0])
    node1.compact_log(15, 2)

    pool1 = RaftPeerClientPool(peers=[addrs[1]])
    eng1 = RaftElectionEngine(node=node1, peer_pool=pool1)

    eng2 = RaftElectionEngine(node=node2)
    srv2 = GRPCServerManager(port=ports[1], node=node2, election_engine=eng2)
    await srv2.start()

    try:
        success = await eng1._send_install_snapshot(addrs[1])
        assert success is True
        assert node1.leader_state.match_index[addrs[1]] == 15
        assert node1.leader_state.next_index[addrs[1]] == 16
    finally:
        await srv2.stop()
        await pool1.close_all()


@pytest.mark.asyncio
async def test_corrupted_snapshot_preserves_existing_valid_snapshot(temp_cluster_dirs):
    """Test 6: Verify corrupted incoming snapshot transfer fails and leaves existing valid snapshot.json byte-for-byte unchanged."""
    import socket
    s_f = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_f.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s_f.bind(('127.0.0.1', 0))
    port = s_f.getsockname()[1]
    s_f.close()
    addr = f"127.0.0.1:{port}"

    follower_node = RaftNode(node_id="node-2", base_dir=temp_cluster_dirs[1])
    await follower_node.initialize()
    follower_node.persistent_state.current_term = 1

    # 1. Create and persist valid snapshot.json on follower at index 5
    state_data = dump_postgres_state_machine()
    checksum = compute_sha256(dump_bytes(state_data))
    original_snap = RaftSnapshot(
        last_included_index=5,
        last_included_term=1,
        state_machine_checksum=checksum,
        metadata_state=state_data
    )
    original_path = SnapshotManager.save_snapshot_atomically(follower_node.node_id, original_snap, temp_cluster_dirs[1])

    # 2. Record exact bytes and hash of original valid snapshot.json
    with open(original_path, "rb") as f:
        original_bytes = f.read()
    original_hash = compute_sha256(original_bytes)

    # 3. Start Follower gRPC server
    eng2 = RaftElectionEngine(node=follower_node)
    srv2 = GRPCServerManager(port=port, node=follower_node, election_engine=eng2)
    await srv2.start()

    pool = RaftPeerClientPool(peers=[addr])
    client = pool.get_client(addr)

    try:
        # 4. Send corrupted snapshot request with done=True over gRPC
        corrupted_payload = b"{ 'invalid_json': True, corrupted_bytes... }"
        req = raft_pb2.InstallSnapshotRequest(
            term=1,
            leader_id="node-1",
            last_included_index=20,
            last_included_term=1,
            offset=0,
            data=corrupted_payload,
            done=True
        )

        res = await client.send_install_snapshot(req, timeout=5.0)
        assert res is not None
        assert res.term == 1

        # 5. Verify snapshot.json still exists
        assert os.path.exists(original_path)

        # 6. Verify snapshot.json is byte-for-byte identical to original
        with open(original_path, "rb") as f:
            current_bytes = f.read()
        assert current_bytes == original_bytes
        assert compute_sha256(current_bytes) == original_hash

        # 7. Verify temporary snapshot was cleaned up and not activated
        target_dir = SnapshotManager.get_snapshot_dir(follower_node.node_id, temp_cluster_dirs[1])
        temp_path = os.path.join(target_dir, "snapshot.installing.tmp")
        assert not os.path.exists(temp_path)

        # 8. Verify follower state machine indices were NOT advanced
        assert follower_node.last_included_index == 0 or follower_node.last_included_index == 5
    finally:
        await srv2.stop()
        await pool.close_all()


def dump_bytes(data):
    import json
    return json.dumps(data, sort_keys=True).encode("utf-8")
