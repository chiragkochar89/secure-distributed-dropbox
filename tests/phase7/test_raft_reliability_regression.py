import os
import socket
import shutil
import tempfile
import pytest
import asyncio
from typing import List

from app.db.session import SessionLocal, init_db
from app.services.commands import FileOperationCommand, FileOperationType
from app.raft.node import RaftNode, NodeRole
from app.raft.state import LeaderVolatileState
from app.raft.engine import RaftElectionEngine
from app.raft.rpc_client import RaftPeerClientPool
from app.raft.rpc_server import GRPCServerManager


def find_free_ports(count: int = 3) -> List[int]:
    """Dynamically allocates unique free TCP ports from OS kernel."""
    ports = []
    sockets = []
    for _ in range(count):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('127.0.0.1', 0))
        port = s.getsockname()[1]
        ports.append(port)
        sockets.append(s)
    for s in sockets:
        s.close()
    return ports


@pytest.fixture(autouse=True)
def clean_env():
    """Ensure clean database and storage environment before each test."""
    from app.db.session import engine
    engine.dispose()
    init_db()
    storage_dir = os.path.join(os.getcwd(), "data", "storage")
    raft_dir = os.path.join(os.getcwd(), "data", "raft")
    for d in [storage_dir, raft_dir]:
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d, exist_ok=True)

    yield

    engine.dispose()
    for d in [storage_dir, raft_dir]:
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def temp_cluster_dirs():
    dirs = [tempfile.mkdtemp() for _ in range(3)]
    init_db()
    yield dirs
    for d in dirs:
        shutil.rmtree(d, ignore_errors=True)


@pytest.mark.asyncio
async def test_peer_timeout_does_not_hang_replication(temp_cluster_dirs):
    """Test 1: Unreachable peer timing out repeatedly terminates _replicate_to_peer within max_retries without hanging."""
    ports = find_free_ports(2)
    unreachable_addr = f"127.0.0.1:{ports[1]}"

    node1 = RaftNode(node_id="node-1", base_dir=temp_cluster_dirs[0])
    await node1.initialize()
    node1.role = NodeRole.LEADER
    node1.persistent_state.current_term = 1
    node1.leader_state = LeaderVolatileState(
        next_index={unreachable_addr: 1},
        match_index={unreachable_addr: 0}
    )

    cmd = FileOperationCommand(
        operation=FileOperationType.UPLOAD,
        file_id="f-timeout-1",
        owner_id="u1",
        filename="test.txt",
        sha256="hash1",
        object_key="u1/f-timeout-1/v1",
        size_bytes=100,
        version=1
    )
    await node1.append_log_entry(cmd)

    pool1 = RaftPeerClientPool(peers=[unreachable_addr])
    eng1 = RaftElectionEngine(node=node1, peer_pool=pool1)

    try:
        # Must complete within 10.0 seconds timeout constraint
        await asyncio.wait_for(eng1._replicate_to_peer(unreachable_addr, target_index=1, max_retries=2), timeout=10.0)
        # match_index must NOT be updated for failed peer
        assert node1.leader_state.match_index[unreachable_addr] == 0
    finally:
        await pool1.close_all()


@pytest.mark.asyncio
async def test_replication_cancels_on_leadership_loss(temp_cluster_dirs):
    """Test 2: _replicate_to_peer cancels immediately when node role changes from LEADER to FOLLOWER."""
    ports = find_free_ports(2)
    peer_addr = f"127.0.0.1:{ports[1]}"

    node1 = RaftNode(node_id="node-1", base_dir=temp_cluster_dirs[0])
    await node1.initialize()
    node1.role = NodeRole.LEADER
    node1.persistent_state.current_term = 1
    node1.leader_state = LeaderVolatileState(
        next_index={peer_addr: 1},
        match_index={peer_addr: 0}
    )

    # Step down leader to follower
    node1.role = NodeRole.FOLLOWER

    pool1 = RaftPeerClientPool(peers=[peer_addr])
    eng1 = RaftElectionEngine(node=node1, peer_pool=pool1)

    try:
        await asyncio.wait_for(eng1._replicate_to_peer(peer_addr, target_index=5, max_retries=5), timeout=2.0)
        assert node1.leader_state.match_index[peer_addr] == 0
    finally:
        await pool1.close_all()


@pytest.mark.asyncio
async def test_failed_follower_not_marked_replicated(temp_cluster_dirs):
    """Test 3: Unreachable follower is not falsely marked as match_index = target_index."""
    ports = find_free_ports(2)
    peer_addr = f"127.0.0.1:{ports[1]}"

    node1 = RaftNode(node_id="node-1", base_dir=temp_cluster_dirs[0])
    await node1.initialize()
    node1.role = NodeRole.LEADER
    node1.persistent_state.current_term = 1
    node1.leader_state = LeaderVolatileState(
        next_index={peer_addr: 1},
        match_index={peer_addr: 0}
    )

    pool1 = RaftPeerClientPool(peers=[peer_addr])
    eng1 = RaftElectionEngine(node=node1, peer_pool=pool1)

    try:
        await asyncio.wait_for(eng1._replicate_to_peer(peer_addr, target_index=10, max_retries=1), timeout=5.0)
        assert node1.leader_state.match_index[peer_addr] == 0
        assert node1.volatile_state.commit_index == 0
    finally:
        await pool1.close_all()


@pytest.mark.asyncio
async def test_dynamic_port_allocation():
    """Test 4: find_free_ports allocates 3 distinct, open TCP ports."""
    ports = find_free_ports(3)
    assert len(ports) == 3
    assert len(set(ports)) == 3
    for p in ports:
        assert 1024 <= p <= 65535


@pytest.mark.asyncio
async def test_failed_node_startup_teardown_clean(temp_cluster_dirs):
    """Test 5: Partial server startup failure and teardown finish cleanly without hanging."""
    ports = find_free_ports(1)
    node1 = RaftNode(node_id="node-1", base_dir=temp_cluster_dirs[0])
    await node1.initialize()

    srv = GRPCServerManager(port=ports[0], node=node1)
    await srv.start()
    assert srv.server is not None

    # Stop cleanly
    await srv.stop()
