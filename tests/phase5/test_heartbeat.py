import asyncio
import shutil
import tempfile
import pytest

from app.proto import raft_pb2
from app.raft.state import RaftLogEntry
from app.raft.node import RaftNode, NodeRole
from app.raft.rpc_server import GRPCServerManager
from app.raft.rpc_client import RaftPeerClientPool
from app.raft.engine import RaftElectionEngine
from app.services.commands import FileOperationCommand, FileOperationType


@pytest.fixture
def temp_cluster_dirs():
    dirs = [tempfile.mkdtemp() for _ in range(3)]
    yield dirs
    for d in dirs:
        shutil.rmtree(d, ignore_errors=True)


@pytest.mark.asyncio
async def test_heartbeat_loop_lifecycle(temp_cluster_dirs):
    """Verify heartbeat loop start, idempotency, step-down cancellation, and clean shutdown."""
    node = RaftNode(node_id="leader-1", base_dir=temp_cluster_dirs[0])
    await node.initialize()
    await node.transition_to_leader(peers=["127.0.0.1:50099"])

    pool = RaftPeerClientPool(peers=["127.0.0.1:50099"])
    engine = RaftElectionEngine(node=node, peer_pool=pool)
    engine.is_running = True

    # 1. Start heartbeat loop
    await engine.start_heartbeat_loop()
    assert engine.heartbeat_task is not None
    assert not engine.heartbeat_task.done()

    # 2. Idempotent check
    first_task = engine.heartbeat_task
    await engine.start_heartbeat_loop()
    assert engine.heartbeat_task is first_task

    # 3. Stop heartbeat loop
    await engine.stop_heartbeat_loop()
    assert engine.heartbeat_task.done()

    # 4. Engine shutdown stops heartbeat loop
    await engine.start_heartbeat_loop()
    await engine.stop()
    assert engine.heartbeat_task.done()
    await pool.close_all()


@pytest.mark.asyncio
async def test_append_entries_rules_and_log_precheck(temp_cluster_dirs):
    """Test lower term rejection, higher term step-down, same-term candidate step-down, and log pre-checks."""
    node = RaftNode(node_id="follower-node", base_dir=temp_cluster_dirs[0])
    await node.initialize()
    await node.transition_to_follower(term=2)

    server = GRPCServerManager(port=50123, node=node)
    await server.start()
    client = RaftPeerClientPool(peers=["127.0.0.1:50123"]).get_client("127.0.0.1:50123")

    try:
        # 1. Lower term (1 < 2) -> REJECT
        req_lower = raft_pb2.AppendEntriesRequest(term=1, leader_id="leader-old", prev_log_index=0, prev_log_term=0, entries=[])
        res_lower = await client.send_append_entries(req_lower)
        assert res_lower.success is False
        assert res_lower.term == 2
        assert node.leader_id is None

        # 2. Higher term (3 > 2) -> STEP DOWN & ACCEPT
        req_higher = raft_pb2.AppendEntriesRequest(term=3, leader_id="leader-3", prev_log_index=0, prev_log_term=0, entries=[])
        res_higher = await client.send_append_entries(req_higher)
        assert res_higher.success is True
        assert res_higher.term == 3
        assert node.persistent_state.current_term == 3
        assert node.leader_id == "leader-3"

        # 3. Same term CANDIDATE step-down
        await node.transition_to_candidate()
        assert node.role == NodeRole.CANDIDATE
        req_same = raft_pb2.AppendEntriesRequest(term=4, leader_id="leader-4", prev_log_index=0, prev_log_term=0, entries=[])
        res_same = await client.send_append_entries(req_same)
        assert res_same.success is True
        assert node.role == NodeRole.FOLLOWER
        assert node.leader_id == "leader-4"

        # 4. Log consistency pre-check
        # Add log entry at index=1, term=4
        node.persistent_state.log.append(
            RaftLogEntry(
                index=1,
                term=4,
                command=FileOperationCommand(
                    operation=FileOperationType.UPLOAD,
                    file_id="f1",
                    owner_id="u1",
                    version=1,
                    filename="test.txt",
                    sha256="hash",
                    object_key="key"
                )
            )
        )
        # prev_log_index=1, prev_log_term=4 -> MATCH -> Success
        req_match = raft_pb2.AppendEntriesRequest(term=4, leader_id="leader-4", prev_log_index=1, prev_log_term=4, entries=[])
        res_match = await client.send_append_entries(req_match)
        assert res_match.success is True

        # prev_log_index=1, prev_log_term=3 -> MISMATCH -> Fail
        req_mismatch = raft_pb2.AppendEntriesRequest(term=4, leader_id="leader-4", prev_log_index=1, prev_log_term=3, entries=[])
        res_mismatch = await client.send_append_entries(req_mismatch)
        assert res_mismatch.success is False

        # prev_log_index=5 -> MISSING -> Fail
        req_missing = raft_pb2.AppendEntriesRequest(term=4, leader_id="leader-4", prev_log_index=5, prev_log_term=4, entries=[])
        res_missing = await client.send_append_entries(req_missing)
        assert res_missing.success is False

    finally:
        await server.stop(grace=0.5)


@pytest.mark.asyncio
async def test_3node_heartbeat_cluster_execution(temp_cluster_dirs):
    """
    Full 3-Node Cluster Heartbeat Verification:
    Leader (Node 1) streams AppendEntries heartbeats to Node 2 and Node 3 concurrently.
    Verifies followers stay in FOLLOWER role and election timers do not trigger elections.
    Verifies Phase 5 boundaries (no commitIndex advancement or log entry mutation).
    """
    import socket
    ports = []
    sockets = []
    for _ in range(3):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('127.0.0.1', 0))
        ports.append(s.getsockname()[1])
        sockets.append(s)
    for s in sockets:
        s.close()
    addrs = [f"127.0.0.1:{p}" for p in ports]

    servers, nodes, pools, engines = [], [], [], []

    try:
        for i in range(3):
            nid = f"node-{i+1}"
            peers = [addrs[j] for j in range(3) if j != i]
            n = RaftNode(node_id=nid, base_dir=temp_cluster_dirs[i])
            await n.initialize()
            nodes.append(n)

            pool = RaftPeerClientPool(peers=peers)
            pools.append(pool)

            eng = RaftElectionEngine(node=n, peer_pool=pool, min_timeout_ms=150, max_timeout_ms=300)
            engines.append(eng)

            srv = GRPCServerManager(port=ports[i], node=n, election_engine=eng)
            await srv.start()
            servers.append(srv)

            await eng.start()

        # Node 1 starts election and becomes LEADER
        await engines[0].start_election()
        assert nodes[0].role == NodeRole.LEADER
        assert engines[0].heartbeat_task is not None

        # Let heartbeats run for 200ms
        await asyncio.sleep(0.2)

        # Verify Node 2 and Node 3 received heartbeats and remained FOLLOWERS in term 1
        assert nodes[1].role == NodeRole.FOLLOWER
        assert nodes[1].persistent_state.current_term == 1
        assert nodes[1].leader_id == "node-1"

        assert nodes[2].role == NodeRole.FOLLOWER
        assert nodes[2].persistent_state.current_term == 1
        assert nodes[2].leader_id == "node-1"

        # Phase 5 Boundary Verification:
        # Confirm no log entries appended, no commitIndex advancement
        for n in nodes:
            assert len(n.persistent_state.log) == 0
            assert n.volatile_state.commit_index == 0
            assert n.volatile_state.last_applied == 0

    finally:
        for eng in engines:
            await eng.stop()
        for pool in pools:
            await pool.close_all()
        for srv in servers:
            await srv.stop(grace=0.5)
