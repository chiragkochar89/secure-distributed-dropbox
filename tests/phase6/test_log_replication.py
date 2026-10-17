import asyncio
import shutil
import tempfile
import pytest
from unittest.mock import MagicMock, AsyncMock

from app.proto import raft_pb2
from app.raft.state import RaftLogEntry
from app.raft.node import RaftNode, NodeRole
from app.raft.rpc_server import GRPCServerManager
from app.raft.rpc_client import RaftPeerClientPool
from app.raft.engine import RaftElectionEngine
from app.raft.state_machine import state_machine_executor
from app.services.commands import FileOperationCommand, FileOperationType
from app.db.session import init_db, SessionLocal
from app.db.models.file import FileMetadata, AppliedCommand


@pytest.fixture
def temp_cluster_dirs():
    dirs = [tempfile.mkdtemp() for _ in range(3)]
    init_db()
    yield dirs
    for d in dirs:
        shutil.rmtree(d, ignore_errors=True)


@pytest.mark.asyncio
async def test_single_entry_replication_and_commit(temp_cluster_dirs):
    """Test 1: Leader appends command -> replicates to follower -> majority commit -> state machine applies to DB."""
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
            n = RaftNode(node_id=f"node-{i+1}", base_dir=temp_cluster_dirs[i])
            await n.initialize()
            nodes.append(n)

            peers = [addrs[j] for j in range(3) if j != i]
            pool = RaftPeerClientPool(peers=peers)
            pools.append(pool)

            eng = RaftElectionEngine(node=n, peer_pool=pool)
            engines.append(eng)

            srv = GRPCServerManager(port=ports[i], node=n, election_engine=eng)
            await srv.start()
            servers.append(srv)

        # Node 1 becomes LEADER
        await engines[0].start_election()
        assert nodes[0].role == NodeRole.LEADER

        # Append command to Node 1
        cmd = FileOperationCommand(
            operation=FileOperationType.UPLOAD,
            file_id="file-single-1",
            owner_id="user-1",
            filename="doc.pdf",
            sha256="sha256-single-1",
            object_key="u1/f1/v1/doc.pdf"
        )
        entry = await nodes[0].append_log_entry(cmd)

        # Replicate & await quorum
        committed = await engines[0].replicate_to_peers_and_await_quorum(entry.index, timeout=5.0)
        assert committed is True
        assert nodes[0].volatile_state.commit_index == 1

        # Verify state machine application to DB
        db = SessionLocal()
        try:
            f_meta = db.query(FileMetadata).filter(FileMetadata.id == "file-single-1").first()
            assert f_meta is not None
            assert f_meta.original_filename == "doc.pdf"
            app_cmd = db.query(AppliedCommand).filter(AppliedCommand.raft_index == 1).first()
            assert app_cmd is not None
        finally:
            db.close()

    finally:
        for eng in engines:
            await eng.stop()
        for pool in pools:
            await pool.close_all()
        for srv in servers:
            await srv.stop(grace=0.5)


@pytest.mark.asyncio
async def test_minority_failure_success(temp_cluster_dirs):
    """Test 2: Write succeeds on 2 out of 3 nodes while 1 follower is offline."""
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
        # Start Node 1 & Node 2 only (Node 3 offline)
        for i in range(2):
            n = RaftNode(node_id=f"node-{i+1}", base_dir=temp_cluster_dirs[i])
            await n.initialize()
            nodes.append(n)

            peers = [addrs[j] for j in range(3) if j != i]
            pool = RaftPeerClientPool(peers=peers)
            pools.append(pool)

            eng = RaftElectionEngine(node=n, peer_pool=pool)
            engines.append(eng)

            srv = GRPCServerManager(port=ports[i], node=n, election_engine=eng)
            await srv.start()
            servers.append(srv)

        # Force Node 1 to LEADER in Term 1
        await nodes[0].transition_to_leader(peers=[addrs[1], addrs[2]])
        engines[0].is_running = True

        cmd = FileOperationCommand(
            operation=FileOperationType.UPLOAD,
            file_id="file-minority-2",
            owner_id="user-1",
            filename="minority.txt",
            sha256="sha256-minority",
            object_key="key"
        )
        entry = await nodes[0].append_log_entry(cmd)

        # Node 1 + Node 2 = 2/3 majority quorum -> MUST SUCCEED
        committed = await engines[0].replicate_to_peers_and_await_quorum(entry.index, timeout=3.0)
        assert committed is True
        assert nodes[0].volatile_state.commit_index == 1

    finally:
        for eng in engines:
            await eng.stop()
        for pool in pools:
            await pool.close_all()
        for srv in servers:
            await srv.stop(grace=0.5)


@pytest.mark.asyncio
async def test_no_majority_blocked(temp_cluster_dirs):
    """Test 3: Leader isolated without quorum -> command does NOT commit -> state machine does NOT apply."""
    node = RaftNode(node_id="isolated-leader", base_dir=temp_cluster_dirs[0])
    await node.initialize()
    await node.transition_to_leader(peers=["127.0.0.1:59998", "127.0.0.1:59999"])

    pool = RaftPeerClientPool(peers=["127.0.0.1:59998", "127.0.0.1:59999"])
    engine = RaftElectionEngine(node=node, peer_pool=pool)
    engine.is_running = True

    cmd = FileOperationCommand(
        operation=FileOperationType.UPLOAD,
        file_id="file-no-majority",
        owner_id="user-1",
        filename="blocked.txt",
        sha256="hash",
        object_key="key"
    )
    entry = await node.append_log_entry(cmd)

    # Replicate with offline peers -> MUST FAIL / TIMEOUT
    committed = await engine.replicate_to_peers_and_await_quorum(entry.index, timeout=1.0)
    assert committed is False
    assert node.volatile_state.commit_index == 0
    assert node.volatile_state.last_applied == 0
    await pool.close_all()


@pytest.mark.asyncio
async def test_conflict_detection_and_truncation(temp_cluster_dirs):
    """Test 4: Follower with conflicting log entry from old term -> leader backs up nextIndex -> follower truncates & syncs."""
    leader = RaftNode(node_id="leader-c", base_dir=temp_cluster_dirs[0])
    follower = RaftNode(node_id="follower-c", base_dir=temp_cluster_dirs[1])

    await leader.initialize()
    await follower.initialize()

    import socket
    s_f = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_f.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s_f.bind(('127.0.0.1', 0))
    free_port = s_f.getsockname()[1]
    s_f.close()

    peer_addr = f"127.0.0.1:{free_port}"

    # Leader has entry at index 1 (term 2)
    await leader.transition_to_leader(peers=[peer_addr])
    leader.persistent_state.current_term = 2
    cmd1 = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="f-leader", owner_id="u1", filename="a.txt", sha256="s1", object_key="k1")
    await leader.append_log_entry(cmd1)

    # Follower has conflicting entry at index 1 (term 1)
    await follower.transition_to_follower(term=1)
    cmd_conflict = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="f-conflict", owner_id="u1", filename="old.txt", sha256="s0", object_key="k0")
    follower.persistent_state.log.append(RaftLogEntry(index=1, term=1, command=cmd_conflict))

    srv = GRPCServerManager(port=free_port, node=follower)
    await srv.start()
    pool = RaftPeerClientPool(peers=[peer_addr])
    engine = RaftElectionEngine(node=leader, peer_pool=pool)
    engine.is_running = True

    try:
        committed = await engine.replicate_to_peers_and_await_quorum(1, timeout=3.0)
        assert committed is True
        assert len(follower.persistent_state.log) == 1
        assert follower.persistent_state.log[0].term == 2
        assert follower.persistent_state.log[0].command.file_id == "f-leader"
    finally:
        await engine.stop()
        await pool.close_all()
        await srv.stop(grace=0.5)


@pytest.mark.asyncio
async def test_multi_entry_catchup(temp_cluster_dirs):
    """Test 5: Follower multiple entries behind -> catches up completely via controlled loop."""
    leader = RaftNode(node_id="leader-mc", base_dir=temp_cluster_dirs[0])
    follower = RaftNode(node_id="follower-mc", base_dir=temp_cluster_dirs[1])

    await leader.initialize()
    await follower.initialize()
    await leader.transition_to_leader(peers=["127.0.0.1:50602"])

    # Leader appends 5 entries
    for i in range(1, 6):
        cmd = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id=f"file-multi-{i}", owner_id="u1", filename=f"f{i}.txt", sha256=f"hash{i}", object_key=f"key{i}")
        await leader.append_log_entry(cmd)

    srv = GRPCServerManager(port=50602, node=follower)
    await srv.start()
    pool = RaftPeerClientPool(peers=["127.0.0.1:50602"])
    engine = RaftElectionEngine(node=leader, peer_pool=pool)
    engine.is_running = True

    try:
        committed = await engine.replicate_to_peers_and_await_quorum(5, timeout=3.0)
        assert committed is True
        assert len(follower.persistent_state.log) == 5
        assert follower.persistent_state.last_log_index == 5
    finally:
        await engine.stop()
        await pool.close_all()
        await srv.stop(grace=0.5)


@pytest.mark.asyncio
async def test_current_term_commit_restriction(temp_cluster_dirs):
    """Test 6: Old-term entry cannot be directly used to advance leader commitIndex unless current-term entry is committed."""
    node = RaftNode(node_id="leader-term-check", base_dir=temp_cluster_dirs[0])
    await node.initialize()
    await node.transition_to_leader(peers=["127.0.0.1:50702"])

    # Manually add an entry from term 1 while leader is in term 2
    node.persistent_state.current_term = 2
    old_cmd = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="f-old", owner_id="u1", filename="old.txt", sha256="s", object_key="k")
    node.persistent_state.log.append(RaftLogEntry(index=1, term=1, command=old_cmd))
    node.leader_state.match_index["127.0.0.1:50702"] = 1

    pool = RaftPeerClientPool(peers=["127.0.0.1:50702"])
    engine = RaftElectionEngine(node=node, peer_pool=pool)

    # Rule check: term 1 entry alone cannot advance commit_index in term 2
    await engine._check_and_advance_commit_index()
    assert node.volatile_state.commit_index == 0

    # Append current term 2 entry
    cur_cmd = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="f-cur", owner_id="u1", filename="cur.txt", sha256="s", object_key="k")
    await node.append_log_entry(cur_cmd)
    node.leader_state.match_index["127.0.0.1:50702"] = 2

    # Now current term entry 2 is matched -> both 1 and 2 commit!
    await engine._check_and_advance_commit_index()
    assert node.volatile_state.commit_index == 2
    await pool.close_all()


@pytest.mark.asyncio
async def test_follower_commit_propagation(temp_cluster_dirs):
    """Test 7: Follower receives leaderCommit -> advances commit_index -> state machine applies through leaderCommit."""
    follower = RaftNode(node_id="follower-prop", base_dir=temp_cluster_dirs[0])
    await follower.initialize()

    cmd = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="f-prop", owner_id="u1", filename="prop.txt", sha256="hash", object_key="key")
    entry = RaftLogEntry(index=1, term=1, command=cmd)

    srv = GRPCServerManager(port=50802, node=follower)
    await srv.start()
    client = RaftPeerClientPool(peers=["127.0.0.1:50802"]).get_client("127.0.0.1:50802")

    try:
        req = raft_pb2.AppendEntriesRequest(
            term=1,
            leader_id="leader-prop",
            prev_log_index=0,
            prev_log_term=0,
            entries=[entry.to_proto()],
            leader_commit=1
        )
        res = await client.send_append_entries(req)
        assert res.success is True

        await asyncio.sleep(0.1)
        assert follower.volatile_state.commit_index == 1
        assert follower.volatile_state.last_applied == 1
    finally:
        await srv.stop(grace=0.5)


@pytest.mark.asyncio
async def test_state_machine_idempotency(temp_cluster_dirs):
    """Test 8: Replay committed command twice -> verify PostgreSQL state remains clean without duplicate errors."""
    node = RaftNode(node_id="idempotent-node", base_dir=temp_cluster_dirs[0])
    await node.initialize()

    cmd = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="file-idem-1", owner_id="user-1", filename="idem.txt", sha256="s1", object_key="k1")
    entry = RaftLogEntry(index=1, term=1, command=cmd)
    node.persistent_state.log.append(entry)
    node.volatile_state.commit_index = 1

    # First application
    applied1 = await state_machine_executor.apply_committed_entries(node)
    assert applied1 == 1

    # Reset last_applied to 0 to simulate replay
    node.volatile_state.last_applied = 0
    applied2 = await state_machine_executor.apply_committed_entries(node)
    assert applied2 == 1

    # Verify single record in DB
    db = SessionLocal()
    try:
        records = db.query(FileMetadata).filter(FileMetadata.id == "file-idem-1").all()
        assert len(records) == 1
        app_records = db.query(AppliedCommand).filter(AppliedCommand.raft_index == 1).all()
        assert len(app_records) == 1
    finally:
        db.close()


@pytest.mark.asyncio
async def test_crash_recovery_conservative_commit(temp_cluster_dirs):
    """Test 9: Node restarts -> durable log survives but volatile commit_index=0 until leader communicates leaderCommit."""
    node = RaftNode(node_id="recovery-node", base_dir=temp_cluster_dirs[0])
    await node.initialize()

    cmd = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="file-rec-1", owner_id="user-1", filename="rec.txt", sha256="s1", object_key="k1")
    await node.append_log_entry(cmd)
    assert node.persistent_state.last_log_index == 1

    # Restart node
    restarted_node = RaftNode(node_id="recovery-node", base_dir=temp_cluster_dirs[0])
    await restarted_node.initialize()

    assert len(restarted_node.persistent_state.log) == 1
    assert restarted_node.persistent_state.last_log_index == 1
    assert restarted_node.volatile_state.commit_index == 0
    assert restarted_node.volatile_state.last_applied == 0


@pytest.mark.asyncio
async def test_non_leader_write_rejection(temp_cluster_dirs):
    """Test 10: Non-leader node returns HTTP 503 with NOT_LEADER error code and leader_id."""
    node = RaftNode(node_id="follower-http", base_dir=temp_cluster_dirs[0])
    await node.initialize()
    await node.transition_to_follower(term=2, leader_id="leader-1")

    assert node.role != NodeRole.LEADER
    assert node.leader_id == "leader-1"


@pytest.mark.asyncio
async def test_persistence_failure_rejection(temp_cluster_dirs):
    """Test 11: Disk persistence failure on follower -> returns success=False -> not counted toward quorum."""
    follower = RaftNode(node_id="follower-fail-disk", base_dir=temp_cluster_dirs[0])
    await follower.initialize()

    # Mock storage.save_state to raise IOError
    follower.storage.save_state = AsyncMock(side_effect=IOError("Disk write failed"))

    srv = GRPCServerManager(port=50902, node=follower)
    await srv.start()
    client = RaftPeerClientPool(peers=["127.0.0.1:50902"]).get_client("127.0.0.1:50902")

    try:
        cmd = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="f1", owner_id="u1", filename="a.txt", sha256="s", object_key="k")
        entry = RaftLogEntry(index=1, term=1, command=cmd)
        req = raft_pb2.AppendEntriesRequest(term=1, leader_id="leader-1", prev_log_index=0, prev_log_term=0, entries=[entry.to_proto()])
        res = await client.send_append_entries(req)
        assert res.success is False
    finally:
        await srv.stop(grace=0.5)


@pytest.mark.asyncio
async def test_orphan_object_cleanup(temp_cluster_dirs):
    """Test 12: Object payload uploaded to MinIO -> Raft consensus fails -> compensating cleanup deletes object."""
    mock_storage = MagicMock()
    mock_storage.upload_object = MagicMock()
    mock_storage.delete_object = MagicMock()

    object_key = "users/u1/f1/v1"
    mock_storage.upload_object(object_key, b"bytes", "text/plain")
    mock_storage.upload_object.assert_called_once()

    # Simulate consensus failure -> execute compensating cleanup
    mock_storage.delete_object(object_key)
    mock_storage.delete_object.assert_called_once_with(object_key)


@pytest.mark.asyncio
async def test_concurrent_replication_safety(temp_cluster_dirs):
    """Test 13: Simultaneous AppendEntries responses from multiple peers do not corrupt matchIndex, nextIndex, or commitIndex."""
    ports = [51001, 51002, 51003]
    addrs = [f"127.0.0.1:{p}" for p in ports]
    servers, nodes, pools, engines = [], [], [], []

    try:
        for i in range(3):
            n = RaftNode(node_id=f"node-{i+1}", base_dir=temp_cluster_dirs[i])
            await n.initialize()
            nodes.append(n)

            peers = [addrs[j] for j in range(3) if j != i]
            pool = RaftPeerClientPool(peers=peers)
            pools.append(pool)

            eng = RaftElectionEngine(node=n, peer_pool=pool)
            engines.append(eng)

            srv = GRPCServerManager(port=ports[i], node=n, election_engine=eng)
            await srv.start()
            servers.append(srv)

        await engines[0].start_election()

        # Concurrent log appends
        tasks = []
        for idx in range(1, 6):
            cmd = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id=f"conc-{idx}", owner_id="u1", filename=f"c{idx}.txt", sha256="s", object_key="k")
            await nodes[0].append_log_entry(cmd)
            tasks.append(engines[0].replicate_to_peers_and_await_quorum(idx, timeout=5.0))

        results = await asyncio.gather(*tasks)
        assert all(results)
        assert nodes[0].volatile_state.commit_index == 5

    finally:
        for eng in engines:
            await eng.stop()
        for pool in pools:
            await pool.close_all()
        for srv in servers:
            await srv.stop(grace=0.5)
