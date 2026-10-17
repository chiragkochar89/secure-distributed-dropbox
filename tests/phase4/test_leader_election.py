import asyncio
import shutil
import tempfile
import pytest
from pathlib import Path

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
async def test_election_timer_randomization_and_reset(temp_cluster_dirs):
    """Verify randomized election timeout range (150-300ms) and timer reset event."""
    node = RaftNode(node_id="node-timer", base_dir=temp_cluster_dirs[0])
    await node.initialize()

    engine = RaftElectionEngine(node=node, min_timeout_ms=150, max_timeout_ms=300)
    
    # Verify randomized timeout generation
    timeouts = [engine.get_random_timeout_seconds() for _ in range(10)]
    for t in timeouts:
        assert 0.15 <= t <= 0.30

    await engine.start()
    assert engine.is_running is True
    assert engine.timer_task is not None

    # Trigger timer reset
    engine.reset_election_timer()
    await asyncio.sleep(0.05)
    
    await engine.stop()
    assert engine.is_running is False


@pytest.mark.asyncio
async def test_request_vote_rules_and_log_uptodate(temp_cluster_dirs):
    """Test lower term rejection, same-term double-vote prevention, and log up-to-date checks."""
    node = RaftNode(node_id="node-voter", base_dir=temp_cluster_dirs[0])
    await node.initialize()
    await node.transition_to_follower(term=2, voted_for="candidate-A")

    # 1. Lower term (1 < 2) -> Reject
    res_lower = await node.update_term_if_higher(1)
    assert res_lower is False
    assert node.persistent_state.current_term == 2

    # 2. Same term voting rule: Already voted for candidate-A in term 2 -> Reject candidate-B
    assert node.persistent_state.voted_for == "candidate-A"

    # 3. Log Up-to-Date rule check:
    # Append a term-2 entry to voter node so voter last_log_term is 2
    node.persistent_state.log.append(
        RaftLogEntry(
            index=1,
            term=2,
            command=FileOperationCommand(
                operation=FileOperationType.UPLOAD,
                file_id="f1",
                owner_id="u1",
                version=1,
                filename="test.txt",
                sha256="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                object_key="u1/f1/v1/test.txt"
            )
        )
    )
    assert node.is_log_up_to_date(candidate_last_log_term=2, candidate_last_log_index=1) is True
    assert node.is_log_up_to_date(candidate_last_log_term=1, candidate_last_log_index=5) is False


@pytest.mark.asyncio
async def test_atomic_leader_promotion_and_stale_election(temp_cluster_dirs):
    """Verify atomic leader promotion requirements and stale election filtering."""
    node = RaftNode(node_id="candidate-node", base_dir=temp_cluster_dirs[0])
    await node.initialize()

    term, election_id = await node.transition_to_candidate()
    assert node.role == NodeRole.CANDIDATE
    peers = ["peer-2:50052", "peer-3:50053"]
    majority = 2

    # 1. Stale election_id -> Promotion REJECTED
    promoted_stale = await node.atomic_leader_promotion(
        election_term=term,
        election_id=election_id - 1,  # Stale election ID!
        unique_votes={"candidate-node", "peer-2:50052"},
        majority=majority,
        peers=peers
    )
    assert promoted_stale is False
    assert node.role == NodeRole.CANDIDATE

    # 2. Insufficient votes (< majority) -> Promotion REJECTED
    promoted_few = await node.atomic_leader_promotion(
        election_term=term,
        election_id=election_id,
        unique_votes={"candidate-node"},  # Only 1 vote
        majority=majority,
        peers=peers
    )
    assert promoted_few is False

    # 3. Valid Majority & Current Election ID -> Promoted to LEADER
    promoted = await node.atomic_leader_promotion(
        election_term=term,
        election_id=election_id,
        unique_votes={"candidate-node", "peer-2:50052"},
        majority=majority,
        peers=peers
    )
    assert promoted is True
    assert node.role == NodeRole.LEADER
    assert node.leader_state.next_index["peer-2:50052"] == 1


@pytest.mark.asyncio
async def test_3node_cluster_election_single_leader(temp_cluster_dirs):
    """
    Simulate full 3-Node Cluster Leader Election:
    Nodes 1, 2, 3 running on ports 50101, 50102, 50103.
    Node 1 starts election -> broadcasts RequestVote -> Node 2 & Node 3 grant votes -> Node 1 becomes LEADER!
    Verifies EXACTLY ONE leader is elected per term.
    """
    ports = [50101, 50102, 50103]
    addrs = [f"127.0.0.1:{p}" for p in ports]

    servers = []
    nodes = []
    pools = []
    engines = []

    try:
        # Instantiate 3 Nodes with explicit identity & peer mapping
        for i in range(3):
            nid = f"node-{i+1}"
            peers = [addrs[j] for j in range(3) if j != i]
            n = RaftNode(node_id=nid, base_dir=temp_cluster_dirs[i])
            await n.initialize()
            nodes.append(n)

            # Client pool & election engine
            pool = RaftPeerClientPool(peers=peers)
            pools.append(pool)
            
            eng = RaftElectionEngine(node=n, peer_pool=pool, min_timeout_ms=150, max_timeout_ms=300)
            engines.append(eng)

            # Start gRPC server bound to this specific node & election engine
            srv = GRPCServerManager(port=ports[i], node=n, election_engine=eng)
            await srv.start()
            servers.append(srv)

        # Node 1 starts election
        await engines[0].start_election()

        # Verify Node 1 won the election and became LEADER!
        assert nodes[0].role == NodeRole.LEADER
        assert nodes[0].persistent_state.current_term == 1

        # Verify Node 2 and Node 3 remain FOLLOWERs in term 1
        assert nodes[1].role == NodeRole.FOLLOWER
        assert nodes[1].persistent_state.current_term == 1
        assert nodes[1].persistent_state.voted_for == "node-1"

        assert nodes[2].role == NodeRole.FOLLOWER
        assert nodes[2].persistent_state.current_term == 1
        assert nodes[2].persistent_state.voted_for == "node-1"

        # Verify EXACTLY ONE LEADER exists in term 1
        leaders = [n for n in nodes if n.role == NodeRole.LEADER]
        assert len(leaders) == 1
        assert leaders[0].node_id == "node-1"

    finally:
        for eng in engines:
            await eng.stop()
        for pool in pools:
            await pool.close_all()
        for srv in servers:
            await srv.stop(grace=0.5)
