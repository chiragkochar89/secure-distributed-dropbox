import os
import shutil
import tempfile
import pytest
import asyncio
from unittest.mock import AsyncMock

from app.raft.node import RaftNode, NodeRole
from app.raft.state import LeaderVolatileState, RaftLogEntry
from app.raft.engine import RaftElectionEngine
from app.raft.rpc_client import RaftPeerClientPool
from app.raft.rpc_server import RaftConsensusServicer
from app.proto import raft_pb2
from app.services.commands import FileOperationCommand, FileOperationType


@pytest.mark.asyncio
async def test_partition_scenario_a_follower_isolation():
    """
    Scenario A: Follower isolation.
    3-node cluster: Node 1 (Leader), Node 2 (Follower), Node 3 (Follower).
    Node 3 is partitioned off.
    Leader can still communicate with Node 2 (forming a 2/3 majority).
    Leader must successfully commit and advance commit_index.
    """
    dir1 = tempfile.mkdtemp()
    dir2 = tempfile.mkdtemp()
    dir3 = tempfile.mkdtemp()

    try:
        node1 = RaftNode(node_id="node-1", base_dir=dir1)
        await node1.initialize()
        node1.role = NodeRole.LEADER
        node1.persistent_state.current_term = 1

        node2 = RaftNode(node_id="node-2", base_dir=dir2)
        await node2.initialize()
        node2.persistent_state.current_term = 1

        node3 = RaftNode(node_id="node-3", base_dir=dir3)
        await node3.initialize()
        node3.persistent_state.current_term = 1

        peer2_addr = "peer2:50052"
        peer3_addr = "peer3:50053"

        node1.leader_state = LeaderVolatileState(
            next_index={peer2_addr: 1, peer3_addr: 1},
            match_index={peer2_addr: 0, peer3_addr: 0}
        )

        pool1 = RaftPeerClientPool(peers=[peer2_addr, peer3_addr])
        eng1 = RaftElectionEngine(node=node1, peer_pool=pool1)

        # Servicer for node2 directly uses production RaftConsensusServicer logic
        servicer2 = RaftConsensusServicer(node=node2, election_engine=RaftElectionEngine(node=node2))

        async def mock_send_ae_peer2(req, timeout=1.0):
            return await servicer2.AppendEntries(req, AsyncMock())

        # Peer 3 is partitioned: returns None (RPC failure/timeout)
        async def mock_send_ae_peer3(req, timeout=1.0):
            return None

        client2 = pool1.get_client(peer2_addr)
        client3 = pool1.get_client(peer3_addr)
        client2.send_append_entries = mock_send_ae_peer2
        client3.send_append_entries = mock_send_ae_peer3

        # Append entry on leader
        cmd = FileOperationCommand(
            operation=FileOperationType.UPLOAD,
            file_id="f-iso-1",
            owner_id="u1",
            filename="test.txt",
            sha256="hash123",
            size_bytes=100
        )
        await node1.append_log_entry(cmd)
        assert node1.last_log_index == 1

        # Replicate with timeout
        committed = await eng1.replicate_to_peers_and_await_quorum(target_index=1, timeout=2.0)

        # Quorum is 2 out of 3: Node 1 (self) + Node 2 = 2 (Majority reached!)
        assert committed is True
        assert node1.volatile_state.commit_index == 1
        assert node2.last_log_index == 1
        assert node3.last_log_index == 0  # Node 3 received nothing

        await pool1.close_all()

    finally:
        shutil.rmtree(dir1, ignore_errors=True)
        shutil.rmtree(dir2, ignore_errors=True)
        shutil.rmtree(dir3, ignore_errors=True)


@pytest.mark.asyncio
async def test_partition_scenario_b_leader_isolation_and_reelection():
    """
    Scenario B: Leader isolation & follower majority election.
    Partition splits cluster: [Node 1 (old leader)] | [Node 2, Node 3].
    1. Node 1 cannot commit (lacks majority, 1/3).
    2. Node 2 & Node 3 elect Node 2 as new Leader for Term 2.
    3. Node 2 commits new entries with Node 3.
    """
    dir1 = tempfile.mkdtemp()
    dir2 = tempfile.mkdtemp()
    dir3 = tempfile.mkdtemp()

    try:
        # Node 1: Old leader in Term 1
        node1 = RaftNode(node_id="node-1", base_dir=dir1)
        await node1.initialize()
        node1.role = NodeRole.LEADER
        node1.persistent_state.current_term = 1

        # Node 2 & Node 3: Followers in Term 1
        node2 = RaftNode(node_id="node-2", base_dir=dir2)
        await node2.initialize()
        node2.persistent_state.current_term = 1

        node3 = RaftNode(node_id="node-3", base_dir=dir3)
        await node3.initialize()
        node3.persistent_state.current_term = 1

        p2_addr = "p2:50052"
        p3_addr = "p3:50053"
        node1.leader_state = LeaderVolatileState(
            next_index={p2_addr: 1, p3_addr: 1},
            match_index={p2_addr: 0, p3_addr: 0}
        )

        pool1 = RaftPeerClientPool(peers=[p2_addr, p3_addr])
        eng1 = RaftElectionEngine(node=node1, peer_pool=pool1)

        # Leader 1 is isolated from p2 and p3 (all RPCs fail)
        pool1.get_client(p2_addr).send_append_entries = AsyncMock(return_value=None)
        pool1.get_client(p3_addr).send_append_entries = AsyncMock(return_value=None)

        # Node 1 tries to commit
        cmd1 = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="f1", owner_id="u1", filename="f1.txt")
        await node1.append_log_entry(cmd1)

        committed_node1 = await eng1.replicate_to_peers_and_await_quorum(target_index=1, timeout=0.3)
        # Node 1 cannot commit!
        assert committed_node1 is False
        assert node1.volatile_state.commit_index == 0

        # Majority partition: Node 2 and Node 3
        # Node 2 starts election for Term 2
        p1_from_2 = "p1:50051"
        p3_from_2 = "p3:50053"
        pool2 = RaftPeerClientPool(peers=[p1_from_2, p3_from_2])
        eng2 = RaftElectionEngine(node=node2, peer_pool=pool2)

        # Node 1 cannot be reached by Node 2
        pool2.get_client(p1_from_2).send_request_vote = AsyncMock(return_value=None)

        # Node 3 servicer handles RequestVote
        servicer3 = RaftConsensusServicer(node=node3, election_engine=RaftElectionEngine(node=node3))

        async def mock_rv_node3(req, timeout=1.0):
            return await servicer3.RequestVote(req, AsyncMock())

        pool2.get_client(p3_from_2).send_request_vote = mock_rv_node3

        # Run election on Node 2
        await eng2.start_election()

        # Node 2 won election! (Votes: Node 2 self + Node 3 = 2/3)
        assert node2.role == NodeRole.LEADER
        assert node2.persistent_state.current_term == 2

        # Node 2 appends and commits on majority
        pool2.get_client(p1_from_2).send_append_entries = AsyncMock(return_value=None)

        async def mock_ae_node3(req, timeout=1.0):
            return await servicer3.AppendEntries(req, AsyncMock())

        pool2.get_client(p3_from_2).send_append_entries = mock_ae_node3

        cmd2 = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="f2", owner_id="u1", filename="f2.txt")
        await node2.append_log_entry(cmd2)
        committed_node2 = await eng2.replicate_to_peers_and_await_quorum(target_index=1, timeout=1.0)

        assert committed_node2 is True
        assert node2.volatile_state.commit_index == 1
        assert node3.last_log_index == 1

        await pool1.close_all()
        await pool2.close_all()

    finally:
        shutil.rmtree(dir1, ignore_errors=True)
        shutil.rmtree(dir2, ignore_errors=True)
        shutil.rmtree(dir3, ignore_errors=True)


@pytest.mark.asyncio
async def test_partition_scenario_c_complete_loss_of_majority():
    """
    Scenario C: Complete loss of quorum majority.
    Single node remains operational; 2 nodes are offline or unreachable.
    Attempting writes must timeout and return failure (503 condition).
    """
    tmp_dir = tempfile.mkdtemp()
    try:
        node = RaftNode(node_id="isolated-node", base_dir=tmp_dir)
        await node.initialize()
        node.role = NodeRole.LEADER
        node.persistent_state.current_term = 1

        p1 = "peer1:50051"
        p2 = "peer2:50052"
        node.leader_state = LeaderVolatileState(
            next_index={p1: 1, p2: 1},
            match_index={p1: 0, p2: 0}
        )
        pool = RaftPeerClientPool(peers=[p1, p2])
        eng = RaftElectionEngine(node=node, peer_pool=pool)

        # Both peers fail
        pool.get_client(p1).send_append_entries = AsyncMock(return_value=None)
        pool.get_client(p2).send_append_entries = AsyncMock(return_value=None)

        cmd = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="fx", owner_id="u1", filename="x.txt")
        await node.append_log_entry(cmd)

        res = await eng.replicate_to_peers_and_await_quorum(target_index=1, timeout=0.3)
        assert res is False
        assert node.volatile_state.commit_index == 0

        await pool.close_all()
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_partition_scenario_d_healing_and_log_conflict_reconciliation():
    """
    Scenario D: Partition heals after competing uncommitted logs (Raft §5.3).
    Node 1 had an uncommitted entry from Term 1 at index 2.
    Node 2 (new leader, Term 2) has valid committed entries at index 2.
    Upon reconnect, Node 2's AppendEntries forces Node 1 to truncate its
    conflicting uncommitted entry and overwrite it with the leader's entry.
    """
    dir1 = tempfile.mkdtemp()
    try:
        node1 = RaftNode(node_id="node-1", base_dir=dir1)
        await node1.initialize()
        node1.persistent_state.current_term = 1

        # Entry 1 was committed in Term 1
        cmd1 = FileOperationCommand(operation=FileOperationType.REGISTER_USER, file_id="u1", owner_id="u1", filename="u1")
        await node1.append_log_entry(cmd1)
        node1.volatile_state.commit_index = 1

        # Entry 2 was appended locally on Node 1 in Term 1 while partitioned (NEVER COMMITTED)
        cmd_stale = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="stale", owner_id="u1", filename="stale.txt")
        await node1.append_log_entry(cmd_stale)
        assert node1.last_log_index == 2
        assert node1.get_log_term_at(2) == 1

        # Healing: Leader Node 2 (Term 2) sends AppendEntries:
        # prev_log_index=1, prev_log_term=1, leader_commit=2
        # entries: new entry at index 2 with term 2
        cmd_leader = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="valid", owner_id="u1", filename="valid.txt")
        new_entry_term2 = RaftLogEntry(index=2, term=2, command=cmd_leader)

        req = raft_pb2.AppendEntriesRequest(
            term=2,
            leader_id="node-2",
            prev_log_index=1,
            prev_log_term=1,
            entries=[new_entry_term2.to_proto()],
            leader_commit=2
        )

        servicer = RaftConsensusServicer(node=node1, election_engine=RaftElectionEngine(node=node1))
        context = AsyncMock()

        res = await servicer.AppendEntries(req, context)
        assert res.success is True
        assert res.term == 2

        # Invariant: Node 1 must have stepped down to follower, updated term to 2
        assert node1.persistent_state.current_term == 2
        assert node1.role == NodeRole.FOLLOWER

        # Invariant: Conflicting entry at index 2 must have been truncated and replaced!
        assert node1.last_log_index == 2
        assert node1.get_log_term_at(2) == 2
        assert node1.get_log_entry(2).command.file_id == "valid"
        assert node1.volatile_state.commit_index == 2

    finally:
        shutil.rmtree(dir1, ignore_errors=True)


@pytest.mark.asyncio
async def test_partition_scenario_e_transient_rpc_drop_recovery():
    """
    Scenario E: Dropped/delayed RPCs recovery.
    First 2 attempts encounter network drops (return None).
    3rd attempt succeeds.
    Leader's controlled retry loop recovers and successfully replicates.
    """
    dir1 = tempfile.mkdtemp()
    try:
        node1 = RaftNode(node_id="node-1", base_dir=dir1)
        await node1.initialize()
        node1.role = NodeRole.LEADER
        node1.persistent_state.current_term = 1

        peer_addr = "peer:50052"
        node1.leader_state = LeaderVolatileState(
            next_index={peer_addr: 1},
            match_index={peer_addr: 0}
        )

        pool = RaftPeerClientPool(peers=[peer_addr])
        eng = RaftElectionEngine(node=node1, peer_pool=pool)

        cmd = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="f-drop", owner_id="u1", filename="doc.txt")
        await node1.append_log_entry(cmd)

        attempt = 0
        async def flappy_send_ae(req, timeout=1.0):
            nonlocal attempt
            attempt += 1
            if attempt < 3:
                return None  # Dropped packet
            return raft_pb2.AppendEntriesResponse(term=1, success=True, match_index=1)

        pool.get_client(peer_addr).send_append_entries = flappy_send_ae

        # Replicate with retry
        await eng._replicate_to_peer(peer_addr, target_index=1, max_retries=4)

        assert attempt == 3
        assert node1.leader_state.match_index[peer_addr] == 1
        assert node1.leader_state.next_index[peer_addr] == 2
        assert node1.volatile_state.commit_index == 1

        await pool.close_all()
    finally:
        shutil.rmtree(dir1, ignore_errors=True)
