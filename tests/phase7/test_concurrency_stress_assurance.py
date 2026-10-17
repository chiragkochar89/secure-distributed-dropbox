import os
import shutil
import tempfile
import uuid
import pytest
import asyncio

from app.raft.node import RaftNode, NodeRole
from app.raft.state import LeaderVolatileState, RaftLogEntry
from app.raft.engine import RaftElectionEngine
from app.raft.rpc_client import RaftPeerClientPool
from app.services.commands import FileOperationCommand, FileOperationType
from app.proto import raft_pb2


@pytest.mark.asyncio
async def test_concurrent_proposals_linearizability():
    """Phase 8 Test 1: Concurrent writes are serialized in strict monotonic log order."""
    dir1 = tempfile.mkdtemp()
    try:
        node = RaftNode(node_id="node-leader", base_dir=dir1)
        await node.initialize()
        node.role = NodeRole.LEADER
        node.persistent_state.current_term = 1

        # Simulate 10 concurrent client write proposals
        async def submit_write(i: int):
            cmd = FileOperationCommand(
                operation=FileOperationType.UPLOAD,
                file_id=f"concurrent-{i}",
                owner_id="u1",
                filename=f"doc_{i}.txt",
                sha256=f"hash_{i}",
                size_bytes=100
            )
            return await node.append_log_entry(cmd)

        tasks = [submit_write(i) for i in range(10)]
        entries = await asyncio.gather(*tasks)

        assert len(entries) == 10
        # Verify indices are unique and contiguous 1..10
        indices = [e.index for e in entries]
        assert sorted(indices) == list(range(1, 11))
        assert len(set(indices)) == 10
        assert node.last_log_index == 10

        # Verify disk persistence matches memory
        reloaded = RaftNode(node_id="node-leader", base_dir=dir1)
        await reloaded.initialize()
        assert reloaded.last_log_index == 10
        assert len(reloaded.persistent_state.log) == 10

    finally:
        shutil.rmtree(dir1, ignore_errors=True)


@pytest.mark.asyncio
async def test_concurrent_quorum_replication():
    """Phase 8 Test 2: Leader replicates multiple concurrent entries across quorum peers."""
    dir1 = tempfile.mkdtemp()
    try:
        node = RaftNode(node_id="node-leader", base_dir=dir1)
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

        # Mock peers successfully appending
        peer1_log = []
        peer2_log = []

        async def send_ae_p1(req, timeout=1.0):
            for ep in req.entries:
                peer1_log.append(ep)
            return raft_pb2.AppendEntriesResponse(term=1, success=True, match_index=len(peer1_log))

        async def send_ae_p2(req, timeout=1.0):
            for ep in req.entries:
                peer2_log.append(ep)
            return raft_pb2.AppendEntriesResponse(term=1, success=True, match_index=len(peer2_log))

        pool.get_client(p1).send_append_entries = send_ae_p1
        pool.get_client(p2).send_append_entries = send_ae_p2

        # Append 5 entries
        for i in range(1, 6):
            cmd = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id=f"f-{i}", owner_id="u1", filename=f"f{i}.txt")
            await node.append_log_entry(cmd)

        # Await replication to target index 5
        committed = await eng.replicate_to_peers_and_await_quorum(target_index=5, timeout=2.0)
        assert committed is True
        assert node.volatile_state.commit_index == 5
        assert len(peer1_log) == 5
        assert len(peer2_log) == 5

        await pool.close_all()
    finally:
        shutil.rmtree(dir1, ignore_errors=True)
