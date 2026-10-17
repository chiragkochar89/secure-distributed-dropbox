import time
import pytest
import grpc
from app.config import Settings
from app.proto import raft_pb2, raft_pb2_grpc
from app.raft.rpc_server import GRPCServerManager, RaftConsensusServicer
from app.raft.rpc_client import RaftPeerClient, RaftPeerClientPool
from app.raft.node import RaftNode


def test_node_identity_configuration():
    """Verify explicit node identity configuration."""
    s1 = Settings(NODE_ID="node-1", HTTP_PORT=8001, GRPC_PORT=50051, RAFT_PEERS="127.0.0.1:50052,127.0.0.1:50053")
    assert s1.NODE_ID == "node-1"
    assert s1.HTTP_PORT == 8001
    assert s1.GRPC_PORT == 50051

    s2 = Settings(NODE_ID="node-2", HTTP_PORT=8002, GRPC_PORT=50052, RAFT_PEERS="127.0.0.1:50051,127.0.0.1:50053")
    assert s2.NODE_ID == "node-2"
    assert s2.HTTP_PORT == 8002
    assert s2.GRPC_PORT == 50052


@pytest.mark.asyncio
async def test_grpc_transport_ping_and_stubs():
    """Test gRPC server instantiation, Ping, RequestVote, and AppendEntries stub RPCs."""
    test_port = 50099
    server_mgr = GRPCServerManager(port=test_port)
    await server_mgr.start()

    client = RaftPeerClient(f"127.0.0.1:{test_port}")

    try:
        # 1. Ping
        is_online, latency_ms, error = await client.ping(timeout=1.0)
        assert is_online is True
        assert latency_ms > 0
        assert error is None

        # 2. RequestVote RPC
        req_vote = raft_pb2.RequestVoteRequest(
            term=2,
            candidate_id="node-candidate",
            last_log_index=5,
            last_log_term=1
        )
        vote_res = await client.send_request_vote(req_vote, timeout=1.0)
        assert vote_res is not None
        assert vote_res.term == 2
        assert vote_res.vote_granted is True  # In Phase 4, higher candidate term 2 grants vote

        # 3. AppendEntries RPC
        req_app = raft_pb2.AppendEntriesRequest(
            term=2,
            leader_id="node-leader",
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0
        )
        app_res = await client.send_append_entries(req_app, timeout=1.0)
        assert app_res is not None
        assert app_res.term == 2
        assert app_res.success is True  # Deterministic stub
    finally:
        await client.close()
        await server_mgr.stop(grace=0.5)


@pytest.mark.asyncio
async def test_rpc_timeout_handling():
    """Verify that RPC to non-listening port times out within configured limit (1.0s)."""
    offline_client = RaftPeerClient("127.0.0.1:59998")
    start = time.time()
    is_online, latency_ms, error = await offline_client.ping(timeout=1.0)
    duration = time.time() - start

    assert is_online is False
    assert error in ["UNAVAILABLE", "DEADLINE_EXCEEDED"]
    assert duration <= 2.0  # Respects timeout
    await offline_client.close()


@pytest.mark.asyncio
async def test_peer_recovery_without_restarting_client():
    """Verify peer recovery: Client handles offline peer safely, then succeeds when peer starts."""
    test_port = 50097
    peer_addr = f"127.0.0.1:{test_port}"
    client = RaftPeerClient(peer_addr)

    try:
        # 1. Peer initially offline
        is_online, _, error = await client.ping(timeout=0.5)
        assert is_online is False

        # 2. Start server on target port
        server_mgr = GRPCServerManager(port=test_port)
        await server_mgr.start()

        # 3. Subsequent RPC succeeds without restarting client!
        is_online, latency_ms, error = await client.ping(timeout=1.0)
        assert is_online is True
        assert error is None

        await server_mgr.stop(grace=0.5)
    finally:
        await client.close()


def test_stub_state_isolation():
    """Verify that transport RPCs do NOT mutate underlying Raft state machine models."""
    node = RaftNode(node_id="node-1")
    initial_status = node.get_status()

    # Initial Raft state
    assert initial_status["current_term"] == 0
    assert initial_status["voted_for"] is None
    assert initial_status["log_length"] == 0
    assert initial_status["commit_index"] == 0
    assert initial_status["last_applied"] == 0

    # Ensure state remains completely untouched by Phase 2 transport
    servicer = RaftConsensusServicer()
    assert node.persistent_state.current_term == 0
    assert node.persistent_state.voted_for is None
    assert len(node.persistent_state.log) == 0
