import time
import logging
from typing import Dict, List, Tuple, Optional
import grpc

from app.config import settings
from app.proto import raft_pb2, raft_pb2_grpc

logger = logging.getLogger("secure-raft-dropbox.grpc-client")


class RaftPeerClient:
    """
    Manages persistent async gRPC channel and stub to a single peer node.
    Channels are persistent and automatically reconnect when offline peers become available.
    """

    def __init__(self, peer_address: str):
        self.peer_address = peer_address
        self.channel = grpc.aio.insecure_channel(peer_address)
        self.stub = raft_pb2_grpc.RaftConsensusStub(self.channel)

    async def send_request_vote(
        self, request: raft_pb2.RequestVoteRequest, timeout: float = 1.0
    ) -> Optional[raft_pb2.RequestVoteResponse]:
        """Send RequestVote RPC to peer with configurable timeout."""
        start_time = time.time()
        try:
            response = await self.stub.RequestVote(request, timeout=timeout)
            duration_ms = (time.time() - start_time) * 1000
            logger.info(
                f"RPC SENT [RequestVote] Node: {settings.NODE_ID} -> Peer: {self.peer_address} | "
                f"Status: OK | Duration: {duration_ms:.2f}ms"
            )
            return response
        except grpc.aio.AioRpcError as e:
            duration_ms = (time.time() - start_time) * 1000
            logger.warning(
                f"RPC FAIL [RequestVote] Node: {settings.NODE_ID} -> Peer: {self.peer_address} | "
                f"gRPC Status: {e.code().name} | Duration: {duration_ms:.2f}ms"
            )
            return None

    async def send_append_entries(
        self, request: raft_pb2.AppendEntriesRequest, timeout: float = 1.0
    ) -> Optional[raft_pb2.AppendEntriesResponse]:
        """Send AppendEntries RPC to peer with configurable timeout."""
        start_time = time.time()
        try:
            response = await self.stub.AppendEntries(request, timeout=timeout)
            duration_ms = (time.time() - start_time) * 1000
            logger.info(
                f"RPC SENT [AppendEntries] Node: {settings.NODE_ID} -> Peer: {self.peer_address} | "
                f"Status: OK | Duration: {duration_ms:.2f}ms"
            )
            return response
        except grpc.aio.AioRpcError as e:
            duration_ms = (time.time() - start_time) * 1000
            logger.warning(
                f"RPC FAIL [AppendEntries] Node: {settings.NODE_ID} -> Peer: {self.peer_address} | "
                f"gRPC Status: {e.code().name} | Duration: {duration_ms:.2f}ms"
            )
            return None

    async def send_install_snapshot(
        self, request: raft_pb2.InstallSnapshotRequest, timeout: float = 5.0
    ) -> Optional[raft_pb2.InstallSnapshotResponse]:
        """Send unary InstallSnapshot RPC chunk to peer with configurable timeout."""
        start_time = time.time()
        try:
            response = await self.stub.InstallSnapshot(request, timeout=timeout)
            duration_ms = (time.time() - start_time) * 1000
            logger.info(
                f"RPC SENT [InstallSnapshot] Node: {settings.NODE_ID} -> Peer: {self.peer_address} | "
                f"Offset: {request.offset} | Done: {request.done} | Duration: {duration_ms:.2f}ms"
            )
            return response
        except grpc.aio.AioRpcError as e:
            duration_ms = (time.time() - start_time) * 1000
            logger.warning(
                f"RPC FAIL [InstallSnapshot] Node: {settings.NODE_ID} -> Peer: {self.peer_address} | "
                f"gRPC Status: {e.code().name} | Duration: {duration_ms:.2f}ms"
            )
            return None

    async def ping(self, timeout: float = 1.0) -> Tuple[bool, float, Optional[str]]:
        """
        Send Ping RPC to check transport reachability and measure round-trip latency.
        Returns (is_online, latency_ms, last_error_message).
        """
        start_time = time.time()
        req = raft_pb2.PingRequest(
            sender_id=settings.NODE_ID,
            timestamp_ms=int(start_time * 1000)
        )
        try:
            res = await self.stub.Ping(req, timeout=timeout)
            duration_ms = (time.time() - start_time) * 1000
            return True, round(duration_ms, 2), None
        except grpc.aio.AioRpcError as e:
            duration_ms = (time.time() - start_time) * 1000
            return False, round(duration_ms, 2), e.code().name

    async def close(self):
        """Close persistent gRPC channel."""
        await self.channel.close()


class RaftPeerClientPool:
    """Connection pool managing persistent gRPC client channels for all configured peers."""

    def __init__(self, peers: Optional[List[str]] = None):
        self._peers_override = peers
        self.clients: Dict[str, RaftPeerClient] = {}

    @property
    def peer_addresses(self) -> List[str]:
        if self._peers_override is not None:
            return self._peers_override
        return settings.peer_list

    def get_client(self, peer_address: str) -> Optional[RaftPeerClient]:
        """Get persistent client for a given peer address."""
        if peer_address not in self.clients:
            self.clients[peer_address] = RaftPeerClient(peer_address)
        return self.clients[peer_address]

    async def ping_all_peers(self, timeout: float = 1.0) -> List[Dict]:
        """Ping all configured peers asynchronously and return transport diagnostic states."""
        results = []
        for addr, client in self.clients.items():
            is_online, latency_ms, error = await client.ping(timeout=timeout)
            results.append({
                "peer_address": addr,
                "status": "ONLINE" if is_online else "OFFLINE",
                "latency_ms": latency_ms if is_online else None,
                "error": error
            })
        return results

    async def close_all(self):
        """Close all peer channels during application shutdown."""
        for client in self.clients.values():
            await client.close()
        logger.info("Closed all peer gRPC client channels.")


peer_client_pool = RaftPeerClientPool()
