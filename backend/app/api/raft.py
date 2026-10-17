from typing import List, Optional
from fastapi import APIRouter
from pydantic import BaseModel

from app.config import settings
from app.raft.rpc_client import peer_client_pool
from app.raft.node import raft_node_instance
from app.raft.engine import raft_election_engine

router = APIRouter(prefix="/api/v1/raft", tags=["raft-cluster-diagnostics"])


class PeerDiagnostic(BaseModel):
    peer_address: str
    status: str
    latency_ms: Optional[float] = None
    error: Optional[str] = None


class ClusterDiagnosticResponse(BaseModel):
    node_id: str
    http_port: int
    grpc_port: int
    role: str
    current_term: int
    leader_id: Optional[str] = None
    voted_for: Optional[str] = None
    heartbeat_active: bool
    peers: List[str]
    peer_diagnostics: List[PeerDiagnostic]


@router.get("/cluster", response_model=ClusterDiagnosticResponse)
async def get_cluster_diagnostics():
    """
    Diagnostic transport endpoint returning node state, leader identity,
    active heartbeat loop status, and live peer reachability across configured cluster nodes.
    """
    diagnostics = await peer_client_pool.ping_all_peers(timeout=1.0)
    heartbeat_active = bool(raft_election_engine.heartbeat_task and not raft_election_engine.heartbeat_task.done())

    return ClusterDiagnosticResponse(
        node_id=raft_node_instance.node_id,
        http_port=settings.HTTP_PORT,
        grpc_port=settings.GRPC_PORT,
        role=raft_node_instance.role,
        current_term=raft_node_instance.persistent_state.current_term,
        leader_id=raft_node_instance.leader_id,
        voted_for=raft_node_instance.persistent_state.voted_for,
        heartbeat_active=heartbeat_active,
        peers=settings.peer_list,
        peer_diagnostics=[
            PeerDiagnostic(
                peer_address=d["peer_address"],
                status=d["status"],
                latency_ms=d["latency_ms"],
                error=d["error"]
            )
            for d in diagnostics
        ]
    )
