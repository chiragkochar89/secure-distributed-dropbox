from fastapi import APIRouter
from pydantic import BaseModel
from typing import List
from app.config import settings

router = APIRouter(prefix="/api/v1", tags=["health"])


class HealthResponse(BaseModel):
    status: str
    node_id: str
    service: str
    phase: str
    http_port: int
    grpc_port: int
    peers: List[str]


@router.get("/health", response_model=HealthResponse)
async def get_health():
    return HealthResponse(
        status="healthy",
        node_id=settings.NODE_ID,
        service="secure-raft-dropbox",
        phase=settings.PHASE,
        http_port=settings.HTTP_PORT,
        grpc_port=settings.GRPC_PORT,
        peers=settings.peer_list,
    )
