import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.db.session import init_db
from app.api.health import router as health_router
from app.api.auth import router as auth_router
from app.api.files import router as file_router
from app.api.audit import router as audit_router
from app.api.raft import router as raft_router
from app.raft.rpc_server import grpc_server_manager
from app.raft.rpc_client import peer_client_pool
from app.raft.node import raft_node_instance
from app.raft.engine import raft_election_engine

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] [%(name)s] [%(filename)s:%(lineno)d] - %(message)s",
)
logger = logging.getLogger("secure-raft-dropbox")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup hook: initialize DB schema, storage, Raft node persistent state, election engine, & async gRPC server
    logger.info(
        f"Starting Node [{settings.NODE_ID}] on HTTP port {settings.HTTP_PORT}, "
        f"gRPC port {settings.GRPC_PORT}, Phase: {settings.PHASE}"
    )
    init_db()
    await raft_node_instance.initialize()
    await grpc_server_manager.start()
    await raft_election_engine.start()
    yield
    # Shutdown hook: stop election engine, gRPC server & close client pool channels
    logger.info(f"Shutting down Node [{settings.NODE_ID}]...")
    await raft_election_engine.stop()
    await grpc_server_manager.stop(grace=2.0)
    await peer_client_pool.close_all()


app = FastAPI(
    title=f"Secure Raft Dropbox Node - {settings.NODE_ID}",
    description="Secure Distributed Dropbox using custom Raft consensus algorithm",
    version="0.1.0",
    lifespan=lifespan,
)

# Configure CORS for React frontend dashboard (supporting credentials for HttpOnly Cookies across all local ports)
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"http://(localhost|127\.0\.0\.1)(:\d+)?",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from prometheus_client import make_asgi_app

# Register API Routers
app.include_router(health_router)
app.include_router(auth_router)
app.include_router(file_router)
app.include_router(audit_router)
app.include_router(raft_router)

# Mount Prometheus metrics endpoint for cluster scraping
app.mount("/metrics", make_asgi_app())


@app.get("/")
async def root():
    return {
        "message": "Secure Distributed Dropbox API Node",
        "node_id": settings.NODE_ID,
        "phase": settings.PHASE,
        "docs": "/docs",
    }
