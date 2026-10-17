@echo off
set PYTHONPATH=backend
set NODE_ID=node-3
set HTTP_PORT=8003
set GRPC_PORT=50053
set RAFT_PEERS=127.0.0.1:50051,127.0.0.1:50052
set DATABASE_URL=sqlite:///./dropbox.db
set MINIO_ENDPOINT=localhost:9000
set PHASE=phase-7

echo Starting Secure Raft Dropbox Node 3 on HTTP 8003 / gRPC 50053...
.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8003 --reload
