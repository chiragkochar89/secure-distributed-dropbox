@echo off
set PYTHONPATH=backend
set NODE_ID=node-2
set HTTP_PORT=8002
set GRPC_PORT=50052
set RAFT_PEERS=127.0.0.1:50051,127.0.0.1:50053
set DATABASE_URL=sqlite:///./dropbox.db
set MINIO_ENDPOINT=localhost:9000
set PHASE=phase-7

echo Starting Secure Raft Dropbox Node 2 on HTTP 8002 / gRPC 50052...
.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8002 --reload
