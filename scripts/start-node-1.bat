@echo off
set PYTHONPATH=backend
set NODE_ID=node-1
set HTTP_PORT=8001
set GRPC_PORT=50051
set RAFT_PEERS=127.0.0.1:50052,127.0.0.1:50053
set DATABASE_URL=sqlite:///./dropbox.db
set MINIO_ENDPOINT=localhost:9000
set PHASE=phase-7

echo Starting Secure Raft Dropbox Node 1 on HTTP 8001 / gRPC 50051...
.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8001 --reload
