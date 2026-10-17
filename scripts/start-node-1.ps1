$env:PYTHONPATH = "backend"
$env:NODE_ID = "node-1"
$env:HTTP_PORT = "8001"
$env:GRPC_PORT = "50051"
$env:RAFT_PEERS = "127.0.0.1:50052,127.0.0.1:50053"
$env:DATABASE_URL = "sqlite:///./dropbox.db"
$env:MINIO_ENDPOINT = "localhost:9000"
$env:PHASE = "phase-7"

Write-Host "Starting Secure Raft Dropbox Node 1 on HTTP 8001 / gRPC 50051..." -ForegroundColor Green
python -m uvicorn app.main:app --host 127.0.0.1 --port 8001 --reload
