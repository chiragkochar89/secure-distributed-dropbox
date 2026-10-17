$env:PYTHONPATH = "backend"
$env:NODE_ID = "node-3"
$env:HTTP_PORT = "8003"
$env:GRPC_PORT = "50053"
$env:RAFT_PEERS = "127.0.0.1:50051,127.0.0.1:50052"
$env:DATABASE_URL = "sqlite:///./dropbox.db"
$env:MINIO_ENDPOINT = "localhost:9000"
$env:PHASE = "phase-7"

Write-Host "Starting Secure Raft Dropbox Node 3 on HTTP 8003 / gRPC 50053..." -ForegroundColor Green
python -m uvicorn app.main:app --host 127.0.0.1 --port 8003 --reload
