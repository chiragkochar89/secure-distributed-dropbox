$env:PYTHONPATH = "backend"
$env:NODE_ID = "node-2"
$env:HTTP_PORT = "8002"
$env:GRPC_PORT = "50052"
$env:RAFT_PEERS = "127.0.0.1:50051,127.0.0.1:50053"
$env:DATABASE_URL = "sqlite:///./dropbox.db"
$env:MINIO_ENDPOINT = "localhost:9000"
$env:PHASE = "phase-7"

Write-Host "Starting Secure Raft Dropbox Node 2 on HTTP 8002 / gRPC 50052..." -ForegroundColor Green
python -m uvicorn app.main:app --host 127.0.0.1 --port 8002 --reload
