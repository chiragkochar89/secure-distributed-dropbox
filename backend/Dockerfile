FROM python:3.13-slim

WORKDIR /app

# Install build essentials for dependencies if required
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY backend/app/ ./backend/app/
COPY backend/app/ ./app/

# Install dependencies via pip
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

COPY proto/ /proto/

# Generate gRPC Python modules from proto
RUN python -m grpc_tools.protoc \
    -I/proto \
    --python_out=./backend/app/proto \
    --grpc_python_out=./backend/app/proto \
    /proto/raft.proto

RUN python -m grpc_tools.protoc \
    -I/proto \
    --python_out=./app/proto \
    --grpc_python_out=./app/proto \
    /proto/raft.proto

EXPOSE 8001 50051

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${HTTP_PORT:-8001}"]

