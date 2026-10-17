import sys
from pathlib import Path

# Add app/proto directory to sys.path so protoc generated imports work seamlessly
_proto_dir = str(Path(__file__).parent.resolve())
if _proto_dir not in sys.path:
    sys.path.insert(0, _proto_dir)

from app.proto import raft_pb2, raft_pb2_grpc

__all__ = ["raft_pb2", "raft_pb2_grpc"]
