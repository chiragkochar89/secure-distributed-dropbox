import os
import pytest
from app.config import Settings


def test_default_config_loading():
    settings = Settings()
    assert settings.NODE_ID == "node-1"
    assert settings.HTTP_PORT == 8001
    assert settings.GRPC_PORT == 50051
    assert "node-2:50052" in settings.peer_list
    assert "node-3:50053" in settings.peer_list


def test_custom_node_identities():
    # Node 1
    s1 = Settings(NODE_ID="node-1", HTTP_PORT=8001, GRPC_PORT=50051, RAFT_PEERS="127.0.0.1:50052,127.0.0.1:50053")
    assert s1.NODE_ID == "node-1"
    assert s1.peer_list == ["127.0.0.1:50052", "127.0.0.1:50053"]

    # Node 2
    s2 = Settings(NODE_ID="node-2", HTTP_PORT=8002, GRPC_PORT=50052, RAFT_PEERS="127.0.0.1:50051,127.0.0.1:50053")
    assert s2.NODE_ID == "node-2"
    assert s2.HTTP_PORT == 8002
    assert s2.peer_list == ["127.0.0.1:50051", "127.0.0.1:50053"]

    # Node 3
    s3 = Settings(NODE_ID="node-3", HTTP_PORT=8003, GRPC_PORT=50053, RAFT_PEERS="127.0.0.1:50051,127.0.0.1:50052")
    assert s3.NODE_ID == "node-3"
    assert s3.HTTP_PORT == 8003
    assert s3.peer_list == ["127.0.0.1:50051", "127.0.0.1:50052"]
