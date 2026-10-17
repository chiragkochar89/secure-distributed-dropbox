import os
import re
import pytest
from app.config import settings
from app.storage.replication_service import ObjectReplicationService


def test_config_phase_default_is_phase7():
    """Verify that backend/app/config.py default PHASE is phase-7."""
    assert settings.PHASE == "phase-7"


def test_docker_compose_topology_contract():
    """Verify that docker-compose.yml strictly enforces per-node independence."""
    compose_path = os.path.join(os.getcwd(), "docker-compose.yml")
    assert os.path.exists(compose_path), "docker-compose.yml not found"

    with open(compose_path, "r", encoding="utf-8") as f:
        content = f.read()

    # 1. Independent PostgreSQL services
    assert "postgres-1:" in content
    assert "postgres-2:" in content
    assert "postgres-3:" in content

    # 2. Independent MinIO services
    assert "minio-1:" in content
    assert "minio-2:" in content
    assert "minio-3:" in content

    # 3. Independent Volumes
    for v in [
        "postgres_data_1", "postgres_data_2", "postgres_data_3",
        "minio_data_1", "minio_data_2", "minio_data_3"
    ]:
        assert v in content, f"Volume {v} missing in docker-compose.yml"

    # 4. Database URLs per node
    assert "postgres-1:5432/dropbox_db_1" in content
    assert "postgres-2:5432/dropbox_db_2" in content
    assert "postgres-3:5432/dropbox_db_3" in content

    # 5. MinIO endpoints per node
    assert "MINIO_ENDPOINT=minio-1:9000" in content
    assert "MINIO_ENDPOINT=minio-2:9000" in content
    assert "MINIO_ENDPOINT=minio-3:9000" in content

    # 6. MinIO buckets per node
    assert "MINIO_BUCKET=node-1-bucket" in content
    assert "MINIO_BUCKET=node-2-bucket" in content
    assert "MINIO_BUCKET=node-3-bucket" in content

    # 7. MinIO peer endpoints per node
    assert "MINIO_PEER_ENDPOINTS=node-2=minio-2:9000,node-3=minio-3:9000" in content
    assert "MINIO_PEER_ENDPOINTS=node-1=minio-1:9000,node-3=minio-3:9000" in content
    assert "MINIO_PEER_ENDPOINTS=node-1=minio-1:9000,node-2=minio-2:9000" in content

    # 8. Phase 7 setting
    assert content.count("PHASE=phase-7") == 3


def test_peer_endpoint_parsing():
    """Verify ObjectReplicationService correctly resolves peer endpoints."""
    original = settings.MINIO_PEER_ENDPOINTS
    try:
        settings.MINIO_PEER_ENDPOINTS = "node-2=minio-2:9000,node-3=minio-3:9000"
        service = ObjectReplicationService()
        endpoints = service.peer_endpoints
        assert endpoints == {
            "node-2": "minio-2:9000",
            "node-3": "minio-3:9000"
        }
    finally:
        settings.MINIO_PEER_ENDPOINTS = original
