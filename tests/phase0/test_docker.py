from pathlib import Path


def test_docker_compose_validity():
    docker_path = Path(__file__).parents[2] / "docker-compose.yml"
    assert docker_path.exists(), "docker-compose.yml must exist"

    content = docker_path.read_text(encoding="utf-8")
    assert "node-1:" in content
    assert "node-2:" in content
    assert "node-3:" in content
    assert "postgres:" in content
    assert "minio:" in content
    assert "prometheus:" in content
    assert "grafana:" in content

    # Port checks
    assert "8001:8001" in content
    assert "50051:50051" in content
    assert "8002:8002" in content
    assert "50052:50052" in content
    assert "8003:8003" in content
    assert "50053:50053" in content
