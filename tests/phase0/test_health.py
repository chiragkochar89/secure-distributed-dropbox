import pytest
from httpx import AsyncClient, ASGITransport
from app.config import settings
from app.main import app


@pytest.mark.asyncio
async def test_health_endpoint():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "node_id" in data
        assert data["service"] == "secure-raft-dropbox"
        assert data["phase"] == settings.PHASE
        assert "http_port" in data
        assert "grpc_port" in data
        assert "peers" in data


@pytest.mark.asyncio
async def test_root_endpoint():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "node_id" in data
        assert data["phase"] == settings.PHASE
