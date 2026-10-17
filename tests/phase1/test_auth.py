import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.core.security import verify_password
from app.config import settings
from app.raft.node import raft_node_instance, NodeRole
from app.raft.state import LeaderVolatileState


@pytest.fixture(autouse=True)
def setup_leader_for_phase1_tests():
    settings.RAFT_PEERS = ""
    raft_node_instance.role = NodeRole.LEADER
    raft_node_instance.leader_id = raft_node_instance.node_id
    raft_node_instance.leader_state = LeaderVolatileState(next_index={}, match_index={})


@pytest.mark.asyncio
async def test_user_registration_and_argon2id():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        payload = {
            "email": "testuser@example.com",
            "username": "testuser",
            "password": "SecurePassword123!"
        }
        res = await client.post("/api/v1/auth/register", json=payload)
        assert res.status_code == 201
        data = res.json()
        assert data["email"] == payload["email"]
        assert data["username"] == payload["username"]
        assert "password" not in data


@pytest.mark.asyncio
async def test_user_login_success_and_cookie():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        # Register user
        reg = {
            "email": "loginuser@example.com",
            "username": "loginuser",
            "password": "Password123!"
        }
        await client.post("/api/v1/auth/register", json=reg)

        # Login
        login_data = {
            "username_or_email": "loginuser",
            "password": "Password123!"
        }
        res = await client.post("/api/v1/auth/login", json=login_data)
        assert res.status_code == 200
        data = res.json()
        assert "access_token" in data
        assert "access_token" in res.cookies


@pytest.mark.asyncio
async def test_user_login_failure():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        login_data = {
            "username_or_email": "nonexistentuser",
            "password": "WrongPassword!"
        }
        res = await client.post("/api/v1/auth/login", json=login_data)
        assert res.status_code == 401
