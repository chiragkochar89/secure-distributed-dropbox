import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.core.crypto import compute_sha256, sanitize_filename
from app.config import settings
from app.raft.node import raft_node_instance, NodeRole
from app.raft.state import LeaderVolatileState
from app.raft.rpc_client import RaftPeerClientPool, peer_client_pool
from app.raft.engine import raft_election_engine


@pytest.fixture(autouse=True)
def setup_leader_for_phase1_tests():
    settings.RAFT_PEERS = ""
    raft_node_instance.role = NodeRole.LEADER
    raft_node_instance.leader_id = raft_node_instance.node_id
    raft_node_instance.leader_state = LeaderVolatileState(next_index={}, match_index={})


@pytest.mark.asyncio
async def test_file_upload_download_rename_delete():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        # Register and login User A
        await client.post("/api/v1/auth/register", json={
            "email": "usera@example.com", "username": "usera", "password": "Password123!"
        })
        login_res = await client.post("/api/v1/auth/login", json={
            "username_or_email": "usera", "password": "Password123!"
        })
        token = login_res.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        # 1. Upload File
        file_content = b"Hello, Secure Distributed Dropbox Phase 1!"
        expected_hash = compute_sha256(file_content)

        files = {"file": ("test_file.txt", file_content, "text/plain")}
        upload_res = await client.post("/api/v1/files", files=files, headers=headers)
        assert upload_res.status_code == 201
        file_data = upload_res.json()
        file_id = file_data["id"]
        assert file_data["current_sha256"] == expected_hash
        assert file_data["sanitized_name"] == "test_file.txt"

        # 2. List Files
        list_res = await client.get("/api/v1/files", headers=headers)
        assert list_res.status_code == 200
        assert len(list_res.json()) >= 1

        # 3. Download File
        download_res = await client.get(f"/api/v1/files/{file_id}/download", headers=headers)
        assert download_res.status_code == 200
        assert download_res.content == file_content

        # 4. Rename File
        rename_res = await client.patch(
            f"/api/v1/files/{file_id}",
            json={"new_filename": "renamed_doc.txt"},
            headers=headers
        )
        assert rename_res.status_code == 200
        assert rename_res.json()["sanitized_name"] == "renamed_doc.txt"

        # 5. Delete File
        del_res = await client.delete(f"/api/v1/files/{file_id}", headers=headers)
        assert del_res.status_code == 200


@pytest.mark.asyncio
async def test_server_side_ownership_enforcement():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        # Register User A
        await client.post("/api/v1/auth/register", json={
            "email": "owner@example.com", "username": "owner", "password": "Password123!"
        })
        res_a = await client.post("/api/v1/auth/login", json={"username_or_email": "owner", "password": "Password123!"})
        headers_a = {"Authorization": f"Bearer {res_a.json()['access_token']}"}

        # Upload file by User A
        upload_res = await client.post(
            "/api/v1/files",
            files={"file": ("private.txt", b"Private Data", "text/plain")},
            headers=headers_a
        )
        file_id = upload_res.json()["id"]

        # Register User B
        await client.post("/api/v1/auth/register", json={
            "email": "attacker@example.com", "username": "attacker", "password": "Password123!"
        })
        res_b = await client.post("/api/v1/auth/login", json={"username_or_email": "attacker", "password": "Password123!"})
        headers_b = {"Authorization": f"Bearer {res_b.json()['access_token']}"}

        # User B attempts to access / download User A's file -> Expect 403 Forbidden
        download_res = await client.get(f"/api/v1/files/{file_id}/download", headers=headers_b)
        assert download_res.status_code == 403


def test_path_traversal_sanitization():
    unsafe_name = "../../../etc/passwd"
    sanitized = sanitize_filename(unsafe_name)
    assert sanitized == "passwd"
    assert ".." not in sanitized
    assert "/" not in sanitized
