import uuid
import pytest
import httpx
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.config import settings
from app.db.session import SessionLocal, init_db
from app.db.models.user import User, UserRole
from app.core.crypto import compute_sha256
from app.raft.node import raft_node_instance, NodeRole
from app.raft.state import LeaderVolatileState
from app.storage.minio_client import storage_service


@pytest.fixture(autouse=True)
def setup_e2e_env():
    init_db()
    settings.RAFT_PEERS = ""
    raft_node_instance.role = NodeRole.LEADER
    raft_node_instance.leader_id = raft_node_instance.node_id
    raft_node_instance.leader_state = LeaderVolatileState(next_index={}, match_index={})
    storage_service.use_local_fallback = True


@pytest.mark.asyncio
async def test_full_browser_user_journey_e2e():
    """Phase 7 Test 1: Complete end-to-end user workflow matching frontend actions."""
    username = f"user_{uuid.uuid4().hex[:6]}"
    email = f"{username}@example.com"
    password = "E2E_SecurePassword_2026!"

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://localhost:8001") as client:
        # 1. Registration
        reg_payload = {"email": email, "username": username, "password": password}
        reg_res = await client.post("/api/v1/auth/register", json=reg_payload)
        assert reg_res.status_code == 201
        user_info = reg_res.json()
        assert user_info["username"] == username
        assert user_info["email"] == email

        # 2. Login
        login_payload = {"username_or_email": username, "password": password}
        login_res = await client.post("/api/v1/auth/login", json=login_payload)
        assert login_res.status_code == 200
        token_data = login_res.json()
        assert "access_token" in token_data
        token = token_data["access_token"]
        auth_headers = {"Authorization": f"Bearer {token}"}

        # 3. Initial File Listing (empty)
        list_res = await client.get("/api/v1/files", headers=auth_headers)
        assert list_res.status_code == 200
        assert len(list_res.json()) == 0

        # 4. Upload File
        file_content = b"CRITICAL_END_TO_END_BROWSER_WORKFLOW_TEST_DOCUMENT_2026"
        expected_sha256 = compute_sha256(file_content)
        upload_files = {"file": ("report_v1.txt", file_content, "text/plain")}
        
        upload_res = await client.post("/api/v1/files", files=upload_files, headers=auth_headers)
        assert upload_res.status_code == 201
        file_meta = upload_res.json()
        file_id = file_meta["id"]
        assert file_meta["original_filename"] == "report_v1.txt"
        assert file_meta["sanitized_name"] == "report_v1.txt"
        assert file_meta["size_bytes"] == len(file_content)
        assert file_meta["current_sha256"] == expected_sha256
        assert file_meta["current_version"] == 1

        # 5. File Listing (contains newly uploaded file)
        list_res2 = await client.get("/api/v1/files", headers=auth_headers)
        assert list_res2.status_code == 200
        files_list = list_res2.json()
        assert len(files_list) == 1
        assert files_list[0]["id"] == file_id

        # 6. Download File & SHA-256 Verification
        download_res = await client.get(f"/api/v1/files/{file_id}/download", headers=auth_headers)
        assert download_res.status_code == 200
        assert download_res.content == file_content
        assert compute_sha256(download_res.content) == expected_sha256

        # 7. Rename File
        rename_payload = {"new_filename": "final_audited_report.txt"}
        rename_res = await client.patch(f"/api/v1/files/{file_id}", json=rename_payload, headers=auth_headers)
        assert rename_res.status_code == 200
        renamed_meta = rename_res.json()
        assert renamed_meta["original_filename"] == "final_audited_report.txt"
        assert renamed_meta["sanitized_name"] == "final_audited_report.txt"

        # Verify rename reflected in listing
        list_res3 = await client.get("/api/v1/files", headers=auth_headers)
        assert list_res3.json()[0]["sanitized_name"] == "final_audited_report.txt"

        # 8. Delete File
        delete_res = await client.delete(f"/api/v1/files/{file_id}", headers=auth_headers)
        assert delete_res.status_code in [200, 204]

        # Verify file disappears from active listing
        list_res4 = await client.get("/api/v1/files", headers=auth_headers)
        assert len(list_res4.json()) == 0

        # Verify subsequent download returns 404
        dl_deleted = await client.get(f"/api/v1/files/{file_id}/download", headers=auth_headers)
        assert dl_deleted.status_code == 404


@pytest.mark.asyncio
async def test_client_failover_interception_not_leader():
    """Phase 7 Test 2: Verify follower intercepts write with NOT_LEADER (HTTP 503) and surfaces leader ID."""
    raft_node_instance.role = NodeRole.FOLLOWER
    raft_node_instance.leader_id = "node-1"

    db = SessionLocal()
    user = User(id="u-failover", email="failover@test.com", username="failover", password_hash="dummy", role=UserRole.USER.value)
    db.add(user)
    db.commit()
    db.close()

    from app.core.security import create_access_token
    token = create_access_token({"sub": "u-failover"})

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://localhost:8002") as client:
        # Follower intercepts POST upload
        files = {"file": ("test.txt", b"abc", "text/plain")}
        res = await client.post("/api/v1/files", files=files, headers={"Authorization": f"Bearer {token}"})
        assert res.status_code == 503
        detail = res.json().get("detail", {})
        assert detail.get("error") == "NOT_LEADER"
        assert detail.get("leader_id") == "node-1"

        # Follower intercepts PATCH rename
        res_ren = await client.patch("/api/v1/files/f123", json={"new_filename": "new.txt"}, headers={"Authorization": f"Bearer {token}"})
        assert res_ren.status_code == 503
        assert res_ren.json().get("detail", {}).get("error") == "NOT_LEADER"

        # Follower intercepts DELETE
        res_del = await client.delete("/api/v1/files/f123", headers={"Authorization": f"Bearer {token}"})
        assert res_del.status_code == 503
        assert res_del.json().get("detail", {}).get("error") == "NOT_LEADER"
