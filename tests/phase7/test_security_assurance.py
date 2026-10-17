import os
import uuid
import pytest
from datetime import timedelta
import jwt
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.config import settings
from app.db.session import SessionLocal, init_db
from app.db.models.user import User, UserRole
from app.db.models.file import FileMetadata, FileVersion
from app.core.security import hash_password, create_access_token, decode_access_token, ph
from app.core.crypto import sanitize_filename, compute_sha256
from app.raft.node import raft_node_instance, NodeRole
from app.raft.state import LeaderVolatileState
from app.storage.minio_client import storage_service


@pytest.fixture(autouse=True)
def setup_security_test_env():
    init_db()
    settings.RAFT_PEERS = ""
    raft_node_instance.role = NodeRole.LEADER
    raft_node_instance.leader_id = raft_node_instance.node_id
    raft_node_instance.leader_state = LeaderVolatileState(next_index={}, match_index={})
    storage_service.use_local_fallback = True


@pytest.mark.asyncio
async def test_invalid_credentials_rejected():
    """Phase 6 Test 1: Invalid username/password combination returns HTTP 401."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        # Register user
        reg = {"email": "sec1@test.com", "username": "sec1", "password": "CorrectPassword123!"}
        await client.post("/api/v1/auth/register", json=reg)

        # Attempt login with bad password
        res = await client.post("/api/v1/auth/login", json={"username_or_email": "sec1", "password": "WRONG_PASSWORD"})
        assert res.status_code == 401
        assert "Incorrect" in res.json().get("detail", "")


@pytest.mark.asyncio
async def test_jwt_tampered_and_expired_tokens_rejected():
    """Phase 6 Test 2: Tampered JWT signature and expired JWT return HTTP 401."""
    db = SessionLocal()
    user = User(
        id=str(uuid.uuid4()),
        email="jwtuser@test.com",
        username="jwtuser",
        password_hash=hash_password("Pass123!"),
        role=UserRole.USER.value
    )
    db.add(user)
    db.commit()
    user_id = user.id
    db.close()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        # 1. Valid Token works
        valid_token = create_access_token({"sub": user_id})
        res_valid = await client.get("/api/v1/files", headers={"Authorization": f"Bearer {valid_token}"})
        assert res_valid.status_code == 200

        # 2. Tampered signature
        tampered_token = valid_token[:-4] + "fake"
        res_tampered = await client.get("/api/v1/files", headers={"Authorization": f"Bearer {tampered_token}"})
        assert res_tampered.status_code == 401

        # 3. Expired token
        expired_token = create_access_token({"sub": user_id}, expires_delta=timedelta(seconds=-10))
        res_expired = await client.get("/api/v1/files", headers={"Authorization": f"Bearer {expired_token}"})
        assert res_expired.status_code == 401

        # 4. Token signed with wrong secret key
        wrong_secret_token = jwt.encode({"sub": user_id}, "WRONG_SECRET_KEY_12345678901234567890", algorithm="HS256")
        res_wrong_secret = await client.get("/api/v1/files", headers={"Authorization": f"Bearer {wrong_secret_token}"})
        assert res_wrong_secret.status_code == 401


@pytest.mark.asyncio
async def test_cross_user_file_isolation_enforced():
    """Phase 6 Test 3: User A is denied access (HTTP 403) when attempting to access User B's file."""
    db = SessionLocal()
    user_a = User(id="user-a", email="a@test.com", username="usera", password_hash=hash_password("P123!"), role=UserRole.USER.value)
    user_b = User(id="user-b", email="b@test.com", username="userb", password_hash=hash_password("P123!"), role=UserRole.USER.value)
    db.add(user_a)
    db.add(user_b)

    file_b_id = str(uuid.uuid4())
    obj_key = f"users/user-b/{file_b_id}/v1"
    file_b = FileMetadata(
        id=file_b_id,
        owner_id="user-b",
        original_filename="confidential.pdf",
        sanitized_name="confidential.pdf",
        content_type="application/pdf",
        size_bytes=100,
        current_sha256="abc",
        current_version=1,
        is_deleted=False
    )
    ver_b = FileVersion(
        id=str(uuid.uuid4()),
        file_id=file_b_id,
        version_number=1,
        object_key=obj_key,
        size_bytes=100,
        sha256_hash="abc"
    )
    db.add(file_b)
    db.add(ver_b)
    db.commit()
    db.close()

    storage_service.upload_object(obj_key, b"TOP_SECRET_BYTES_FOR_USER_B")

    token_a = create_access_token({"sub": "user-a"})

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        # User A attempts to download User B's file -> 403 Forbidden
        res_dl = await client.get(f"/api/v1/files/{file_b_id}/download", headers={"Authorization": f"Bearer {token_a}"})
        assert res_dl.status_code == 403
        assert "Access to file denied" in res_dl.json().get("detail", "")

        # User A attempts to rename User B's file -> 403 Forbidden
        res_ren = await client.patch(f"/api/v1/files/{file_b_id}", json={"new_filename": "hacked.pdf"}, headers={"Authorization": f"Bearer {token_a}"})
        assert res_ren.status_code == 403

        # User A attempts to delete User B's file -> 403 Forbidden
        res_del = await client.delete(f"/api/v1/files/{file_b_id}", headers={"Authorization": f"Bearer {token_a}"})
        assert res_del.status_code == 403


@pytest.mark.asyncio
async def test_admin_cross_user_file_access_allowed():
    """Phase 6 Test 4: Admin role has privileged cross-user access to files."""
    db = SessionLocal()
    admin = User(id="admin-1", email="admin@test.com", username="admin", password_hash=hash_password("AdminPass1!"), role=UserRole.ADMIN.value)
    user_x = User(id="user-x", email="x@test.com", username="userx", password_hash=hash_password("Pass1!"), role=UserRole.USER.value)
    db.add(admin)
    db.add(user_x)

    file_x_id = str(uuid.uuid4())
    obj_key = f"users/user-x/{file_x_id}/v1"
    file_x = FileMetadata(
        id=file_x_id,
        owner_id="user-x",
        original_filename="user_doc.txt",
        sanitized_name="user_doc.txt",
        content_type="text/plain",
        size_bytes=24,
        current_sha256="hash_x",
        current_version=1,
        is_deleted=False
    )
    ver_x = FileVersion(
        id=str(uuid.uuid4()),
        file_id=file_x_id,
        version_number=1,
        object_key=obj_key,
        size_bytes=24,
        sha256_hash="hash_x"
    )
    db.add(file_x)
    db.add(ver_x)
    db.commit()
    db.close()

    storage_service.upload_object(obj_key, b"Document content user x")
    admin_token = create_access_token({"sub": "admin-1"})

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        # Admin downloads User X's file -> 200 OK
        res = await client.get(f"/api/v1/files/{file_x_id}/download", headers={"Authorization": f"Bearer {admin_token}"})
        assert res.status_code == 200
        assert res.content == b"Document content user x"


def test_path_traversal_filename_sanitization():
    """Phase 6 Test 5: Filename sanitization strips directory traversal sequences."""
    test_cases = [
        ("../../etc/passwd", "passwd"),
        ("../../../Windows/System32/cmd.exe", "cmd.exe"),
        ("..\\..\\boot.ini", "boot.ini"),
        ("file<>:\"?*name.txt", "file______name.txt"),
        (".../....//safe.docx", "safe.docx"),
        ("", "unnamed_file"),
        ("   .hidden.txt   ", "hidden.txt"),
    ]
    for raw, expected in test_cases:
        assert sanitize_filename(raw) == expected


@pytest.mark.asyncio
async def test_oversized_upload_rejected():
    """Phase 6 Test 6: File upload exceeding MAX_UPLOAD_SIZE_BYTES returns HTTP 413."""
    db = SessionLocal()
    user = User(id="u-size", email="size@test.com", username="sizeuser", password_hash=hash_password("Pass1!"), role=UserRole.USER.value)
    db.add(user)
    db.commit()
    token = create_access_token({"sub": "u-size"})
    db.close()

    # Create oversized payload: MAX_UPLOAD_SIZE_BYTES + 10 bytes
    oversized_data = b"X" * (settings.MAX_UPLOAD_SIZE_BYTES + 10)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        files = {"file": ("bigfile.bin", oversized_data, "application/octet-stream")}
        res = await client.post("/api/v1/files", files=files, headers={"Authorization": f"Bearer {token}"})
        assert res.status_code == 413
        assert "exceeds maximum limit" in res.json().get("detail", "")


def test_argon2id_cryptographic_security():
    """Phase 6 Test 7: Verify Argon2id password hashing parameters and unique salt generation."""
    pwd = "HighEntropySecretPassword2026!"
    hash1 = hash_password(pwd)
    hash2 = hash_password(pwd)

    # Hashes must differ because of CSPRNG unique salts
    assert hash1 != hash2
    assert hash1.startswith("$argon2id$")
    assert hash2.startswith("$argon2id$")

    # Verify check parameters
    assert ph.check_needs_rehash(hash1) is False
