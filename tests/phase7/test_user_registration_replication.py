import os
import shutil
import uuid
import pytest
import asyncio
from unittest.mock import patch, AsyncMock

from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.main import app
from app.db.session import SessionLocal, init_db
from app.db.models.user import User, UserRole
from app.db.models.audit import AuditLog
from app.proto import raft_pb2
from app.services.commands import FileOperationCommand, FileOperationType
from app.raft.node import RaftNode, NodeRole, raft_node_instance
from app.raft.state import RaftLogEntry
from app.raft.state_machine import state_machine_executor
from app.core.security import verify_password


@pytest.fixture(autouse=True)
def clean_env():
    """Ensure clean database and storage environment before each test."""
    init_db()
    db = SessionLocal()
    try:
        db.query(AuditLog).delete()
        db.query(User).delete()
        db.commit()
    finally:
        db.close()

    storage_dir = os.path.join(os.getcwd(), "data", "storage")
    raft_dir = os.path.join(os.getcwd(), "data", "raft")
    for d in [storage_dir, raft_dir]:
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d, exist_ok=True)

    yield

    for d in [storage_dir, raft_dir]:
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)


@pytest.mark.asyncio
async def test_register_user_command_proto_serialization():
    """Verify REGISTER_USER command proto serialization preserves all 8 fields in metadata_json without plaintext password."""
    cmd = FileOperationCommand(
        operation=FileOperationType.REGISTER_USER,
        file_id="user-123",
        owner_id="user-123",
        filename="testuser",
        email="testuser@example.com",
        username="testuser",
        password_hash="$argon2id$v=19$m=65536,t=3,p=4$dummyhash$dummyhash",
        role=UserRole.USER.value
    )

    entry = RaftLogEntry(term=1, index=1, command=cmd)
    proto_entry = entry.to_proto()

    # Simulate gRPC wire serialization / deserialization round trip
    serialized_bytes = proto_entry.SerializeToString()
    wire_proto = raft_pb2.LogEntry()
    wire_proto.ParseFromString(serialized_bytes)

    reconstructed_entry = RaftLogEntry.from_proto(wire_proto)
    reconstructed_cmd = reconstructed_entry.command

    # Assert ALL 8 required REGISTER_USER fields are preserved exactly
    assert reconstructed_cmd.operation == FileOperationType.REGISTER_USER
    assert reconstructed_cmd.file_id == "user-123"
    assert reconstructed_cmd.owner_id == "user-123"
    assert reconstructed_cmd.filename == "testuser"
    assert reconstructed_cmd.email == "testuser@example.com"
    assert reconstructed_cmd.username == "testuser"
    assert reconstructed_cmd.password_hash == "$argon2id$v=19$m=65536,t=3,p=4$dummyhash$dummyhash"
    assert reconstructed_cmd.role == UserRole.USER.value
    assert not hasattr(reconstructed_cmd, "password")


@pytest.mark.asyncio
async def test_state_machine_applies_register_user():
    """Verify StateMachineExecutor applies REGISTER_USER entry to PostgreSQL User + AuditLog tables."""
    node = RaftNode(node_id="node-1")
    node.persistent_state.current_term = 1
    node.role = NodeRole.LEADER

    cmd = FileOperationCommand(
        operation=FileOperationType.REGISTER_USER,
        file_id="user-reg-1",
        owner_id="user-reg-1",
        filename="alice",
        email="alice@example.com",
        username="alice",
        password_hash="$argon2id$v=19$m=65536,t=3,p=4$alicehash$alicehash",
        role=UserRole.USER.value
    )
    await node.append_log_entry(cmd)
    node.volatile_state.commit_index = 1
    applied = await state_machine_executor.apply_committed_entries(node)

    assert applied == 1
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == "user-reg-1").first()
        assert user is not None
        assert user.username == "alice"
        assert user.email == "alice@example.com"
        assert user.password_hash == "$argon2id$v=19$m=65536,t=3,p=4$alicehash$alicehash"
        assert user.role == UserRole.USER.value

        audit = db.query(AuditLog).filter(AuditLog.user_id == "user-reg-1", AuditLog.event_type == "USER_REGISTERED").first()
        assert audit is not None
    finally:
        db.close()


@pytest.mark.asyncio
async def test_state_machine_rejects_malformed_register_user():
    """Verify StateMachineExecutor explicitly rejects malformed REGISTER_USER commands with missing required fields."""
    node = RaftNode(node_id="node-1")
    node.persistent_state.current_term = 1
    node.role = NodeRole.LEADER

    cmd = FileOperationCommand(
        operation=FileOperationType.REGISTER_USER,
        file_id="bad-reg-1",
        owner_id="bad-reg-1",
        filename="bad",
        email=None,
        username="bad",
        password_hash="$argon2id$hash",
        role="USER"
    )
    await node.append_log_entry(cmd)
    node.volatile_state.commit_index = 1

    with pytest.raises(ValueError, match="Malformed REGISTER_USER command"):
        await state_machine_executor.apply_committed_entries(node)


@pytest.mark.asyncio
async def test_state_machine_register_user_idempotency():
    """Verify StateMachineExecutor handles duplicate REGISTER_USER entries idempotently."""
    node = RaftNode(node_id="node-1")
    node.persistent_state.current_term = 1
    node.role = NodeRole.LEADER

    cmd = FileOperationCommand(
        operation=FileOperationType.REGISTER_USER,
        file_id="user-reg-2",
        owner_id="user-reg-2",
        filename="bob",
        email="bob@example.com",
        username="bob",
        password_hash="$argon2id$v=19$m=65536,t=3,p=4$bobhash$bobhash",
        role=UserRole.USER.value
    )
    await node.append_log_entry(cmd)
    node.volatile_state.commit_index = 1

    # First application
    applied1 = await state_machine_executor.apply_committed_entries(node)
    assert applied1 == 1

    # Second application of same committed index (simulating replay or redundant apply)
    applied2 = await state_machine_executor.apply_committed_entries(node)
    assert applied2 == 0

    db = SessionLocal()
    try:
        users = db.query(User).filter(User.username == "bob").all()
        assert len(users) == 1
    finally:
        db.close()


def test_follower_registration_rejected():
    """Verify non-leader nodes reject POST /api/v1/auth/register with HTTP 503 NOT_LEADER."""
    raft_node_instance.role = NodeRole.FOLLOWER
    raft_node_instance.leader_id = "node-2"

    client = TestClient(app)
    response = client.post(
        "/api/v1/auth/register",
        json={
            "email": "follower_reg@example.com",
            "username": "follower_reg",
            "password": "Password123!"
        }
    )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["error"] == "NOT_LEADER"
    assert body["detail"]["leader_id"] == "node-2"


def test_leader_registration_flow_and_login():
    """Verify leader processes POST /api/v1/auth/register through Raft consensus and allows login."""
    raft_node_instance.role = NodeRole.LEADER
    raft_node_instance.leader_id = "node-1"

    with patch("app.raft.engine.raft_election_engine.replicate_to_peers_and_await_applied", new_callable=AsyncMock) as mock_repl:
        # Simulate successful quorum replication and state machine apply
        async def mock_quorum(index, timeout=5.0):
            db = SessionLocal()
            try:
                # Retrieve command from log and apply to DB directly in mock
                entry = raft_node_instance.get_log_entry(index)
                if entry and entry.command.operation == FileOperationType.REGISTER_USER:
                    cmd = entry.command
                    user_rec = User(
                        id=cmd.file_id,
                        email=cmd.email,
                        username=cmd.username,
                        password_hash=cmd.password_hash,
                        role=cmd.role
                    )
                    db.add(user_rec)
                    db.commit()
            finally:
                db.close()
            return True

        mock_repl.side_effect = mock_quorum

        client = TestClient(app)
        reg_response = client.post(
            "/api/v1/auth/register",
            json={
                "email": "charlie@example.com",
                "username": "charlie",
                "password": "SecurePassword123!"
            }
        )

        assert reg_response.status_code == 201
        res_data = reg_response.json()
        assert res_data["username"] == "charlie"
        assert res_data["email"] == "charlie@example.com"
        assert "id" in res_data

        # Verify registration entry in Raft log contained hashed password, not plaintext password
        log_entry = raft_node_instance.persistent_state.log[-1]
        assert log_entry.command.operation == FileOperationType.REGISTER_USER
        assert log_entry.command.password_hash != "SecurePassword123!"
        assert verify_password("SecurePassword123!", log_entry.command.password_hash)

        # Verify user can log in with registered credentials
        login_response = client.post(
            "/api/v1/auth/login",
            json={
                "username_or_email": "charlie",
                "password": "SecurePassword123!"
            }
        )
        assert login_response.status_code == 200
        token_data = login_response.json()
        assert "access_token" in token_data
        assert token_data["user"]["username"] == "charlie"


@pytest.mark.asyncio
async def test_replicate_to_peers_and_await_applied_waits_for_last_applied():
    """Regression test: verify replicate_to_peers_and_await_applied waits for last_applied >= target_index."""
    from app.raft.engine import raft_election_engine

    node = RaftNode(node_id="node-1")
    node.role = NodeRole.LEADER
    raft_election_engine.node = node
    orig_peers = raft_election_engine.peer_pool._peers_override
    raft_election_engine.peer_pool._peers_override = ["127.0.0.1:59991", "127.0.0.1:59992"]

    try:
        cmd = FileOperationCommand(
            operation=FileOperationType.REGISTER_USER,
            file_id="user-race-test",
            owner_id="user-race-test",
            filename="race_user",
            email="race@example.com",
            username="race_user",
            password_hash="$argon2id$stub",
            role="USER"
        )
        await node.append_log_entry(cmd)

        # 1. Commit index reaches target (1), but last_applied remains behind (0)
        node.volatile_state.commit_index = 1
        node.volatile_state.last_applied = 0

        # 2. Verify replicate_to_peers_and_await_applied times out / returns False while last_applied is 0
        with patch.object(raft_election_engine, "_replicate_to_peer", new_callable=AsyncMock):
            result_behind = await raft_election_engine.replicate_to_peers_and_await_applied(1, timeout=0.1)
            assert result_behind is False

            # 3. Advance last_applied to 1 (simulating state-machine completion)
            node.volatile_state.last_applied = 1

            # 4. Verify replicate_to_peers_and_await_applied returns True once last_applied reaches target
            result_applied = await raft_election_engine.replicate_to_peers_and_await_applied(1, timeout=0.5)
            assert result_applied is True
    finally:
        raft_election_engine.peer_pool._peers_override = orig_peers


@pytest.mark.asyncio
async def test_register_user_foreign_key_audit_transaction_ordering():
    """Regression test: verify audit log event insertion succeeds without FK violation when request DB session ran pre-Raft queries."""
    from app.core.audit import log_audit_event

    db1 = SessionLocal()
    try:
        # 1. Simulate pre-Raft read query on db1 (opens transaction T1)
        _ = db1.query(User).filter(User.username == "test_fk_user").first()

        # 2. Close/commit initial read transaction on db1 so T1 does not hold a stale snapshot
        db1.commit()

        # 3. Simulate Raft state machine commit on separate session T2
        user_id = str(uuid.uuid4())
        db2 = SessionLocal()
        try:
            user_rec = User(
                id=user_id,
                email="fk_user@example.com",
                username="test_fk_user",
                password_hash="$argon2id$v=19$m=65536,t=3,p=4$stub$stub",
                role="USER"
            )
            db2.add(user_rec)
            db2.commit()
        finally:
            db2.close()

        # 4. Refresh session db1 transaction context following state-machine commit
        db1.commit()

        # 5. Insert audit log event referencing user_id
        audit_entry = log_audit_event(
            db=db1,
            event_type="USER_REGISTERED",
            user_id=user_id,
            details={"username": "test_fk_user"}
        )

        assert audit_entry.user_id == user_id
    finally:
        db1.close()


@pytest.mark.asyncio
async def test_idempotency_same_index_same_command():
    """Verify that applying the exact same command at the same Raft index succeeds idempotently."""
    from app.db.models.file import AppliedCommand, FileMetadata
    node = RaftNode(node_id="test-node-1")
    cmd = FileOperationCommand(
        operation=FileOperationType.REGISTER_USER,
        file_id="user-same-1",
        owner_id="user-same-1",
        filename="same_user",
        email="same@example.com",
        username="same_user",
        password_hash="$argon2id$v=19$m=65536,t=3,p=4$dummy$dummy",
        role="USER"
    )
    entry = RaftLogEntry(term=1, index=1, command=cmd)
    node.persistent_state.log = [entry]
    node.volatile_state.commit_index = 1
    node.volatile_state.last_applied = 0

    applied1 = await state_machine_executor.apply_committed_entries(node)
    assert applied1 == 1
    assert node.volatile_state.last_applied == 1

    # Reset last_applied to 0 to force re-application of index 1
    node.volatile_state.last_applied = 0
    applied2 = await state_machine_executor.apply_committed_entries(node)
    assert applied2 == 1
    assert node.volatile_state.last_applied == 1


@pytest.mark.asyncio
async def test_idempotency_same_index_different_command():
    """Verify that if a stale AppliedCommand exists at the index for a different command, state machine overwrites it and applies the new metadata."""
    from app.db.models.file import AppliedCommand, FileMetadata, FileVersion

    # 1. Manually seed a stale AppliedCommand at index 5
    db = SessionLocal()
    try:
        stale_app = AppliedCommand(
            raft_index=5,
            term=1,
            command_type="FileOperationType.REGISTER_USER",
            file_id="old-stale-user-id"
        )
        db.add(stale_app)
        db.commit()
    finally:
        db.close()

    # 2. Construct a new UPLOAD command targeting index 5
    file_id = "new-upload-file-5"
    cmd = FileOperationCommand(
        operation=FileOperationType.UPLOAD,
        file_id=file_id,
        owner_id="owner-123",
        filename="test_upload.txt",
        sha256="abc123sha256",
        object_key="users/owner-123/new-upload-file-5/v1",
        size_bytes=100,
        content_type="text/plain"
    )

    node = RaftNode(node_id="test-node-2")
    # Pad log with 4 dummy entries so index 5 aligns with log index 5 (1-indexed)
    dummy = RaftLogEntry(term=1, index=1, command=FileOperationCommand(operation=FileOperationType.REGISTER_USER, file_id="u1", owner_id="u1", filename="u1", email="u1@test.com", username="u1", password_hash="hash"))
    node.persistent_state.log = [
        RaftLogEntry(term=1, index=1, command=cmd),
        RaftLogEntry(term=1, index=2, command=cmd),
        RaftLogEntry(term=1, index=3, command=cmd),
        RaftLogEntry(term=1, index=4, command=cmd),
        RaftLogEntry(term=1, index=5, command=cmd)
    ]
    node.volatile_state.commit_index = 5
    node.volatile_state.last_applied = 4

    applied = await state_machine_executor.apply_committed_entries(node)
    assert applied == 1
    assert node.volatile_state.last_applied == 5

    # 3. Verify FileMetadata was correctly created in DB and AppliedCommand updated
    db2 = SessionLocal()
    try:
        f_meta = db2.query(FileMetadata).filter(FileMetadata.id == file_id).first()
        assert f_meta is not None
        assert f_meta.original_filename == "test_upload.txt"
        assert f_meta.current_sha256 == "abc123sha256"

        app_rec = db2.query(AppliedCommand).filter(AppliedCommand.raft_index == 5).first()
        assert app_rec is not None
        assert app_rec.command_type == "FileOperationType.UPLOAD"
        assert app_rec.file_id == file_id
    finally:
        db2.close()

