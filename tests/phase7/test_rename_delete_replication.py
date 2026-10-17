import os
import shutil
import uuid
import json
import pytest
import asyncio
from unittest.mock import patch, AsyncMock, MagicMock

from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.main import app
from app.db.session import SessionLocal, init_db
from app.db.models.user import User, UserRole
from app.db.models.file import FileMetadata, FileVersion, PendingObjectCleanup, AppliedCommand
from app.db.models.audit import AuditLog
from app.proto import raft_pb2
from app.services.commands import FileOperationCommand, FileOperationType
from app.raft.node import RaftNode, NodeRole, raft_node_instance
from app.raft.state import RaftLogEntry
from app.raft.state_machine import state_machine_executor
from app.services.auditor import replica_auditor
from app.core.security import create_access_token


@pytest.fixture(autouse=True)
def clean_env():
    """Ensure clean database and storage environment before each test."""
    init_db()
    db = SessionLocal()
    try:
        db.query(PendingObjectCleanup).delete()
        db.query(AppliedCommand).delete()
        db.query(AuditLog).delete()
        db.query(FileVersion).delete()
        db.query(FileMetadata).delete()
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
async def test_rename_command_proto_serialization():
    """Verify RENAME command proto serialization round-trip preserves all fields and metadata."""
    cmd = FileOperationCommand(
        operation=FileOperationType.RENAME,
        file_id="file-100",
        owner_id="user-100",
        filename="new_document.pdf",
        new_name="new_document.pdf",
        metadata={"original_filename": "New Document (Draft).pdf"}
    )

    entry = RaftLogEntry(term=2, index=5, command=cmd)
    proto_entry = entry.to_proto()

    serialized_bytes = proto_entry.SerializeToString()
    wire_proto = raft_pb2.LogEntry()
    wire_proto.ParseFromString(serialized_bytes)

    reconstructed_entry = RaftLogEntry.from_proto(wire_proto)
    reconstructed_cmd = reconstructed_entry.command

    assert reconstructed_cmd.operation == FileOperationType.RENAME
    assert reconstructed_cmd.file_id == "file-100"
    assert reconstructed_cmd.owner_id == "user-100"
    assert reconstructed_cmd.filename == "new_document.pdf"
    assert reconstructed_cmd.new_name == "new_document.pdf"
    assert reconstructed_cmd.metadata.get("original_filename") == "New Document (Draft).pdf"


@pytest.mark.asyncio
async def test_delete_command_proto_serialization():
    """Verify DELETE command proto serialization round-trip."""
    cmd = FileOperationCommand(
        operation=FileOperationType.DELETE,
        file_id="file-200",
        owner_id="user-200",
        filename="old_archive.zip"
    )

    entry = RaftLogEntry(term=3, index=12, command=cmd)
    proto_entry = entry.to_proto()

    serialized_bytes = proto_entry.SerializeToString()
    wire_proto = raft_pb2.LogEntry()
    wire_proto.ParseFromString(serialized_bytes)

    reconstructed_entry = RaftLogEntry.from_proto(wire_proto)
    reconstructed_cmd = reconstructed_entry.command

    assert reconstructed_cmd.operation == FileOperationType.DELETE
    assert reconstructed_cmd.file_id == "file-200"
    assert reconstructed_cmd.owner_id == "user-200"
    assert reconstructed_cmd.filename == "old_archive.zip"


@pytest.mark.asyncio
async def test_state_machine_applies_rename_deterministically():
    """Verify state machine execution of RENAME updates PostgreSQL metadata and emits AuditLog."""
    db = SessionLocal()
    user_id = str(uuid.uuid4())
    file_id = str(uuid.uuid4())
    try:
        user = User(
            id=user_id,
            email="renamer@example.com",
            username="renamer",
            password_hash="fakehash",
            role=UserRole.USER.value
        )
        file_record = FileMetadata(
            id=file_id,
            owner_id=user_id,
            original_filename="Initial Name.txt",
            sanitized_name="Initial_Name.txt",
            content_type="text/plain",
            size_bytes=100,
            current_sha256="fakehash256",
            current_version=1,
            is_deleted=False
        )
        db.add(user)
        db.add(file_record)
        db.commit()
    finally:
        db.close()

    node = RaftNode(node_id="node-1")
    node.persistent_state.current_term = 1
    node.role = NodeRole.LEADER

    cmd = FileOperationCommand(
        operation=FileOperationType.RENAME,
        file_id=file_id,
        owner_id=user_id,
        filename="Updated_Name.txt",
        new_name="Updated_Name.txt",
        metadata={"original_filename": "Updated Name.txt"}
    )
    entry = RaftLogEntry(term=1, index=1, command=cmd)
    node.persistent_state.log.append(entry)
    node.volatile_state.commit_index = 1

    applied = await state_machine_executor.apply_committed_entries(node)
    assert applied == 1
    assert node.volatile_state.last_applied == 1

    # Verify DB updates
    db = SessionLocal()
    try:
        updated_f = db.query(FileMetadata).filter(FileMetadata.id == file_id).first()
        assert updated_f is not None
        assert updated_f.sanitized_name == "Updated Name.txt"
        assert updated_f.original_filename == "Updated Name.txt"

        audit = db.query(AuditLog).filter(
            AuditLog.resource_id == file_id,
            AuditLog.event_type == "FILE_RENAMED"
        ).first()
        assert audit is not None
        assert "Updated Name.txt" in audit.details_json

        applied_rec = db.query(AppliedCommand).filter(AppliedCommand.raft_index == 1).first()
        assert applied_rec is not None
        assert applied_rec.command_type == "FileOperationType.RENAME"
        assert applied_rec.file_id == file_id
    finally:
        db.close()


@pytest.mark.asyncio
async def test_state_machine_rename_idempotency():
    """Verify that duplicate or replayed RENAME commands at same index are handled idempotently."""
    db = SessionLocal()
    user_id = str(uuid.uuid4())
    file_id = str(uuid.uuid4())
    try:
        user = User(
            id=user_id,
            email="idempotent@example.com",
            username="idempotent",
            password_hash="fakehash",
            role=UserRole.USER.value
        )
        file_record = FileMetadata(
            id=file_id,
            owner_id=user_id,
            original_filename="Doc.pdf",
            sanitized_name="Doc.pdf",
            content_type="application/pdf",
            size_bytes=50,
            current_sha256="fakehash",
            current_version=1,
            is_deleted=False
        )
        db.add(user)
        db.add(file_record)
        db.commit()
    finally:
        db.close()

    node = RaftNode(node_id="node-1")
    node.persistent_state.current_term = 1
    node.role = NodeRole.LEADER

    cmd = FileOperationCommand(
        operation=FileOperationType.RENAME,
        file_id=file_id,
        owner_id=user_id,
        filename="Doc_V2.pdf",
        new_name="Doc_V2.pdf",
        metadata={"original_filename": "Doc V2.pdf"}
    )
    entry = RaftLogEntry(term=1, index=1, command=cmd)
    node.persistent_state.log.append(entry)
    node.volatile_state.commit_index = 1

    # First apply
    applied_1 = await state_machine_executor.apply_committed_entries(node)
    assert applied_1 == 1

    # Simulate node replay from index 0
    node.volatile_state.last_applied = 0
    applied_2 = await state_machine_executor.apply_committed_entries(node)
    # The command is recognized as already applied
    assert node.volatile_state.last_applied == 1

    db = SessionLocal()
    try:
        audits = db.query(AuditLog).filter(AuditLog.resource_id == file_id).all()
        assert len(audits) == 1
    finally:
        db.close()


@pytest.mark.asyncio
async def test_state_machine_applies_delete_and_creates_cleanup():
    """Verify state machine execution of DELETE marks file is_deleted and inserts PendingObjectCleanup."""
    db = SessionLocal()
    user_id = str(uuid.uuid4())
    file_id = str(uuid.uuid4())
    obj_key = f"users/{user_id}/{file_id}/v1"
    try:
        user = User(
            id=user_id,
            email="deleter@example.com",
            username="deleter",
            password_hash="fakehash",
            role=UserRole.USER.value
        )
        file_record = FileMetadata(
            id=file_id,
            owner_id=user_id,
            original_filename="ToDelete.txt",
            sanitized_name="ToDelete.txt",
            content_type="text/plain",
            size_bytes=10,
            current_sha256="fakehash",
            current_version=1,
            is_deleted=False
        )
        version_rec = FileVersion(
            file_id=file_id,
            version_number=1,
            object_key=obj_key,
            size_bytes=10,
            sha256_hash="fakehash"
        )
        db.add(user)
        db.add(file_record)
        db.add(version_rec)
        db.commit()
    finally:
        db.close()

    node = RaftNode(node_id="node-1")
    node.persistent_state.current_term = 1
    node.role = NodeRole.LEADER

    cmd = FileOperationCommand(
        operation=FileOperationType.DELETE,
        file_id=file_id,
        owner_id=user_id,
        filename="ToDelete.txt"
    )
    entry = RaftLogEntry(term=1, index=1, command=cmd)
    node.persistent_state.log.append(entry)
    node.volatile_state.commit_index = 1

    applied = await state_machine_executor.apply_committed_entries(node)
    assert applied == 1

    db = SessionLocal()
    try:
        deleted_f = db.query(FileMetadata).filter(FileMetadata.id == file_id).first()
        assert deleted_f.is_deleted is True

        cleanup_entry = db.query(PendingObjectCleanup).filter(
            PendingObjectCleanup.file_id == file_id
        ).first()
        assert cleanup_entry is not None
        assert cleanup_entry.object_key == obj_key
        assert cleanup_entry.status == "PENDING"
        assert cleanup_entry.retry_count == 0

        audit = db.query(AuditLog).filter(
            AuditLog.resource_id == file_id,
            AuditLog.event_type == "FILE_DELETED"
        ).first()
        assert audit is not None
    finally:
        db.close()


@pytest.mark.asyncio
async def test_replica_auditor_drains_pending_cleanups():
    """Verify ReplicaAuditor drains PendingObjectCleanup records and handles peer deletions."""
    db = SessionLocal()
    cleanup_id = str(uuid.uuid4())
    obj_key = "users/u1/f1/v1"
    try:
        cln = PendingObjectCleanup(
            id=cleanup_id,
            file_id="f1",
            object_key=obj_key,
            status="PENDING",
            retry_count=0
        )
        db.add(cln)
        db.commit()
    finally:
        db.close()

    with patch("app.storage.minio_client.storage_service.delete_object") as mock_local_del, \
         patch("app.storage.replication_service.replication_service.delete_object_from_all_peers", new_callable=AsyncMock) as mock_peer_del:
        mock_peer_del.return_value = {"http://peer2:8002": True, "http://peer3:8003": True}

        processed = await replica_auditor.process_pending_cleanups()
        assert processed == 1
        mock_local_del.assert_called_once_with(obj_key)
        mock_peer_del.assert_awaited_once_with(obj_key)

    db = SessionLocal()
    try:
        updated_cln = db.query(PendingObjectCleanup).filter(PendingObjectCleanup.id == cleanup_id).first()
        assert updated_cln.status == "COMPLETED"
    finally:
        db.close()


def test_api_rename_file_authorization_and_leader_rejection():
    """Verify API PATCH /files/{id} enforces non-leader 503, 404 for missing file, and 403 for non-owner."""
    client = TestClient(app)

    db = SessionLocal()
    owner_id = str(uuid.uuid4())
    other_user_id = str(uuid.uuid4())
    file_id = str(uuid.uuid4())
    try:
        owner = User(id=owner_id, email="owner@test.com", username="owner", password_hash="pwd", role=UserRole.USER.value)
        other_user = User(id=other_user_id, email="other@test.com", username="other", password_hash="pwd", role=UserRole.USER.value)
        f_meta = FileMetadata(
            id=file_id,
            owner_id=owner_id,
            original_filename="orig.txt",
            sanitized_name="orig.txt",
            content_type="text/plain",
            size_bytes=10,
            current_sha256="hash",
            current_version=1,
            is_deleted=False
        )
        db.add(owner)
        db.add(other_user)
        db.add(f_meta)
        db.commit()
    finally:
        db.close()

    owner_token = create_access_token({"sub": owner_id})
    other_token = create_access_token({"sub": other_user_id})

    # 1. Non-leader test
    with patch.object(raft_node_instance, "role", NodeRole.FOLLOWER):
        res = client.patch(
            f"/api/v1/files/{file_id}",
            json={"new_filename": "renamed.txt"},
            headers={"Authorization": f"Bearer {owner_token}"}
        )
        assert res.status_code == 503
        assert res.json()["detail"]["error"] == "NOT_LEADER"

    # Set as leader for remaining authorization checks
    with patch.object(raft_node_instance, "role", NodeRole.LEADER):
        # 2. 404 for non-existent file
        res = client.patch(
            f"/api/v1/files/{uuid.uuid4()}",
            json={"new_filename": "renamed.txt"},
            headers={"Authorization": f"Bearer {owner_token}"}
        )
        assert res.status_code == 404

        # 3. 403 for other non-admin user
        res = client.patch(
            f"/api/v1/files/{file_id}",
            json={"new_filename": "renamed.txt"},
            headers={"Authorization": f"Bearer {other_token}"}
        )
        assert res.status_code == 403

        # 4. Idempotent rename (file already has the name)
        res = client.patch(
            f"/api/v1/files/{file_id}",
            json={"new_filename": "orig.txt"},
            headers={"Authorization": f"Bearer {owner_token}"}
        )
        assert res.status_code == 200
        assert res.json()["sanitized_name"] == "orig.txt"


def test_api_delete_file_authorization_and_idempotency():
    """Verify API DELETE /files/{id} enforces non-leader 503, 404, 403, and idempotent deletion."""
    client = TestClient(app)

    db = SessionLocal()
    owner_id = str(uuid.uuid4())
    other_user_id = str(uuid.uuid4())
    file_id = str(uuid.uuid4())
    try:
        owner = User(id=owner_id, email="delowner@test.com", username="delowner", password_hash="pwd", role=UserRole.USER.value)
        other = User(id=other_user_id, email="delother@test.com", username="delother", password_hash="pwd", role=UserRole.USER.value)
        f_meta = FileMetadata(
            id=file_id,
            owner_id=owner_id,
            original_filename="delme.txt",
            sanitized_name="delme.txt",
            content_type="text/plain",
            size_bytes=10,
            current_sha256="hash",
            current_version=1,
            is_deleted=False
        )
        db.add(owner)
        db.add(other)
        db.add(f_meta)
        db.commit()
    finally:
        db.close()

    owner_token = create_access_token({"sub": owner_id})
    other_token = create_access_token({"sub": other_user_id})

    # 1. Non-leader test
    with patch.object(raft_node_instance, "role", NodeRole.FOLLOWER):
        res = client.delete(
            f"/api/v1/files/{file_id}",
            headers={"Authorization": f"Bearer {owner_token}"}
        )
        assert res.status_code == 503
        assert res.json()["detail"]["error"] == "NOT_LEADER"

    # Set as leader for authorization checks
    with patch.object(raft_node_instance, "role", NodeRole.LEADER):
        # 2. 404 for non-existent file
        res = client.delete(
            f"/api/v1/files/{uuid.uuid4()}",
            headers={"Authorization": f"Bearer {owner_token}"}
        )
        assert res.status_code == 404

        # 3. 403 for unauthorized non-owner
        res = client.delete(
            f"/api/v1/files/{file_id}",
            headers={"Authorization": f"Bearer {other_token}"}
        )
        assert res.status_code == 403

        # Mark as deleted in DB to test idempotent return
        db = SessionLocal()
        f_rec = db.query(FileMetadata).filter(FileMetadata.id == file_id).first()
        f_rec.is_deleted = True
        db.commit()
        db.close()

        res = client.delete(
            f"/api/v1/files/{file_id}",
            headers={"Authorization": f"Bearer {owner_token}"}
        )
        assert res.status_code == 200
        assert "deleted successfully" in res.json()["message"]
