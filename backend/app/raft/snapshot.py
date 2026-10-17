import os
import json
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field

from app.config import settings
from app.core.crypto import compute_sha256
from app.db.session import SessionLocal
from app.db.models.user import User
from app.db.models.file import FileMetadata, FileVersion, AppliedCommand
from app.db.models.audit import AuditLog

logger = logging.getLogger("secure-raft-dropbox.snapshot")


class RaftSnapshot(BaseModel):
    """
    Strongly-typed Raft Snapshot model representing a complete, deterministic
    state-machine checkpoint at last_included_index.
    CRITICAL: Raw file content bytes MUST NEVER be placed inside RaftSnapshot.
    """
    schema_version: int = 1
    snapshot_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    last_included_index: int
    last_included_term: int
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    state_machine_checksum: str
    metadata_state: Dict[str, List[Dict[str, Any]]]

    def to_json(self) -> str:
        return self.model_dump_json(indent=2)

    @classmethod
    def from_json(cls, json_str: str) -> "RaftSnapshot":
        data = json.loads(json_str)
        snapshot = cls.model_validate(data)
        # Verify state machine checksum integrity
        canonical_bytes = json.dumps(snapshot.metadata_state, sort_keys=True).encode("utf-8")
        computed_checksum = compute_sha256(canonical_bytes)
        if snapshot.state_machine_checksum != computed_checksum:
            raise ValueError(
                f"Snapshot checksum mismatch! Stored: {snapshot.state_machine_checksum}, Computed: {computed_checksum}"
            )
        return snapshot


def dump_postgres_state_machine() -> Dict[str, List[Dict[str, Any]]]:
    """
    Exports complete deterministic PostgreSQL table checkpoint.
    Includes users, file_metadata, file_versions, audit_logs, and applied_commands.
    """
    db = SessionLocal()
    try:
        users = [
            {
                "id": u.id,
                "email": u.email,
                "username": u.username,
                "password_hash": u.password_hash,
                "role": str(u.role),
                "is_active": u.is_active,
                "created_at": u.created_at.isoformat() if u.created_at else None
            }
            for u in db.query(User).all()
        ]
        file_metadata = [
            {
                "id": fm.id,
                "owner_id": fm.owner_id,
                "original_filename": fm.original_filename,
                "sanitized_name": fm.sanitized_name,
                "content_type": fm.content_type,
                "size_bytes": fm.size_bytes,
                "current_sha256": fm.current_sha256,
                "current_version": fm.current_version,
                "is_deleted": fm.is_deleted,
                "created_at": fm.created_at.isoformat() if fm.created_at else None
            }
            for fm in db.query(FileMetadata).all()
        ]
        file_versions = [
            {
                "id": fv.id,
                "file_id": fv.file_id,
                "version_number": fv.version_number,
                "object_key": fv.object_key,
                "size_bytes": fv.size_bytes,
                "sha256_hash": fv.sha256_hash,
                "created_at": fv.created_at.isoformat() if fv.created_at else None
            }
            for fv in db.query(FileVersion).all()
        ]
        audit_logs = [
            {
                "id": al.id,
                "user_id": al.user_id,
                "event_type": al.event_type,
                "resource_id": al.resource_id,
                "ip_address": al.ip_address,
                "details_json": al.details_json,
                "created_at": al.created_at.isoformat() if al.created_at else None
            }
            for al in db.query(AuditLog).all()
        ]
        applied_commands = [
            {
                "raft_index": ac.raft_index,
                "term": ac.term,
                "command_type": ac.command_type,
                "file_id": ac.file_id,
                "applied_at": ac.applied_at.isoformat() if ac.applied_at else None
            }
            for ac in db.query(AppliedCommand).all()
        ]
        return {
            "users": users,
            "file_metadata": file_metadata,
            "file_versions": file_versions,
            "audit_logs": audit_logs,
            "applied_commands": applied_commands,
        }
    finally:
        db.close()


def restore_postgres_state_machine(state_data: Dict[str, List[Dict[str, Any]]]):
    """
    Restores PostgreSQL state machine tables from snapshot metadata dictionary inside a single transaction.
    """
    db = SessionLocal()
    try:
        db.query(AppliedCommand).delete()
        db.query(FileVersion).delete()
        db.query(FileMetadata).delete()
        db.query(AuditLog).delete()
        db.query(User).delete()
        db.flush()

        for u in state_data.get("users", []):
            user_rec = User(
                id=u["id"],
                email=u["email"],
                username=u["username"],
                password_hash=u["password_hash"],
                role=u.get("role", "USER"),
                is_active=u.get("is_active", True)
            )
            db.add(user_rec)
        db.flush()

        for fm in state_data.get("file_metadata", []):
            fm_rec = FileMetadata(
                id=fm["id"],
                owner_id=fm["owner_id"],
                original_filename=fm["original_filename"],
                sanitized_name=fm["sanitized_name"],
                content_type=fm["content_type"],
                size_bytes=fm["size_bytes"],
                current_sha256=fm["current_sha256"],
                current_version=fm["current_version"],
                is_deleted=fm["is_deleted"]
            )
            db.add(fm_rec)
        db.flush()

        for fv in state_data.get("file_versions", []):
            fv_rec = FileVersion(
                id=fv["id"],
                file_id=fv["file_id"],
                version_number=fv["version_number"],
                object_key=fv["object_key"],
                size_bytes=fv["size_bytes"],
                sha256_hash=fv["sha256_hash"]
            )
            db.add(fv_rec)

        for al in state_data.get("audit_logs", []):
            al_rec = AuditLog(
                id=al["id"],
                user_id=al["user_id"],
                event_type=al["event_type"],
                resource_id=al["resource_id"],
                ip_address=al.get("ip_address"),
                details_json=al.get("details_json")
            )
            db.add(al_rec)

        for ac in state_data.get("applied_commands", []):
            ac_rec = AppliedCommand(
                raft_index=ac["raft_index"],
                term=ac["term"],
                command_type=ac["command_type"],
                file_id=ac["file_id"]
            )
            db.add(ac_rec)

        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to restore PostgreSQL state machine from snapshot: {e}")
        raise e
    finally:
        db.close()


class SnapshotManager:
    """Manages crash-safe atomic snapshot disk persistence and loading."""

    @staticmethod
    def get_snapshot_dir(node_id: str, base_dir: Optional[str] = None) -> str:
        root_dir = base_dir or os.path.join(os.getcwd(), "data", "raft")
        if root_dir.endswith(node_id) or os.path.basename(root_dir) == node_id:
            target_dir = root_dir
        else:
            target_dir = os.path.join(root_dir, node_id)
        os.makedirs(target_dir, exist_ok=True)
        return target_dir

    @classmethod
    def save_snapshot_atomically(cls, node_id: str, snapshot: RaftSnapshot, base_dir: Optional[str] = None) -> str:
        target_dir = cls.get_snapshot_dir(node_id, base_dir)
        temp_path = os.path.join(target_dir, "snapshot.json.tmp")
        final_path = os.path.join(target_dir, "snapshot.json")

        json_data = snapshot.to_json()

        with open(temp_path, "w", encoding="utf-8") as f:
            f.write(json_data)
            f.flush()
            os.fsync(f.fileno())

        os.replace(temp_path, final_path)
        logger.info(f"Atomically persisted snapshot at index {snapshot.last_included_index} to {final_path}")
        return final_path

    @classmethod
    def load_snapshot(cls, node_id: str, base_dir: Optional[str] = None) -> Optional[RaftSnapshot]:
        target_dir = cls.get_snapshot_dir(node_id, base_dir)
        final_path = os.path.join(target_dir, "snapshot.json")

        if not os.path.exists(final_path):
            return None

        try:
            with open(final_path, "r", encoding="utf-8") as f:
                content = f.read()
            return RaftSnapshot.from_json(content)
        except Exception as e:
            logger.error(f"Error loading snapshot from {final_path}: {e}")
            return None
