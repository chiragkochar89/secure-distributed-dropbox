import json
import asyncio
import logging
from typing import Optional, Any

from app.db.session import SessionLocal
from app.db.models.file import FileMetadata, FileVersion, AppliedCommand, PendingObjectCleanup
from app.db.models.user import User, UserRole
from app.db.models.audit import AuditLog
from app.services.commands import FileOperationCommand, FileOperationType
from app.core.crypto import sanitize_filename
from app.raft.node import RaftNode

logger = logging.getLogger("secure-raft-dropbox.state-machine")


class StateMachineExecutor:
    """
    Raft State Machine Executor for local PostgreSQL metadata storage.
    Serializes entry application and guarantees deterministic, idempotent state updates.
    """

    def __init__(self):
        self.lock = asyncio.Lock()
        self.checkpoint_barrier = asyncio.Lock()

    async def apply_committed_entries(self, node: RaftNode) -> int:
        """
        Applies all committed entries from last_applied + 1 up to commit_index
        to the local PostgreSQL database transactionally under checkpoint_barrier.
        Advances last_applied only after successful database commit.
        """
        async with self.lock:
            applied_count = 0
            if node.volatile_state.last_applied < node.last_included_index:
                node.volatile_state.last_applied = node.last_included_index
            while node.volatile_state.last_applied < node.volatile_state.commit_index:
                next_idx = node.volatile_state.last_applied + 1
                entry = node.get_log_entry(next_idx)

                if not entry:
                    logger.warning(
                        f"State Machine on [{node.node_id}] expected entry at index {next_idx} "
                        f"but log length is {len(node.persistent_state.log)}. Stopping application."
                    )
                    break

                async with self.checkpoint_barrier:
                    # Execute PostgreSQL application transactionally
                    db = SessionLocal()
                    commit_succeeded = False
                    last_applied_before = node.volatile_state.last_applied
                    cmd = None
                    try:
                        cmd = entry.command
                        # Idempotency check: verify if raft_index was already applied
                        existing_app = db.query(AppliedCommand).filter(AppliedCommand.raft_index == next_idx).first()

                        should_apply = not existing_app
                        if existing_app:
                            cmd_file_id = getattr(cmd, "file_id", None) or getattr(cmd, "owner_id", None)
                            op_str = str(cmd.operation)
                            op_val = cmd.operation.value if hasattr(cmd.operation, "value") else op_str
                            same_command = (
                                existing_app.command_type in (op_str, op_val) and
                                existing_app.file_id == str(cmd_file_id)
                            )
                            if same_command:
                                should_apply = False
                                if cmd.operation == FileOperationType.REGISTER_USER:
                                    user_id = cmd.file_id or cmd.owner_id
                                    existing_user = db.query(User).filter(
                                        (User.id == user_id) | (User.email == cmd.email) | (User.username == cmd.username)
                                    ).first()
                                    if not existing_user:
                                        should_apply = True
                                elif cmd.operation == FileOperationType.UPLOAD:
                                    existing_file = db.query(FileMetadata).filter(FileMetadata.id == cmd.file_id).first()
                                    if not existing_file:
                                        should_apply = True
                                elif cmd.operation == FileOperationType.RENAME:
                                    existing_file = db.query(FileMetadata).filter(FileMetadata.id == cmd.file_id).first()
                                    target_raw = (cmd.metadata.get("original_filename") if cmd.metadata else None) or cmd.new_name or cmd.filename
                                    sanitized_target = sanitize_filename(target_raw) if target_raw else (cmd.filename or "")
                                    if not existing_file or (not existing_file.is_deleted and existing_file.sanitized_name != sanitized_target):
                                        should_apply = True
                                elif cmd.operation == FileOperationType.DELETE:
                                    existing_file = db.query(FileMetadata).filter(FileMetadata.id == cmd.file_id).first()
                                    if not existing_file or not existing_file.is_deleted:
                                        should_apply = True
                            else:
                                logger.info(
                                    f"State Machine detected stale AppliedCommand at index {next_idx}: "
                                    f"Old ({existing_app.command_type}, {existing_app.file_id}) vs New ({cmd.operation}, {cmd.file_id}). "
                                    f"Re-applying state machine for current command on Node [{node.node_id}]."
                                )
                                should_apply = True

                        if should_apply:
                            logger.info(
                                f"State Machine applying Raft Index {next_idx} (Term {entry.term}) "
                                f"Command: {cmd.operation} | FileID: {cmd.file_id} on Node [{node.node_id}]"
                            )

                            if cmd.operation == FileOperationType.REGISTER_USER:
                                logger.info(
                                    f"[DIAGNOSTIC] REGISTER_USER Application Start | Node: [{node.node_id}] | Raft Index: {next_idx} | "
                                    f"cmd.file_id: '{cmd.file_id}' | "
                                    f"cmd.email is None: {cmd.email is None} | "
                                    f"cmd.username is None: {cmd.username is None} | "
                                    f"cmd.password_hash is None: {cmd.password_hash is None} | "
                                    f"last_applied_before: {last_applied_before}"
                                )

                            if cmd.operation == FileOperationType.UPLOAD:
                                size = getattr(cmd, "size_bytes", 0) or 0
                                # Ensure owner_id exists in User table to avoid FK integrity error during testing/replication
                                user_exists = db.query(User).filter(User.id == cmd.owner_id).first()
                                if not user_exists:
                                    stub_user = User(
                                        id=cmd.owner_id,
                                        email=f"{cmd.owner_id}@system.local",
                                        username=cmd.owner_id,
                                        password_hash="$argon2id$v=19$m=65536,t=3,p=4$stubhash$stubhash"
                                    )
                                    db.add(stub_user)
                                    db.flush()

                                f_meta = db.query(FileMetadata).filter(FileMetadata.id == cmd.file_id).first()
                                if not f_meta:
                                    f_meta = FileMetadata(
                                        id=cmd.file_id,
                                        owner_id=cmd.owner_id,
                                        original_filename=cmd.filename,
                                        sanitized_name=cmd.filename,
                                        content_type="application/octet-stream",
                                        size_bytes=size,
                                        current_sha256=cmd.sha256,
                                        current_version=cmd.version,
                                        is_deleted=False
                                    )
                                    db.add(f_meta)
                                else:
                                    f_meta.current_version = cmd.version
                                    f_meta.current_sha256 = cmd.sha256
                                    f_meta.size_bytes = size
                                    f_meta.is_deleted = False

                                f_ver = db.query(FileVersion).filter(
                                    FileVersion.file_id == cmd.file_id,
                                    FileVersion.version_number == cmd.version
                                ).first()

                                if not f_ver:
                                    f_ver = FileVersion(
                                        file_id=cmd.file_id,
                                        version_number=cmd.version,
                                        object_key=cmd.object_key,
                                        size_bytes=size,
                                        sha256_hash=cmd.sha256
                                    )
                                    db.add(f_ver)

                            elif cmd.operation == FileOperationType.RENAME:
                                f_meta = db.query(FileMetadata).filter(FileMetadata.id == cmd.file_id).first()
                                if not f_meta:
                                    logger.warning(f"RENAME command ignored for non-existent file {cmd.file_id} on Node [{node.node_id}]")
                                elif f_meta.is_deleted:
                                    logger.warning(f"RENAME command ignored for already-deleted file {cmd.file_id} on Node [{node.node_id}]")
                                else:
                                    old_name = f_meta.sanitized_name
                                    target_raw = (cmd.metadata.get("original_filename") if cmd.metadata else None) or cmd.filename or cmd.new_name
                                    target_sanitized = sanitize_filename(target_raw) if target_raw else (cmd.filename or "")
                                    f_meta.original_filename = target_raw or target_sanitized
                                    f_meta.sanitized_name = target_sanitized

                                    audit_rec = AuditLog(
                                        user_id=cmd.owner_id or f_meta.owner_id,
                                        event_type="FILE_RENAMED",
                                        resource_id=cmd.file_id,
                                        ip_address=None,
                                        details_json=json.dumps({"old_name": old_name, "new_name": target_sanitized})
                                    )
                                    db.add(audit_rec)

                            elif cmd.operation == FileOperationType.DELETE:
                                f_meta = db.query(FileMetadata).filter(FileMetadata.id == cmd.file_id).first()
                                if not f_meta:
                                    logger.warning(f"DELETE command ignored for non-existent file {cmd.file_id} on Node [{node.node_id}]")
                                else:
                                    f_meta.is_deleted = True

                                    # Transactionally record durable cleanup pending records for all versions
                                    versions = db.query(FileVersion).filter(FileVersion.file_id == cmd.file_id).all()
                                    for v in versions:
                                        existing_cln = db.query(PendingObjectCleanup).filter(
                                            PendingObjectCleanup.file_id == cmd.file_id,
                                            PendingObjectCleanup.object_key == v.object_key
                                        ).first()
                                        if not existing_cln:
                                            cln_rec = PendingObjectCleanup(
                                                file_id=cmd.file_id,
                                                object_key=v.object_key,
                                                status="PENDING"
                                            )
                                            db.add(cln_rec)

                                    audit_rec = AuditLog(
                                        user_id=cmd.owner_id or f_meta.owner_id,
                                        event_type="FILE_DELETED",
                                        resource_id=cmd.file_id,
                                        ip_address=None,
                                        details_json=json.dumps({"filename": f_meta.sanitized_name})
                                    )
                                    db.add(audit_rec)

                            elif cmd.operation == FileOperationType.REGISTER_USER:
                                user_id = cmd.file_id or cmd.owner_id
                                if not cmd.email or not cmd.username or not cmd.password_hash:
                                    logger.error(
                                        f"Rejecting malformed REGISTER_USER command at Raft index {next_idx}: "
                                        f"Missing required fields (email={cmd.email}, username={cmd.username}, password_hash={bool(cmd.password_hash)})"
                                    )
                                    raise ValueError(
                                        f"Malformed REGISTER_USER command at index {next_idx}: email, username, and password_hash are required."
                                    )
                                existing_user = db.query(User).filter(
                                    (User.id == user_id) | (User.email == cmd.email) | (User.username == cmd.username)
                                ).first()
                                if not existing_user:
                                    user_rec = User(
                                        id=user_id,
                                        email=cmd.email,
                                        username=cmd.username,
                                        password_hash=cmd.password_hash,
                                        role=cmd.role or UserRole.USER.value
                                    )
                                    db.add(user_rec)

                                    audit_rec = AuditLog(
                                        user_id=user_id,
                                        event_type="USER_REGISTERED",
                                        resource_id=None,
                                        ip_address=None,
                                        details_json=json.dumps({"username": cmd.username, "role": cmd.role or UserRole.USER.value})
                                    )
                                    db.add(audit_rec)

                            # Record application tracking entry
                            if not existing_app:
                                app_rec = AppliedCommand(
                                    raft_index=next_idx,
                                    term=entry.term,
                                    command_type=str(cmd.operation),
                                    file_id=cmd.file_id
                                )
                                db.add(app_rec)
                            else:
                                existing_app.term = entry.term
                                existing_app.command_type = str(cmd.operation)
                                existing_app.file_id = cmd.file_id
                            db.commit()
                            commit_succeeded = True

                            # Post-commit async object storage replication (non-blocking)
                            if cmd.operation == FileOperationType.UPLOAD and cmd.object_key:
                                async def _async_replicate_object(obj_key=cmd.object_key, c_type=getattr(cmd, "content_type", "application/octet-stream")):
                                    try:
                                        from app.storage.minio_client import storage_service
                                        from app.storage.replication_service import replication_service
                                        file_bytes = await asyncio.to_thread(storage_service.download_object, obj_key)
                                        await replication_service.replicate_object_to_all_peers(obj_key, file_bytes, c_type)
                                    except Exception as st_err:
                                        logger.debug(
                                            f"Object payload {obj_key} not found locally on Node [{node.node_id}] for post-commit replication: {st_err}"
                                        )
                                asyncio.create_task(_async_replicate_object())

                            # Post-commit async object cleanup trigger (non-blocking)
                            if cmd.operation == FileOperationType.DELETE:
                                async def _async_trigger_cleanup():
                                    try:
                                        from app.services.auditor import replica_auditor
                                        await replica_auditor.process_pending_cleanups()
                                    except Exception as cln_err:
                                        logger.debug(f"Asynchronous object cleanup trigger note on Node [{node.node_id}]: {cln_err}")
                                asyncio.create_task(_async_trigger_cleanup())

                        # Transaction succeeded -> advance volatile last_applied monotonically
                        node.volatile_state.last_applied = next_idx
                        last_applied_after = node.volatile_state.last_applied
                        applied_count += 1

                        if cmd and cmd.operation == FileOperationType.REGISTER_USER:
                            logger.info(
                                f"[DIAGNOSTIC] REGISTER_USER Application Succeeded | Node: [{node.node_id}] | Raft Index: {next_idx} | "
                                f"commit_succeeded: {commit_succeeded} | last_applied_before: {last_applied_before} | last_applied_after: {last_applied_after}"
                            )

                    except Exception as e:
                        db.rollback()
                        if cmd and cmd.operation == FileOperationType.REGISTER_USER:
                            logger.error(
                                f"[DIAGNOSTIC] REGISTER_USER Application Failed | Node: [{node.node_id}] | Raft Index: {next_idx} | "
                                f"cmd.file_id: '{cmd.file_id}' | commit_succeeded: {commit_succeeded} | "
                                f"last_applied_before: {last_applied_before} | last_applied_current: {node.volatile_state.last_applied} | "
                                f"Exception: {type(e).__name__}: {e}",
                                exc_info=True
                            )
                        else:
                            logger.error(f"Failed to apply Raft index {next_idx} to PostgreSQL state machine: {e}", exc_info=True)
                        raise e
                    finally:
                        db.close()

            if applied_count > 0:
                try:
                    await self.maybe_create_snapshot(node)
                except Exception as snap_err:
                    logger.error(f"Automatic snapshot creation failed on Node [{node.node_id}]: {snap_err}")

            return applied_count

    async def maybe_create_snapshot(
        self,
        node: RaftNode,
        snapshot_threshold: Optional[int] = None,
        base_dir: Optional[str] = None
    ) -> Optional[Any]:
        """
        Creates a crash-safe, non-blocking Raft snapshot if last_applied - last_included_index >= snapshot_threshold.
        Uses State-Machine Checkpoint Barrier to ensure exact state machine snapshot consistency.
        Releases RaftNode lock during DB export and disk I/O.
        """
        from app.config import settings
        effective_threshold = snapshot_threshold if snapshot_threshold is not None else settings.SNAPSHOT_THRESHOLD

        last_applied = node.volatile_state.last_applied
        if last_applied - node.last_included_index < effective_threshold:
            return None

        boundary_index = last_applied
        boundary_term = node.get_log_term_at(boundary_index)

        # 1. Acquire Checkpoint Barrier: Exports DB tables at exactly boundary_index
        from app.raft.snapshot import dump_postgres_state_machine, RaftSnapshot, SnapshotManager
        from app.core.crypto import compute_sha256
        import json

        async with self.checkpoint_barrier:
            state_data = dump_postgres_state_machine()
            canonical_bytes = json.dumps(state_data, sort_keys=True).encode("utf-8")
            checksum = compute_sha256(canonical_bytes)

        # 2. Construct RaftSnapshot
        snapshot = RaftSnapshot(
            last_included_index=boundary_index,
            last_included_term=boundary_term,
            state_machine_checksum=checksum,
            metadata_state=state_data
        )

        # 3. Persist snapshot atomically to disk (outside RaftNode lock)
        effective_base_dir = base_dir or getattr(node.storage, "base_dir", None)
        SnapshotManager.save_snapshot_atomically(node.node_id, snapshot, effective_base_dir)

        # 4. Briefly acquire RaftNode lock to update snapshot boundary and compact log suffix
        async with node.lock:
            node.compact_log(boundary_index, boundary_term)

        return snapshot


state_machine_executor = StateMachineExecutor()
