from typing import List
from fastapi import APIRouter, Depends, UploadFile, File, Request, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

import logging
from app.config import settings
from app.db.session import get_db
from app.db.models.user import User, UserRole
from app.core.dependencies import get_current_user
from app.services.file_service import FileService

logger = logging.getLogger("secure-raft-dropbox.api.files")

router = APIRouter(prefix="/api/v1/files", tags=["file-management"])


class FileResponse(BaseModel):
    id: str
    owner_id: str
    original_filename: str
    sanitized_name: str
    content_type: str
    size_bytes: int
    current_sha256: str
    current_version: int
    created_at: str
    updated_at: str


class RenameFileRequest(BaseModel):
    new_filename: str


def format_file_response(f) -> FileResponse:
    return FileResponse(
        id=f.id,
        owner_id=f.owner_id,
        original_filename=f.original_filename,
        sanitized_name=f.sanitized_name,
        content_type=f.content_type,
        size_bytes=f.size_bytes,
        current_sha256=f.current_sha256,
        current_version=f.current_version,
        created_at=f.created_at.isoformat() if f.created_at else "",
        updated_at=f.updated_at.isoformat() if f.updated_at else ""
    )


from app.raft.node import raft_node_instance, NodeRole
from app.raft.engine import raft_election_engine
from app.services.commands import FileOperationCommand, FileOperationType
from app.storage.minio_client import storage_service
from app.core.crypto import compute_sha256, sanitize_filename
import uuid


@router.post("", response_model=FileResponse, status_code=status.HTTP_201_CREATED)
async def upload_file(
    req: Request,
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    # 1. Non-Leader Write Interception (Rule 10)
    if raft_node_instance.role != NodeRole.LEADER:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "NOT_LEADER",
                "message": "Write operations must be sent to the current Raft leader.",
                "leader_id": raft_node_instance.leader_id
            }
        )

    file_bytes = await file.read()
    if len(file_bytes) > settings.MAX_UPLOAD_SIZE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File size exceeds maximum limit of {settings.MAX_UPLOAD_SIZE_BYTES // (1024*1024)}MB"
        )

    sanitized_name = sanitize_filename(file.filename)
    sha256_hash = compute_sha256(file_bytes)
    file_id = str(uuid.uuid4())
    version_number = 1
    object_key = f"users/{current_user.id}/{file_id}/v{version_number}"

    # 2. Upload Binary Payload to Object Storage (Outside Raft Log)
    storage_service.upload_object(object_key, file_bytes, file.content_type or "application/octet-stream")

    # 3. Create Metadata Command
    cmd = FileOperationCommand(
        operation=FileOperationType.UPLOAD,
        file_id=file_id,
        owner_id=current_user.id,
        filename=sanitized_name,
        sha256=sha256_hash,
        object_key=object_key,
        size_bytes=len(file_bytes),
        content_type=file.content_type or "application/octet-stream"
    )

    # 4. Append Metadata Command to Raft Log
    entry = await raft_node_instance.append_log_entry(cmd)

    # 5. Replicate across cluster & await Majority Quorum Commit AND State-Machine Application
    committed = await raft_election_engine.replicate_to_peers_and_await_applied(entry.index, timeout=5.0)

    if not committed:
        # Orphan Object Compensating Cleanup if consensus fails
        logger.error(f"Consensus failed for upload file_id={file_id}. Executing compensating MinIO cleanup.")
        storage_service.delete_object(object_key)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "CONSENSUS_FAILED",
                "message": "Failed to achieve majority consensus for file upload."
            }
        )

    # Fetch applied metadata from local DB state machine
    from app.db.models.file import FileMetadata
    file_record = db.query(FileMetadata).filter(FileMetadata.id == file_id).first()
    if not file_record:
        # Fallback if DB fetch timing
        file_record = FileService.upload_file(db, current_user, file.filename, file_bytes, file.content_type or "application/octet-stream")

    return format_file_response(file_record)


@router.get("", response_model=List[FileResponse])
def list_files(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    files = FileService.list_user_files(db=db, user=current_user)
    return [format_file_response(f) for f in files]


@router.get("/{file_id}", response_model=FileResponse)
def get_file(
    file_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    file_record = FileService.get_file_metadata(db=db, file_id=file_id, user=current_user)
    return format_file_response(file_record)


@router.get("/{file_id}/download")
def download_file(
    file_id: str,
    req: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    ip_addr = req.client.host if req.client else None
    file_record, file_bytes = FileService.download_file(
        db=db,
        file_id=file_id,
        user=current_user,
        ip_address=ip_addr
    )
    
    headers = {
        "Content-Disposition": f'attachment; filename="{file_record.sanitized_name}"'
    }
    return StreamingResponse(
        iter([file_bytes]),
        media_type=file_record.content_type,
        headers=headers
    )


@router.patch("/{file_id}", response_model=FileResponse)
async def rename_file(
    file_id: str,
    request_data: RenameFileRequest,
    req: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    # 1. Non-Leader Write Interception (Rule 10)
    if raft_node_instance.role != NodeRole.LEADER:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "NOT_LEADER",
                "message": "Write operations must be sent to the current Raft leader.",
                "leader_id": raft_node_instance.leader_id
            }
        )

    # 2. Pre-consensus authorization and existence verification
    file_record = FileService.get_file_metadata(db=db, file_id=file_id, user=current_user)

    sanitized_new = sanitize_filename(request_data.new_filename)

    # 3. Idempotent check: if already renamed to target, return immediately
    if file_record.sanitized_name == sanitized_new and file_record.original_filename == request_data.new_filename:
        return format_file_response(file_record)

    # Close initial read transaction snapshot so session db does not hold a stale snapshot across async Raft consensus
    db.commit()

    # 4. Propose RENAME command to Raft log
    cmd = FileOperationCommand(
        operation=FileOperationType.RENAME,
        file_id=file_id,
        owner_id=current_user.id,
        filename=sanitized_new,
        new_name=sanitized_new,
        metadata={"original_filename": request_data.new_filename}
    )

    entry = await raft_node_instance.append_log_entry(cmd)

    # 5. Replicate across cluster & await Majority Quorum Commit AND State-Machine Application
    applied = await raft_election_engine.replicate_to_peers_and_await_applied(entry.index, timeout=5.0)

    if not applied:
        is_committed = raft_node_instance.volatile_state.commit_index >= entry.index
        if is_committed:
            logger.warning(f"Consensus committed entry {entry.index} for rename file_id={file_id}, but local state-machine application timed out.")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "CONSENSUS_PENDING",
                    "message": "Rename committed to Raft quorum but state machine application is pending. Please retry.",
                    "file_id": file_id
                }
            )
        else:
            logger.error(f"Consensus failed/timed out for rename file_id={file_id} at index {entry.index}.")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "CONSENSUS_TIMEOUT",
                    "message": "Failed to achieve quorum consensus for file rename. Please retry."
                }
            )

    # 6. Refresh DB session snapshot and return updated record
    db.rollback()
    db.expire_all()

    from app.db.models.file import FileMetadata
    updated_record = db.query(FileMetadata).filter(FileMetadata.id == file_id).first()
    if not updated_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File record not found following state machine application."
        )

    return format_file_response(updated_record)


@router.delete("/{file_id}")
async def delete_file(
    file_id: str,
    req: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    # 1. Non-Leader Write Interception (Rule 10)
    if raft_node_instance.role != NodeRole.LEADER:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "NOT_LEADER",
                "message": "Write operations must be sent to the current Raft leader.",
                "leader_id": raft_node_instance.leader_id
            }
        )

    # 2. Pre-consensus authorization and existence verification
    from app.db.models.file import FileMetadata
    existing_file = db.query(FileMetadata).filter(FileMetadata.id == file_id).first()
    if not existing_file:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")

    # Server-side ownership / admin check
    if existing_file.owner_id != current_user.id and current_user.role != UserRole.ADMIN.value:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access to file denied")

    # 3. Idempotency check: if already deleted, return 200 OK immediately
    if existing_file.is_deleted:
        return {"message": f"File {file_id} deleted successfully"}

    # Close initial read transaction snapshot so session db does not hold a stale snapshot across async Raft consensus
    db.commit()

    # 4. Propose DELETE command to Raft log
    cmd = FileOperationCommand(
        operation=FileOperationType.DELETE,
        file_id=file_id,
        owner_id=current_user.id,
        filename=existing_file.sanitized_name
    )

    entry = await raft_node_instance.append_log_entry(cmd)

    # 5. Replicate across cluster & await Majority Quorum Commit AND State-Machine Application
    applied = await raft_election_engine.replicate_to_peers_and_await_applied(entry.index, timeout=5.0)

    if not applied:
        is_committed = raft_node_instance.volatile_state.commit_index >= entry.index
        if is_committed:
            logger.warning(f"Consensus committed entry {entry.index} for delete file_id={file_id}, but local state-machine application timed out.")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "CONSENSUS_PENDING",
                    "message": "Delete committed to Raft quorum but state machine application is pending. Please retry.",
                    "file_id": file_id
                }
            )
        else:
            logger.error(f"Consensus failed/timed out for delete file_id={file_id} at index {entry.index}.")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "CONSENSUS_TIMEOUT",
                    "message": "Failed to achieve quorum consensus for file delete. Please retry."
                }
            )

    return {"message": f"File {file_id} deleted successfully"}
