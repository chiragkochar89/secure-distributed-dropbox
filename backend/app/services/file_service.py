import uuid
import logging
from typing import List, Optional
from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from app.config import settings
from app.db.models.user import User, UserRole
from app.db.models.file import FileMetadata, FileVersion
from app.core.crypto import compute_sha256, sanitize_filename
from app.core.audit import log_audit_event
from app.storage.minio_client import storage_service
from app.services.commands import FileOperationCommand, FileOperationType

logger = logging.getLogger(__name__)


class FileService:
    """File Management Service handling upload, download, list, rename, and delete."""

    @staticmethod
    def upload_file(
        db: Session,
        user: User,
        filename: str,
        file_bytes: bytes,
        content_type: str = "application/octet-stream",
        ip_address: Optional[str] = None
    ) -> FileMetadata:
        # 1. Size limit validation
        if len(file_bytes) > settings.MAX_UPLOAD_SIZE_BYTES:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"File size exceeds maximum limit of {settings.MAX_UPLOAD_SIZE_BYTES // (1024*1024)}MB"
            )

        # 2. Filename sanitization
        sanitized_name = sanitize_filename(filename)
        sha256_hash = compute_sha256(file_bytes)
        file_id = str(uuid.uuid4())
        version_number = 1
        object_key = f"users/{user.id}/{file_id}/v{version_number}"

        # 3. Create Command Object
        cmd = FileOperationCommand(
            operation=FileOperationType.UPLOAD,
            file_id=file_id,
            owner_id=user.id,
            filename=sanitized_name,
            sha256=sha256_hash,
            object_key=object_key,
            size_bytes=len(file_bytes),
            content_type=content_type,
        )

        # 4. Upload to MinIO object storage
        storage_service.upload_object(object_key, file_bytes, content_type)

        # 5. DB Metadata Persistence with COMPENSATING CLEANUP on failure
        try:
            file_record = FileMetadata(
                id=cmd.file_id,
                owner_id=cmd.owner_id,
                original_filename=filename,
                sanitized_name=cmd.filename,
                content_type=cmd.content_type,
                size_bytes=cmd.size_bytes,
                current_sha256=cmd.sha256,
                current_version=version_number,
                is_deleted=False
            )
            version_record = FileVersion(
                id=str(uuid.uuid4()),
                file_id=cmd.file_id,
                version_number=version_number,
                object_key=cmd.object_key,
                size_bytes=cmd.size_bytes,
                sha256_hash=cmd.sha256
            )
            db.add(file_record)
            db.add(version_record)
            db.commit()
            db.refresh(file_record)
        except Exception as e:
            # COMPENSATING CLEANUP: Remove orphaned MinIO object if DB transaction fails
            logger.error(f"DB transaction failed during file upload ({file_id}): {e}. Executing compensating MinIO cleanup.")
            db.rollback()
            storage_service.delete_object(object_key)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database persistence error during file upload. Cleaned up storage."
            )

        # 6. Record Audit Event
        log_audit_event(
            db=db,
            event_type="FILE_UPLOADED",
            user_id=user.id,
            resource_id=file_id,
            ip_address=ip_address,
            details={"filename": cmd.filename, "size": cmd.size_bytes, "sha256": cmd.sha256}
        )

        return file_record

    @staticmethod
    def list_user_files(db: Session, user: User) -> List[FileMetadata]:
        """List active files owned by the user (or all files if admin)."""
        if user.role == UserRole.ADMIN.value:
            return db.query(FileMetadata).filter(FileMetadata.is_deleted == False).all()
        return db.query(FileMetadata).filter(
            FileMetadata.owner_id == user.id,
            FileMetadata.is_deleted == False
        ).all()

    @staticmethod
    def get_file_metadata(db: Session, file_id: str, user: User) -> FileMetadata:
        """Get file metadata with server-side ownership verification."""
        file_record = db.query(FileMetadata).filter(
            FileMetadata.id == file_id,
            FileMetadata.is_deleted == False
        ).first()

        if not file_record:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")

        # Server-side ownership check
        if file_record.owner_id != user.id and user.role != UserRole.ADMIN.value:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access to file denied")

        return file_record

    @staticmethod
    def download_file(db: Session, file_id: str, user: User, ip_address: Optional[str] = None) -> tuple[FileMetadata, bytes]:
        """Download raw file bytes from MinIO after ownership check."""
        file_record = FileService.get_file_metadata(db, file_id, user)
        
        # Get current version object key
        current_v = db.query(FileVersion).filter(
            FileVersion.file_id == file_id,
            FileVersion.version_number == file_record.current_version
        ).first()

        if not current_v:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File version payload missing")

        file_bytes = storage_service.download_object(current_v.object_key)

        log_audit_event(
            db=db,
            event_type="FILE_DOWNLOADED",
            user_id=user.id,
            resource_id=file_id,
            ip_address=ip_address,
            details={"filename": file_record.sanitized_name, "size": len(file_bytes)}
        )

        return file_record, file_bytes

    @staticmethod
    def rename_file(db: Session, file_id: str, new_filename: str, user: User, ip_address: Optional[str] = None) -> FileMetadata:
        """Rename file metadata after ownership check."""
        file_record = FileService.get_file_metadata(db, file_id, user)
        old_name = file_record.sanitized_name
        sanitized_new = sanitize_filename(new_filename)

        file_record.original_filename = new_filename
        file_record.sanitized_name = sanitized_new
        db.commit()
        db.refresh(file_record)

        log_audit_event(
            db=db,
            event_type="FILE_RENAMED",
            user_id=user.id,
            resource_id=file_id,
            ip_address=ip_address,
            details={"old_name": old_name, "new_name": sanitized_new}
        )

        return file_record

    @staticmethod
    def delete_file(db: Session, file_id: str, user: User, ip_address: Optional[str] = None) -> bool:
        """
        Delete Semantics:
        1. Server-side ownership verification.
        2. Mark metadata is_deleted = True.
        3. Remove corresponding MinIO object safely.
        4. Audit event FILE_DELETED.
        """
        file_record = FileService.get_file_metadata(db, file_id, user)

        # Mark metadata soft-deleted
        file_record.is_deleted = True
        
        # Safely remove file object payloads from MinIO
        versions = db.query(FileVersion).filter(FileVersion.file_id == file_id).all()
        for v in versions:
            storage_service.delete_object(v.object_key)

        db.commit()

        log_audit_event(
            db=db,
            event_type="FILE_DELETED",
            user_id=user.id,
            resource_id=file_id,
            ip_address=ip_address,
            details={"filename": file_record.sanitized_name}
        )

        return True
