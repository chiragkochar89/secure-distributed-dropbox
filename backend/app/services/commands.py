from enum import Enum
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class FileOperationType(str, Enum):
    UPLOAD = "UPLOAD"
    DELETE = "DELETE"
    RENAME = "RENAME"
    CREATE_VERSION = "CREATE_VERSION"
    DELETE_VERSION = "DELETE_VERSION"
    REGISTER_USER = "REGISTER_USER"


class FileOperationCommand(BaseModel):
    """
    Serializable Command Object representing a mutating operation.
    In Phase 1: Dispatched directly to FileService.
    In Phase 7: Proposed to Raft consensus log before State Machine execution.
    """
    operation: FileOperationType
    file_id: str = ""
    owner_id: str = ""
    filename: str = ""
    sha256: str = ""
    object_key: str = ""
    version: int = 1
    size_bytes: int = 0
    content_type: str = "application/octet-stream"
    new_name: Optional[str] = None
    email: Optional[str] = None
    username: Optional[str] = None
    password_hash: Optional[str] = None
    role: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
