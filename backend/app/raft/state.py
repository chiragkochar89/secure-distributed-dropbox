from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from pydantic import BaseModel
from app.services.commands import FileOperationCommand


class RaftLogEntry(BaseModel):
    """
    Strongly typed Raft Log Entry.
    Contains 1-based log index, term, and serializable metadata command.
    CRITICAL: Raw file content bytes MUST NEVER be placed inside RaftLogEntry.
    """
    index: int
    term: int
    command: FileOperationCommand

    def to_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "term": self.term,
            "command": self.command.model_dump(mode="json")
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "RaftLogEntry":
        return cls(
            index=d["index"],
            term=d["term"],
            command=FileOperationCommand.model_validate(d["command"])
        )

    def to_proto(self) -> Any:
        import json
        from app.proto import raft_pb2
        meta_json = json.dumps(self.command.model_dump(mode="json"))
        return raft_pb2.LogEntry(
            index=self.index,
            term=self.term,
            operation=str(self.command.operation.value if hasattr(self.command.operation, "value") else self.command.operation),
            file_id=self.command.file_id or "",
            version=getattr(self.command, "version", 1),
            filename=getattr(self.command, "filename", "") or "",
            sha256=getattr(self.command, "sha256", "") or "",
            metadata_json=meta_json
        )

    @classmethod
    def from_proto(cls, proto: Any) -> "RaftLogEntry":
        import json
        meta_json = getattr(proto, "metadata_json", "") or ""
        cmd_dict = json.loads(meta_json) if meta_json else {}
        if not cmd_dict:
            cmd_dict = {
                "operation": proto.operation,
                "file_id": proto.file_id,
                "owner_id": getattr(proto, "owner_id", "system") or "system",
                "version": proto.version or 1,
                "filename": proto.filename or "",
                "sha256": proto.sha256 or "",
                "object_key": f"users/system/{proto.file_id}/v{proto.version or 1}"
            }
        cmd = FileOperationCommand.model_validate(cmd_dict)
        return cls(index=proto.index, term=proto.term, command=cmd)


@dataclass
class PersistentRaftState:
    """Persistent Raft state that MUST survive node restarts."""
    schema_version: int = 1
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[RaftLogEntry] = field(default_factory=list)
    last_included_index: int = 0
    last_included_term: int = 0

    @property
    def last_log_index(self) -> int:
        """Returns last log entry index (or last_included_index if log is compacted)."""
        if self.log:
            return self.log[-1].index
        return self.last_included_index

    @property
    def last_log_term(self) -> int:
        """Returns last log entry term (or last_included_term if log is compacted)."""
        if self.log:
            return self.log[-1].term
        return self.last_included_term

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "last_included_index": self.last_included_index,
            "last_included_term": self.last_included_term,
            "log": [entry.to_dict() for entry in self.log]
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PersistentRaftState":
        schema_ver = d.get("schema_version", 1)
        if schema_ver != 1:
            raise ValueError(f"Unsupported Raft state schema version: {schema_ver}")
        
        log_entries = [RaftLogEntry.from_dict(entry) for entry in d.get("log", [])]
        return cls(
            schema_version=schema_ver,
            current_term=d.get("current_term", 0),
            voted_for=d.get("voted_for"),
            last_included_index=d.get("last_included_index", 0),
            last_included_term=d.get("last_included_term", 0),
            log=log_entries
        )


@dataclass
class VolatileRaftState:
    """Volatile Raft state on all nodes (resets to 0 on restart)."""
    commit_index: int = 0
    last_applied: int = 0


@dataclass
class LeaderVolatileState:
    """Volatile Raft state maintained ONLY by active leaders."""
    next_index: Dict[str, int] = field(default_factory=dict)
    match_index: Dict[str, int] = field(default_factory=dict)
