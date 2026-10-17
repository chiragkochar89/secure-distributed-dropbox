import os
import json
import asyncio
import logging
from pathlib import Path
from typing import Optional

from app.config import settings
from app.raft.state import PersistentRaftState

logger = logging.getLogger("secure-raft-dropbox.raft-persistence")


class RaftCorruptedStateError(Exception):
    """Raised when Raft persistent state file is corrupted or unparseable."""
    pass


class RaftPersistenceError(Exception):
    """Raised when writing Raft persistent state to disk fails."""
    pass


class RaftStorageEngine:
    """
    Crash-safe Atomic Disk Persistence Engine for Raft Persistent State.
    Path: data/raft/{node_id}/state.json
    Uses temporary write -> flush -> fsync -> atomic replace sequence.
    """

    def __init__(self, node_id: Optional[str] = None, base_dir: Optional[str] = None):
        self.node_id = node_id or settings.NODE_ID
        root_dir = base_dir or os.path.join(os.getcwd(), "data", "raft")
        self.base_dir = root_dir
        self.dir_path = Path(root_dir) / self.node_id
        self.file_path = self.dir_path / "state.json"
        self.tmp_path = self.dir_path / "state.json.tmp"
        
        # Ensure node directory exists
        self.dir_path.mkdir(parents=True, exist_ok=True)

    def save_state_sync(self, state: PersistentRaftState):
        """Synchronously write persistent state atomically to disk."""
        try:
            self.dir_path.mkdir(parents=True, exist_ok=True)
            data = state.to_dict()
            json_text = json.dumps(data, indent=2)

            # 1. Write to temporary file in same directory
            with open(self.tmp_path, "w", encoding="utf-8") as f:
                f.write(json_text)
                f.flush()
                os.fsync(f.fileno())

            # 2. Atomic replace temporary file over final file
            os.replace(self.tmp_path, self.file_path)
            logger.debug(f"Saved Raft persistent state to {self.file_path}")
        except Exception as e:
            logger.error(f"Failed to persist Raft state for node {self.node_id}: {e}")
            if self.tmp_path.exists():
                try:
                    os.remove(self.tmp_path)
                except Exception:
                    pass
            raise RaftPersistenceError(f"Durable state write failed: {e}") from e

    def load_state_sync(self) -> PersistentRaftState:
        """Synchronously load persistent state from disk."""
        if not self.file_path.exists():
            logger.info(f"No existing Raft state file at {self.file_path}. Initializing clean state.")
            return PersistentRaftState()

        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                content = f.read()

            data = json.loads(content)
            state = PersistentRaftState.from_dict(data)
            logger.info(f"Loaded Raft persistent state from {self.file_path} (Term: {state.current_term}, Log Entries: {len(state.log)})")
            return state
        except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
            error_msg = f"Raft persistent state file at {self.file_path} is CORRUPTED: {e}"
            logger.critical(error_msg)
            raise RaftCorruptedStateError(error_msg) from e

    async def save_state(self, state: PersistentRaftState):
        """Async-safe state persistence running disk I/O in thread pool."""
        await asyncio.to_thread(self.save_state_sync, state)

    async def load_state(self) -> PersistentRaftState:
        """Async-safe state loading running disk I/O in thread pool."""
        return await asyncio.to_thread(self.load_state_sync)
