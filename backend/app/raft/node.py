import asyncio
import logging
from typing import Optional, List, Dict, Any, Set, Tuple

from app.config import settings
from app.raft.state import PersistentRaftState, VolatileRaftState, LeaderVolatileState, RaftLogEntry
from app.raft.persistence import RaftStorageEngine

logger = logging.getLogger("secure-raft-dropbox.raft-node")


class NodeRole(str):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


class RaftNode:
    """
    Raft Node State Machine implementation.
    Handles per-election generation ID, atomic leader promotion, log up-to-date checks,
    monotonic terms, role transitions, and thread-safe locks.
    """

    def __init__(self, node_id: Optional[str] = None, base_dir: Optional[str] = None):
        self.node_id = node_id or settings.NODE_ID
        self.persistent_state = PersistentRaftState()
        self.volatile_state = VolatileRaftState()
        if not settings.peer_list:
            self.role = NodeRole.LEADER
            self.leader_id = self.node_id
            self.leader_state = LeaderVolatileState(next_index={}, match_index={})
        else:
            self.role = NodeRole.FOLLOWER
            self.leader_id = None
            self.leader_state = None
        self.election_id: int = 0  # Monotonic per-election generation ID
        self.last_included_index: int = 0
        self.last_included_term: int = 0
        self.lock = asyncio.Lock()
        self.storage = RaftStorageEngine(node_id=self.node_id, base_dir=base_dir)

    async def initialize(self):
        """Initialize node by loading persistent state from disk."""
        async with self.lock:
            self.persistent_state = await self.storage.load_state()
            if not settings.peer_list:
                self.role = NodeRole.LEADER
                self.leader_id = self.node_id
                self.leader_state = LeaderVolatileState(next_index={}, match_index={})
            else:
                self.role = NodeRole.FOLLOWER
                self.leader_id = None
                self.leader_state = None
            self.volatile_state.commit_index = 0
            self.volatile_state.last_applied = 0
            self.election_id = 0
            
            # Load snapshot metadata to restore compaction boundary if snapshot exists
            from app.raft.snapshot import SnapshotManager
            snap = SnapshotManager.load_snapshot(self.node_id, base_dir=self.storage.base_dir)
            if snap:
                self.last_included_index = snap.last_included_index
                self.last_included_term = snap.last_included_term
                self.persistent_state.last_included_index = snap.last_included_index
                self.persistent_state.last_included_term = snap.last_included_term
                self.volatile_state.commit_index = max(self.volatile_state.commit_index, snap.last_included_index)
                self.volatile_state.last_applied = max(self.volatile_state.last_applied, snap.last_included_index)
                self.persistent_state.log = [e for e in self.persistent_state.log if e.index > snap.last_included_index]
            else:
                self.last_included_index = self.persistent_state.last_included_index
                self.last_included_term = self.persistent_state.last_included_term
                self.volatile_state.commit_index = max(self.volatile_state.commit_index, self.last_included_index)
                self.volatile_state.last_applied = max(self.volatile_state.last_applied, self.last_included_index)
                if self.last_included_index > 0:
                    self.persistent_state.log = [e for e in self.persistent_state.log if e.index > self.last_included_index]

            logger.info(
                f"Initialized RaftNode [{self.node_id}] | Role: {self.role} | "
                f"Term: {self.persistent_state.current_term} | VotedFor: {self.persistent_state.voted_for} | "
                f"LastLogIndex: {self.last_log_index}"
            )

    @property
    def last_log_index(self) -> int:
        if self.persistent_state.log:
            return self.persistent_state.log[-1].index
        return self.last_included_index

    @property
    def last_log_term(self) -> int:
        if self.persistent_state.log:
            return self.persistent_state.log[-1].term
        return self.last_included_term

    def is_log_up_to_date(self, candidate_last_log_term: int, candidate_last_log_index: int) -> bool:
        """
        Official Raft Log Up-to-Date Rule:
        A voter denies vote if candidate's log is less up-to-date than voter's own log.
        Returns True if candidate log is at least as up-to-date.
        """
        local_term = self.last_log_term
        local_index = self.last_log_index

        if candidate_last_log_term != local_term:
            return candidate_last_log_term > local_term
        return candidate_last_log_index >= local_index

    def get_log_term_at(self, index: int) -> int:
        """Return the term of the log entry at 1-based index, or 0 if index == 0 or not found."""
        if index <= 0:
            return 0
        if index == self.last_included_index:
            return self.last_included_term
        entry = self.get_log_entry(index)
        if entry:
            return entry.term
        return 0

    def get_log_entry(self, index: int) -> Optional[Any]:
        """Return entry at 1-based Raft index, or None if invalid/compacted/out of bounds."""
        if index <= 0 or index <= self.last_included_index:
            return None
        for entry in reversed(self.persistent_state.log):
            if entry.index == index:
                return entry
        return None

    def get_entries_from(self, start_index: int) -> List[Any]:
        """Return list of log entries starting from 1-based Raft index start_index."""
        if start_index <= self.last_included_index:
            start_index = self.last_included_index + 1
        return [e for e in self.persistent_state.log if e.index >= start_index]

    def compact_log(self, last_included_index: int, last_included_term: int):
        """
        Log Compaction: Truncates log entries up to last_included_index
        while preserving log suffix entries > last_included_index.
        """
        self.last_included_index = last_included_index
        self.last_included_term = last_included_term
        self.persistent_state.last_included_index = last_included_index
        self.persistent_state.last_included_term = last_included_term
        self.persistent_state.log = [e for e in self.persistent_state.log if e.index > last_included_index]
        logger.info(f"Compacted log on [{self.node_id}] up to index {last_included_index} (Term {last_included_term}). Remaining log length: {len(self.persistent_state.log)}")

    def validate_prev_log(self, prev_log_index: int, prev_log_term: int) -> bool:
        """
        Log Consistency Pre-Check:
        prev_log_index == 0 always succeeds.
        Otherwise succeeds ONLY IF entry exists at prev_log_index and term matches prev_log_term.
        """
        if prev_log_index == 0:
            return True
        term = self.get_log_term_at(prev_log_index)
        if term == 0:
            return False
        return term == prev_log_term

    def truncate_conflicting_logs(self, from_index: int):
        """Truncates conflicting log entries starting from 1-based index from_index."""
        if from_index > self.last_included_index:
            old_len = len(self.persistent_state.log)
            self.persistent_state.log = [e for e in self.persistent_state.log if e.index < from_index]
            if len(self.persistent_state.log) < old_len:
                logger.warning(
                    f"Truncating conflicting log entries on Node [{self.node_id}] from index {from_index} onwards. "
                    f"(Old length: {old_len}, New length: {len(self.persistent_state.log)})"
                )

    async def append_log_entry(self, command: Any) -> Any:
        """Appends a new metadata command to local Raft log and flushes atomically to disk."""
        async with self.lock:
            next_idx = self.last_log_index + 1
            entry = RaftLogEntry(
                index=next_idx,
                term=self.persistent_state.current_term,
                command=command
            )
            self.persistent_state.log.append(entry)
            await self.storage.save_state(self.persistent_state)
            logger.info(f"Appended log entry at Index {next_idx} (Term {entry.term}) on Node [{self.node_id}]")
            return entry

    async def update_term_if_higher(self, new_term: int) -> bool:
        """
        Enforces Higher-Term Rule:
        If new_term > current_term:
            current_term = new_term
            voted_for = None
            leader_id = None
            role = FOLLOWER
            persist to disk
            returns True
        Otherwise returns False.
        """
        async with self.lock:
            if new_term > self.persistent_state.current_term:
                logger.info(
                    f"Higher term discovered on Node [{self.node_id}]: {self.persistent_state.current_term} -> {new_term}. "
                    f"Stepping down to FOLLOWER."
                )
                self.persistent_state.current_term = new_term
                self.persistent_state.voted_for = None
                self.leader_id = None
                self.role = NodeRole.FOLLOWER
                self.leader_state = None
                await self.storage.save_state(self.persistent_state)
                return True
            return False

    async def transition_to_follower(self, term: int, voted_for: Optional[str] = None, leader_id: Optional[str] = None):
        """
        Transition node to FOLLOWER role.
        Preserves voted_for if term == current_term and voted_for arg is None.
        Sets leader_id if provided.
        """
        async with self.lock:
            if term > self.persistent_state.current_term:
                self.persistent_state.current_term = term
                self.persistent_state.voted_for = voted_for
                self.leader_id = leader_id
            elif term == self.persistent_state.current_term:
                if voted_for is not None:
                    self.persistent_state.voted_for = voted_for
                if leader_id is not None:
                    self.leader_id = leader_id
            else:
                return

            self.role = NodeRole.FOLLOWER
            self.leader_state = None
            await self.storage.save_state(self.persistent_state)
            logger.info(
                f"Node [{self.node_id}] transitioned to FOLLOWER (Term: {self.persistent_state.current_term}, "
                f"VotedFor: {self.persistent_state.voted_for}, LeaderId: {self.leader_id})"
            )

    async def transition_to_candidate(self) -> Tuple[int, int]:
        """
        Transition node to CANDIDATE role for election.
        Increments current_term, votes for self, flushes to disk, and increments election_id.
        Invalidates leader_id.
        Returns (current_term, election_id).
        """
        async with self.lock:
            self.persistent_state.current_term += 1
            self.persistent_state.voted_for = self.node_id
            self.leader_id = None
            self.role = NodeRole.CANDIDATE
            self.election_id += 1
            self.leader_state = None
            await self.storage.save_state(self.persistent_state)
            logger.info(
                f"Node [{self.node_id}] transitioned to CANDIDATE (Term: {self.persistent_state.current_term}, "
                f"Election ID: {self.election_id})"
            )
            return self.persistent_state.current_term, self.election_id

    async def transition_to_leader(self, peers: Optional[List[str]] = None) -> None:
        """
        Transition node directly to LEADER role (used for testing/manual transition).
        """
        async with self.lock:
            self.role = NodeRole.LEADER
            self.leader_id = self.node_id
            peer_list = peers or []
            last_idx = self.persistent_state.last_log_index
            next_idx = {p: last_idx + 1 for p in peer_list}
            match_idx = {p: 0 for p in peer_list}
            self.leader_state = LeaderVolatileState(next_index=next_idx, match_index=match_idx)
            logger.info(f"Node [{self.node_id}] transitioned to LEADER (manual/test)")

    async def atomic_leader_promotion(
        self,
        election_term: int,
        election_id: int,
        unique_votes: Set[str],
        majority: int,
        peers: List[str]
    ) -> bool:
        """
        Atomic Leader Promotion:
        Verifies role == CANDIDATE, current_term == election_term, election_id == current_election_id,
        and len(unique_votes) >= majority under state lock before promoting to LEADER.
        """
        async with self.lock:
            if (
                self.role == NodeRole.CANDIDATE
                and self.persistent_state.current_term == election_term
                and self.election_id == election_id
                and len(unique_votes) >= majority
            ):
                self.role = NodeRole.LEADER
                self.leader_id = self.node_id
                last_idx = self.persistent_state.last_log_index
                next_idx = {peer: last_idx + 1 for peer in peers}
                match_idx = {peer: 0 for peer in peers}
                self.leader_state = LeaderVolatileState(next_index=next_idx, match_index=match_idx)
                logger.info(
                    f"PROMOTED TO LEADER! Node [{self.node_id}] won election (Term: {election_term}, "
                    f"Election ID: {election_id}, Votes: {len(unique_votes)}/{majority})"
                )
                return True
            else:
                logger.warning(
                    f"Leader promotion REJECTED on Node [{self.node_id}]: State changed or stale election. "
                    f"(Role: {self.role}, Term: {self.persistent_state.current_term}, Election ID: {self.election_id})"
                )
                return False

    def get_status(self) -> Dict[str, Any]:
        """Return node status summary dictionary."""
        return {
            "node_id": self.node_id,
            "role": self.role,
            "current_term": self.persistent_state.current_term,
            "voted_for": self.persistent_state.voted_for,
            "leader_id": self.leader_id,
            "election_id": self.election_id,
            "last_log_index": self.persistent_state.last_log_index,
            "last_log_term": self.persistent_state.last_log_term,
            "commit_index": self.volatile_state.commit_index,
            "last_applied": self.volatile_state.last_applied,
            "log_length": len(self.persistent_state.log),
            "is_leader": self.role == NodeRole.LEADER
        }


raft_node_instance = RaftNode()
