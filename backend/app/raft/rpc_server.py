import asyncio
import time
import logging
from typing import Optional, Any
import grpc

from app.config import settings
from app.proto import raft_pb2, raft_pb2_grpc
from app.raft.node import raft_node_instance, NodeRole
from app.raft.engine import raft_election_engine
from app.raft.state import RaftLogEntry
from app.raft.state_machine import state_machine_executor

logger = logging.getLogger("secure-raft-dropbox.grpc-server")


class RaftConsensusServicer(raft_pb2_grpc.RaftConsensusServicer):
    """
    gRPC Server Servicer for Raft Peer-to-Peer Communication.
    Phase 4: Strict RequestVote RPC processing (Term monotonicity, Log up-to-date checks,
    same-term single vote rules, higher-term step-down, and election timer reset).
    """

    def __init__(self, node: Optional[Any] = None, election_engine: Optional[Any] = None):
        self._node = node
        self._election_engine = election_engine

    @property
    def node(self):
        return self._node or raft_node_instance

    @property
    def election_engine(self):
        return self._election_engine or raft_election_engine

    async def RequestVote(self, request: raft_pb2.RequestVoteRequest, context: grpc.aio.ServicerContext) -> raft_pb2.RequestVoteResponse:
        start_time = time.time()
        node = self.node

        logger.info(
            f"RPC RECEIVED [RequestVote] Node: {node.node_id} | Candidate: {request.candidate_id} | "
            f"Term: {request.term} | CandidateLastLogIndex: {request.last_log_index} | CandidateLastLogTerm: {request.last_log_term}"
        )

        async with node.lock:
            current_term = node.persistent_state.current_term

            # 1. Rule 7: Lower-Term RequestVote Handling -> Reject
            if request.term < current_term:
                logger.info(
                    f"RequestVote REJECTED on [{node.node_id}]: Candidate term ({request.term}) < Current term ({current_term})"
                )
                return raft_pb2.RequestVoteResponse(term=current_term, vote_granted=False)

            # 2. Rule 6: Higher-Term RequestVote Handling -> Step down to FOLLOWER & clear voted_for first
            if request.term > current_term:
                logger.info(
                    f"RequestVote HIGHER TERM [{request.term} > {current_term}] on [{node.node_id}]. "
                    f"Stepping down to FOLLOWER."
                )
                node.persistent_state.current_term = request.term
                node.persistent_state.voted_for = None
                node.role = NodeRole.FOLLOWER
                node.leader_state = None
                try:
                    await node.storage.save_state(node.persistent_state)
                except Exception as e:
                    logger.error(f"Failed to persist state on [{node.node_id}] during RequestVote step-down: {e}")
                    return raft_pb2.RequestVoteResponse(term=current_term, vote_granted=False)
                current_term = request.term

            # 3. Rule 8: Same-Term Voting Check
            voted_for = node.persistent_state.voted_for
            can_vote = (voted_for is None) or (voted_for == request.candidate_id)

            # 4. Rule 9: Log Up-to-Date Check
            log_is_ok = node.is_log_up_to_date(request.last_log_term, request.last_log_index)

            if can_vote and log_is_ok:
                node.persistent_state.voted_for = request.candidate_id
                node.role = NodeRole.FOLLOWER
                try:
                    await node.storage.save_state(node.persistent_state)
                except Exception as e:
                    logger.error(f"Failed to persist state on [{node.node_id}] during RequestVote grant: {e}")
                    return raft_pb2.RequestVoteResponse(term=current_term, vote_granted=False)

                # Reset election timer ONLY when a valid vote is granted! (Rule 2)
                self.election_engine.reset_election_timer()

                duration_ms = (time.time() - start_time) * 1000
                logger.info(
                    f"RequestVote GRANTED on [{node.node_id}] to Candidate [{request.candidate_id}] for Term {request.term} "
                    f"(Duration: {duration_ms:.2f}ms)"
                )
                return raft_pb2.RequestVoteResponse(term=request.term, vote_granted=True)
            
            else:
                reason = (
                    "Already voted for another candidate"
                    if not can_vote
                    else "Candidate log not up-to-date"
                )
                logger.warning(
                    f"RequestVote DENIED on [{node.node_id}] to Candidate [{request.candidate_id}] "
                    f"| Term: {request.term} | Reason: {reason} "
                    f"| VotedFor: {voted_for} | CanVote: {can_vote} | LogIsOK: {log_is_ok} "
                    f"| CandidateLog: (term={request.last_log_term}, index={request.last_log_index}) "
                    f"| LocalLog: (term={node.last_log_term}, index={node.last_log_index})"
                )
                return raft_pb2.RequestVoteResponse(
                    term=node.persistent_state.current_term,
                    vote_granted=False,
                )

               

    async def AppendEntries(self, request: raft_pb2.AppendEntriesRequest, context: grpc.aio.ServicerContext) -> raft_pb2.AppendEntriesResponse:
        start_time = time.time()
        node = self.node
        engine = self.election_engine

        logger.info(
            f"RPC RECEIVED [AppendEntries] Node: {node.node_id} | Leader: {request.leader_id} | "
            f"Term: {request.term} | PrevLogIndex: {request.prev_log_index} | PrevLogTerm: {request.prev_log_term}"
        )

        async with node.lock:
            current_term = node.persistent_state.current_term

            # 1. Lower Term Rejection (Rule 8)
            if request.term < current_term:
                logger.info(
                    f"AppendEntries REJECTED on [{node.node_id}]: Leader term ({request.term}) < Current term ({current_term})"
                )
                return raft_pb2.AppendEntriesResponse(term=current_term, success=False, match_index=0)

            # 2. Higher Term Step-Down (Rule 7)
            if request.term > current_term:
                logger.info(
                    f"AppendEntries HIGHER TERM [{request.term} > {current_term}] on [{node.node_id}]. "
                    f"Stepping down to FOLLOWER."
                )
                node.persistent_state.current_term = request.term
                node.persistent_state.voted_for = None
                node.leader_id = request.leader_id
                node.role = NodeRole.FOLLOWER
                node.leader_state = None
                try:
                    await node.storage.save_state(node.persistent_state)
                except Exception as e:
                    logger.error(f"Failed to persist state on [{node.node_id}] during AppendEntries step-down: {e}")
                    return raft_pb2.AppendEntriesResponse(term=current_term, success=False, match_index=0)
                current_term = request.term
                if engine:
                    engine.reset_election_timer()

            # 3. Same-Term Candidate Step-Down & Leader Recognition (Rule 5 & 6)
            elif request.term == current_term:
                if node.role == NodeRole.CANDIDATE:
                    logger.info(
                        f"AppendEntries SAME TERM [{request.term}] received while CANDIDATE on [{node.node_id}]. "
                        f"Stepping down to FOLLOWER."
                    )
                    node.role = NodeRole.FOLLOWER
                    node.leader_state = None
                node.leader_id = request.leader_id
                if engine:
                    engine.reset_election_timer()

            # Stop heartbeat loop if this node was previously running one
            if engine and engine.heartbeat_task and not engine.heartbeat_task.done():
                asyncio.create_task(engine.stop_heartbeat_loop())

            # 4. Log Consistency Pre-Check
            log_is_ok = node.validate_prev_log(request.prev_log_index, request.prev_log_term)
            if not log_is_ok:
                logger.info(
                    f"AppendEntries LOG MISMATCH on [{node.node_id}]: prev_log_index={request.prev_log_index}, "
                    f"prev_log_term={request.prev_log_term}"
                )
                return raft_pb2.AppendEntriesResponse(term=current_term, success=False, match_index=0)

            # 5. Process entries: Conflict Detection, Truncation, & Append
            if request.entries:
                try:
                    new_entries = [RaftLogEntry.from_proto(e) for e in request.entries]
                except Exception as err:
                    logger.error(f"Failed to deserialize entries from proto on [{node.node_id}]: {err}", exc_info=True)
                    return raft_pb2.AppendEntriesResponse(term=current_term, success=False, match_index=0)
                for entry in new_entries:
                    existing = node.get_log_entry(entry.index)
                    if existing:
                        if existing.term != entry.term:
                            # Conflict detected! Truncate log from entry.index onwards
                            node.truncate_conflicting_logs(entry.index)
                            node.persistent_state.log.append(entry)
                    else:
                        node.persistent_state.log.append(entry)

            # 6. Atomic Persistence BEFORE ACK
            try:
                await node.storage.save_state(node.persistent_state)
            except Exception as e:
                logger.error(f"Failed to persist state on [{node.node_id}] during AppendEntries: {e}")
                return raft_pb2.AppendEntriesResponse(term=current_term, success=False, match_index=0)

            # 7. Follower Commit Index Advancement & State Machine Application
            if request.leader_commit > node.volatile_state.commit_index:
                node.volatile_state.commit_index = min(request.leader_commit, node.persistent_state.last_log_index)
                asyncio.create_task(state_machine_executor.apply_committed_entries(node))

            duration_ms = (time.time() - start_time) * 1000
            last_log_idx = node.persistent_state.last_log_index
            logger.info(
                f"AppendEntries ACCEPTED on [{node.node_id}] from Leader [{request.leader_id}] for Term {request.term} "
                f"(MatchIndex: {last_log_idx}, Duration: {duration_ms:.2f}ms)"
            )
            return raft_pb2.AppendEntriesResponse(term=current_term, success=True, match_index=last_log_idx)

    async def Ping(self, request: raft_pb2.PingRequest, context: grpc.aio.ServicerContext) -> raft_pb2.PingResponse:
        return raft_pb2.PingResponse(
            node_id=self.node.node_id,
            timestamp_ms=request.timestamp_ms,
            status="ONLINE"
        )

    async def InstallSnapshot(self, request: raft_pb2.InstallSnapshotRequest, context: grpc.aio.ServicerContext) -> raft_pb2.InstallSnapshotResponse:
        start_time = time.time()
        node = self.node
        engine = self.election_engine

        logger.info(
            f"RPC RECEIVED [InstallSnapshot] Node: {node.node_id} | Leader: {request.leader_id} | "
            f"Term: {request.term} | Offset: {request.offset} | Done: {request.done} | "
            f"LastIncludedIndex: {request.last_included_index} | LastIncludedTerm: {request.last_included_term}"
        )

        async with node.lock:
            current_term = node.persistent_state.current_term

            # 1. Lower Term Rejection
            if request.term < current_term:
                logger.info(f"InstallSnapshot REJECTED on [{node.node_id}]: Leader term ({request.term}) < Current term ({current_term})")
                return raft_pb2.InstallSnapshotResponse(term=current_term)

            # 2. Higher Term Step-Down
            if request.term > current_term:
                logger.info(f"InstallSnapshot HIGHER TERM [{request.term} > {current_term}] on [{node.node_id}]. Stepping down to FOLLOWER.")
                node.persistent_state.current_term = request.term
                node.persistent_state.voted_for = None
                node.leader_id = request.leader_id
                node.role = NodeRole.FOLLOWER
                node.leader_state = None
                try:
                    await node.storage.save_state(node.persistent_state)
                except Exception as e:
                    logger.error(f"Failed to persist state on [{node.node_id}] during InstallSnapshot step-down: {e}")
                    return raft_pb2.InstallSnapshotResponse(term=current_term)
                current_term = request.term
                if engine:
                    engine.reset_election_timer()

            elif request.term == current_term:
                if node.role == NodeRole.CANDIDATE:
                    node.role = NodeRole.FOLLOWER
                    node.leader_state = None
                node.leader_id = request.leader_id
                if engine:
                    engine.reset_election_timer()

            # Temporary installation path
            from app.raft.snapshot import SnapshotManager, RaftSnapshot, restore_postgres_state_machine
            import os

            base_dir = getattr(node.storage, "base_dir", None)
            target_dir = SnapshotManager.get_snapshot_dir(node.node_id, base_dir=base_dir)
            temp_path = os.path.join(target_dir, "snapshot.installing.tmp")

            # Offset Validation
            if request.offset == 0:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            else:
                current_size = os.path.getsize(temp_path) if os.path.exists(temp_path) else 0
                if request.offset != current_size:
                    logger.warning(
                        f"InstallSnapshot INVALID OFFSET on [{node.node_id}]: Expected {current_size}, got {request.offset}"
                    )
                    return raft_pb2.InstallSnapshotResponse(term=current_term)

            # Write chunk data to temp file
            with open(temp_path, "ab") as f:
                f.write(request.data)

            # Intermediate chunk ack
            if not request.done:
                return raft_pb2.InstallSnapshotResponse(term=current_term)

            # Final Chunk Processing (done == True)
            try:
                with open(temp_path, "a+b") as f:
                    f.flush()
                    os.fsync(f.fileno())

                with open(temp_path, "r", encoding="utf-8") as f:
                    content = f.read()

                # 1. Parse complete snapshot & validate schema + state_machine_checksum
                snapshot = RaftSnapshot.from_json(content)

                # 2. Transactional PostgreSQL State Machine Restoration
                restore_postgres_state_machine(snapshot.metadata_state)

                # 3. Log Suffix Reconciliation & Persistent State Save
                matching_boundary = (
                    request.last_included_index > 0
                    and node.get_log_term_at(request.last_included_index) == request.last_included_term
                )

                if matching_boundary:
                    node.compact_log(request.last_included_index, request.last_included_term)
                else:
                    node.last_included_index = request.last_included_index
                    node.last_included_term = request.last_included_term
                    node.persistent_state.log = [
                        e for e in node.persistent_state.log if e.index > request.last_included_index
                    ]

                await node.storage.save_state(node.persistent_state)

                # 4. Monotonic Raft Volatile Index Updates
                node.volatile_state.commit_index = max(node.volatile_state.commit_index, request.last_included_index)
                node.volatile_state.last_applied = max(node.volatile_state.last_applied, request.last_included_index)

                # 5. ONLY AFTER ALL VALIDATION & RESTORATION SUCCEED: Atomically activate snapshot file
                final_path = os.path.join(target_dir, "snapshot.json")
                os.replace(temp_path, final_path)

                duration_ms = (time.time() - start_time) * 1000
                logger.info(
                    f"InstallSnapshot ACTIVATED on [{node.node_id}] at index {request.last_included_index} "
                    f"(Duration: {duration_ms:.2f}ms)"
                )
                return raft_pb2.InstallSnapshotResponse(term=current_term)

            except Exception as ex:
                logger.error(f"Failed to activate InstallSnapshot on [{node.node_id}]: {ex}", exc_info=True)
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                return raft_pb2.InstallSnapshotResponse(term=current_term)


class GRPCServerManager:
    """Async gRPC Server Manager lifecycle wrapper."""

    def __init__(self, port: Optional[int] = None, node: Optional[Any] = None, election_engine: Optional[Any] = None):
        self.port = port or settings.GRPC_PORT
        self.node = node
        self.election_engine = election_engine
        self.server: Optional[grpc.aio.Server] = None

    async def start(self):
        """Initialize and start async gRPC server."""
        self.server = grpc.aio.server()
        servicer = RaftConsensusServicer(node=self.node, election_engine=self.election_engine)
        raft_pb2_grpc.add_RaftConsensusServicer_to_server(servicer, self.server)
        listen_addr = f"[::]:{self.port}"
        self.server.add_insecure_port(listen_addr)
        await self.server.start()
        node_id = self.node.node_id if self.node else settings.NODE_ID
        logger.info(f"gRPC Consensus Server listening on {listen_addr} (Node ID: {node_id})")

    async def stop(self, grace: float = 2.0):
        """Gracefully stop async gRPC server."""
        if self.server:
            logger.info(f"Stopping gRPC Server on port {self.port} (grace: {grace}s)...")
            await self.server.stop(grace)
            logger.info(f"gRPC Server on port {self.port} stopped cleanly.")


grpc_server_manager = GRPCServerManager()
