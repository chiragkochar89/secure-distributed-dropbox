import random
import asyncio
import logging
from typing import Optional, List, Set, Dict

from app.config import settings
from app.proto import raft_pb2
from app.raft.node import RaftNode, NodeRole, raft_node_instance
from app.raft.rpc_client import RaftPeerClientPool, peer_client_pool
from app.raft.state_machine import state_machine_executor

logger = logging.getLogger("secure-raft-dropbox.raft-election")


class RaftElectionEngine:
    """
    Raft Leader Election Engine.
    Manages randomized election timeouts (150-300ms), single-active timer tasks,
    candidate vote broadcasting, unique vote set collection, stale election filtering,
    and atomic leader promotion.
    """

    def __init__(
        self,
        node: Optional[RaftNode] = None,
        peer_pool: Optional[RaftPeerClientPool] = None,
        min_timeout_ms: Optional[int] = None,
        max_timeout_ms: Optional[int] = None,
        heartbeat_interval_ms: Optional[int] = None
    ):
        self.node = node or raft_node_instance
        self.peer_pool = peer_pool or peer_client_pool
        self.min_timeout_ms = min_timeout_ms or settings.ELECTION_TIMEOUT_MIN_MS
        self.max_timeout_ms = max_timeout_ms or settings.ELECTION_TIMEOUT_MAX_MS
        self.timer_task: Optional[asyncio.Task] = None
        self.heartbeat_task: Optional[asyncio.Task] = None
        self._reset_event = asyncio.Event()
        self.is_running = False
        self.heartbeat_interval_ms = heartbeat_interval_ms or min(self.min_timeout_ms // 3, settings.HEARTBEAT_INTERVAL_MS)

    def get_random_timeout_seconds(self) -> float:
        """Generate a NEW random election timeout per election cycle."""
        ms = random.randint(self.min_timeout_ms, self.max_timeout_ms)
        return ms / 1000.0

    def reset_election_timer(self):
        """Reset the current election timer."""
        self._reset_event.set()

    async def start(self):
        """Start the election timer background task. Ensures single active timer loop."""
        if self.is_running:
            return
        self.is_running = True
        if self.timer_task and not self.timer_task.done():
            self.timer_task.cancel()
        self.timer_task = asyncio.create_task(self._election_timer_loop())
        logger.info(f"Election Engine started on Node [{self.node.node_id}] (Range: {self.min_timeout_ms}-{self.max_timeout_ms}ms)")

    async def stop(self):
        """Stop and cancel the election timer & heartbeat background tasks."""
        self.is_running = False
        await self.stop_heartbeat_loop()
        if self.timer_task and not self.timer_task.done():
            self.timer_task.cancel()
            try:
                await self.timer_task
            except asyncio.CancelledError:
                pass
        logger.info(f"Election Engine stopped on Node [{self.node.node_id}].")

    async def start_heartbeat_loop(self):
        """Start the leader heartbeat loop. Ensures at most one active heartbeat task."""
        if self.heartbeat_task and not self.heartbeat_task.done():
            return
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info(f"Leader heartbeat loop started on Node [{self.node.node_id}] (Interval: {self.heartbeat_interval_ms}ms)")
        try:
            from app.services.auditor import replica_auditor
            replica_auditor.node = self.node
            replica_auditor.start()
        except Exception as e:
            logger.warning(f"Could not start replica auditor on Node [{self.node.node_id}]: {e}")

    async def stop_heartbeat_loop(self):
        """Stop and cancel the leader heartbeat loop cleanly."""
        if self.heartbeat_task and not self.heartbeat_task.done():
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
            logger.info(f"Leader heartbeat loop stopped on Node [{self.node.node_id}]")
        try:
            from app.services.auditor import replica_auditor
            await replica_auditor.stop()
        except Exception as e:
            logger.warning(f"Could not stop replica auditor on Node [{self.node.node_id}]: {e}")

    async def _heartbeat_loop(self):
        """Background heartbeat loop sending AppendEntries heartbeats to peers every HEARTBEAT_INTERVAL_MS."""
        while self.is_running and self.node.role == NodeRole.LEADER:
            current_term = self.node.persistent_state.current_term
            peer_addrs = self.peer_pool.peer_addresses

            tasks = []
            peer_clients = []
            for peer_addr in peer_addrs:
                # Per-peer prev_log_index / prev_log_term construction
                next_idx = (
                    self.node.leader_state.next_index.get(peer_addr, self.node.persistent_state.last_log_index + 1)
                    if self.node.leader_state
                    else self.node.persistent_state.last_log_index + 1
                )
                last_log_idx = self.node.persistent_state.last_log_index

                # Trigger log catch-up replication if peer is lagging
                if self.node.leader_state and next_idx <= last_log_idx:
                    asyncio.create_task(self._replicate_to_peer(peer_addr, last_log_idx))
                    continue

                prev_log_idx = max(0, next_idx - 1)
                prev_log_term = self.node.get_log_term_at(prev_log_idx)

                req = raft_pb2.AppendEntriesRequest(
                    term=current_term,
                    leader_id=self.node.node_id,
                    prev_log_index=prev_log_idx,
                    prev_log_term=prev_log_term,
                    entries=[],
                    leader_commit=self.node.volatile_state.commit_index
                )
                client = self.peer_pool.get_client(peer_addr)
                peer_clients.append((peer_addr, client))
                tasks.append(client.send_append_entries(req, timeout=0.25))

            if tasks:
                responses = await asyncio.gather(*tasks, return_exceptions=True)
                for (peer_addr, client), res in zip(peer_clients, responses):
                    if isinstance(res, raft_pb2.AppendEntriesResponse):
                        # Stale response protection: verify still leader & same term
                        if self.node.role == NodeRole.LEADER and self.node.persistent_state.current_term == current_term:
                            if res.term > current_term:
                                logger.info(
                                    f"Discovered higher term {res.term} from peer {peer_addr} during heartbeat. "
                                    f"Leader stepping down."
                                )
                                await self.node.update_term_if_higher(res.term)
                                self.reset_election_timer()
                                break
                            elif not res.success and self.node.leader_state:
                                # Log mismatch on peer during heartbeat: decrement next_index to trigger catch-up
                                curr_next = self.node.leader_state.next_index.get(peer_addr, last_log_idx + 1)
                                self.node.leader_state.next_index[peer_addr] = max(1, curr_next - 1)

            await asyncio.sleep(self.heartbeat_interval_ms / 1000.0)

    async def _check_and_advance_commit_index(self):
        """
        Leader Quorum Commit Calculation:
        Finds highest index N > commit_index such that:
        1. log[N].term == current_term (Current-term commitment restriction)
        2. A majority of nodes (including leader) have match_index[peer] >= N.
        If found, advances commit_index to N and triggers state machine application.
        """
        async with self.node.lock:
            if self.node.role != NodeRole.LEADER or not self.node.leader_state:
                return

            current_term = self.node.persistent_state.current_term
            commit_idx = self.node.volatile_state.commit_index
            last_log_idx = self.node.persistent_state.last_log_index
            peer_addrs = self.peer_pool.peer_addresses
            majority = (len(peer_addrs) + 1) // 2 + 1

            for N in range(last_log_idx, commit_idx, -1):
                entry = self.node.get_log_entry(N)
                if not entry or entry.term != current_term:
                    continue

                match_count = 1  # Leader counts itself
                for peer in peer_addrs:
                    if self.node.leader_state.match_index.get(peer, 0) >= N:
                        match_count += 1

                if match_count >= majority:
                    logger.info(
                        f"QUORUM COMMIT REACHED! Node [{self.node.node_id}] advancing commit_index "
                        f"{commit_idx} -> {N} (Term {current_term}, Quorum {match_count}/{majority})"
                    )
                    self.node.volatile_state.commit_index = N
                    break

        try:
            await state_machine_executor.apply_committed_entries(self.node)
        except Exception as ex:
            logger.error(f"EXCEPTION IN APPLY COMMITTED ENTRIES: {ex}", exc_info=True)
            raise ex

    async def _send_install_snapshot(self, peer_addr: str) -> bool:
        """Sends sequential 64 KB chunked unary InstallSnapshot RPC calls to a lagging peer."""
        from app.raft.snapshot import SnapshotManager
        base_dir = getattr(self.node.storage, "base_dir", None)
        snapshot = SnapshotManager.load_snapshot(self.node.node_id, base_dir=base_dir)
        if not snapshot:
            logger.error(f"Cannot send InstallSnapshot to {peer_addr}: No active snapshot found on Leader [{self.node.node_id}]")
            return False

        json_bytes = snapshot.to_json().encode("utf-8")
        chunk_size = 64 * 1024
        total_size = len(json_bytes)
        offset = 0

        client = self.peer_pool.get_client(peer_addr)

        while offset < total_size and self.node.role == NodeRole.LEADER:
            chunk = json_bytes[offset:offset + chunk_size]
            done = (offset + len(chunk)) >= total_size
            current_term = self.node.persistent_state.current_term

            req = raft_pb2.InstallSnapshotRequest(
                term=current_term,
                leader_id=self.node.node_id,
                last_included_index=snapshot.last_included_index,
                last_included_term=snapshot.last_included_term,
                offset=offset,
                data=chunk,
                done=done
            )

            res = await client.send_install_snapshot(req, timeout=5.0)

            if res is None:
                logger.warning(f"InstallSnapshot RPC timeout to peer {peer_addr} at offset {offset}")
                return False

            if res.term > current_term:
                logger.info(f"Discovered higher term {res.term} from peer {peer_addr} during InstallSnapshot. Stepping down.")
                await self.node.update_term_if_higher(res.term)
                await self.stop_heartbeat_loop()
                self.reset_election_timer()
                return False

            offset += len(chunk)

        if self.node.role == NodeRole.LEADER:
            if self.node.leader_state:
                self.node.leader_state.match_index[peer_addr] = snapshot.last_included_index
                self.node.leader_state.next_index[peer_addr] = snapshot.last_included_index + 1
                logger.info(
                    f"InstallSnapshot to {peer_addr} completed successfully. "
                    f"Updated match_index={snapshot.last_included_index}, next_index={snapshot.last_included_index + 1}"
                )
                await self._check_and_advance_commit_index()
            return True

        return False

    async def _replicate_to_peer(self, peer_addr: str, target_index: int, max_retries: int = 3):
        """Controlled non-recursive loop replicating log entries or snapshots to a single peer until target_index is reached or retries exhausted."""
        try:
            consecutive_failures = 0
            while self.node.role == NodeRole.LEADER:
                current_term = self.node.persistent_state.current_term
                next_idx = (
                    self.node.leader_state.next_index.get(peer_addr, self.node.last_log_index + 1)
                    if self.node.leader_state
                    else self.node.last_log_index + 1
                )

                if self.node.leader_state and self.node.leader_state.match_index.get(peer_addr, 0) >= target_index:
                    break

                # Check if required entries have been compacted
                if self.node.last_included_index > 0 and next_idx <= self.node.last_included_index:
                    logger.info(f"Peer {peer_addr} next_index ({next_idx}) <= leader last_included_index ({self.node.last_included_index}). Initiating InstallSnapshot.")
                    success = await self._send_install_snapshot(peer_addr)
                    if not success or self.node.role != NodeRole.LEADER:
                        consecutive_failures += 1
                        logger.warning(f"InstallSnapshot RPC to peer {peer_addr} failed ({consecutive_failures}/{max_retries}).")
                        if consecutive_failures >= max_retries:
                            logger.error(f"Abandoning InstallSnapshot replication loop to unavailable peer {peer_addr} after {consecutive_failures} failures.")
                            break
                        await asyncio.sleep(min(0.5, 0.05 * (2 ** (consecutive_failures - 1))))
                        continue
                    consecutive_failures = 0
                    continue

                prev_log_idx = max(0, next_idx - 1)
                prev_log_term = self.node.get_log_term_at(prev_log_idx)
                entries = self.node.get_entries_from(next_idx)

                req = raft_pb2.AppendEntriesRequest(
                    term=current_term,
                    leader_id=self.node.node_id,
                    prev_log_index=prev_log_idx,
                    prev_log_term=prev_log_term,
                    entries=[e.to_proto() for e in entries],
                    leader_commit=self.node.volatile_state.commit_index
                )

                client = self.peer_pool.get_client(peer_addr)
                res = await client.send_append_entries(req, timeout=0.5)

                if self.node.role != NodeRole.LEADER:
                    break

                if res is None:
                    consecutive_failures += 1
                    logger.warning(f"Replication RPC to peer {peer_addr} failed/timed out ({consecutive_failures}/{max_retries}). NextIndex: {next_idx}")
                    if consecutive_failures >= max_retries:
                        logger.error(f"Abandoning replication loop to unavailable peer {peer_addr} after {consecutive_failures} consecutive failures.")
                        break
                    await asyncio.sleep(min(0.5, 0.05 * (2 ** (consecutive_failures - 1))))
                    continue

                consecutive_failures = 0

                if res.term > current_term:
                    logger.info(f"Discovered higher term {res.term} from peer {peer_addr} during replication. Stepping down.")
                    await self.node.update_term_if_higher(res.term)
                    await self.stop_heartbeat_loop()
                    self.reset_election_timer()
                    break

                if res.success:
                    if self.node.leader_state:
                        self.node.leader_state.match_index[peer_addr] = res.match_index
                        self.node.leader_state.next_index[peer_addr] = res.match_index + 1
                    await self._check_and_advance_commit_index()
                    if res.match_index >= target_index:
                        break
                else:
                    if self.node.leader_state:
                        self.node.leader_state.next_index[peer_addr] = max(1, next_idx - 1)
        except Exception as err:
            logger.error(f"UNHANDLED EXCEPTION IN _replicate_to_peer for {peer_addr}: {err}", exc_info=True)
            raise err

    async def replicate_to_peers_and_await_quorum(self, target_index: int, timeout: float = 5.0) -> bool:
        """Broadcast log entries to all peers concurrently and await majority quorum commit."""
        if self.node.role != NodeRole.LEADER:
            return False

        peer_addrs = self.peer_pool.peer_addresses
        if not peer_addrs:
            # Single node cluster case
            await self._check_and_advance_commit_index()
            return self.node.volatile_state.commit_index >= target_index

        tasks = [asyncio.create_task(self._replicate_to_peer(peer_addr, target_index)) for peer_addr in peer_addrs]

        start_time = asyncio.get_event_loop().time()
        while (asyncio.get_event_loop().time() - start_time) < timeout:
            if self.node.volatile_state.commit_index >= target_index:
                return True
            if self.node.role != NodeRole.LEADER:
                return False
            await asyncio.sleep(0.01)

        return self.node.volatile_state.commit_index >= target_index

    async def replicate_to_peers_and_await_applied(self, target_index: int, timeout: float = 5.0) -> bool:
        """
        Broadcast log entries to all peers concurrently, await majority quorum commit,
        AND await local state-machine application (last_applied >= target_index).
        Used by client-facing HTTP write operations to guarantee state-machine persistence before response.
        """
        committed = await self.replicate_to_peers_and_await_quorum(target_index, timeout=timeout)
        if not committed:
            return False

        start_time = asyncio.get_event_loop().time()
        while (asyncio.get_event_loop().time() - start_time) < timeout:
            if self.node.volatile_state.last_applied >= target_index:
                return True
            if self.node.role != NodeRole.LEADER:
                return False
            await asyncio.sleep(0.01)

        return self.node.volatile_state.last_applied >= target_index

    async def _election_timer_loop(self):
        """Background election timer loop."""
        while self.is_running:
            timeout = self.get_random_timeout_seconds()
            self._reset_event.clear()

            try:
                # Wait for timeout OR timer reset signal
                await asyncio.wait_for(self._reset_event.wait(), timeout=timeout)
                # If _reset_event was set, loop restarts with a new timer!
            except asyncio.TimeoutError:
                # Timer expired naturally! If not LEADER, trigger election
                if self.is_running and self.node.role != NodeRole.LEADER:
                    logger.info(f"Election timeout expired on Node [{self.node.node_id}] ({timeout*1000:.1f}ms). Starting election...")
                    await self.start_election()
            except asyncio.CancelledError:
                break

    async def start_election(self):
        """
        Execute Candidate election flow:
        1. Transition to Candidate (term += 1, voted_for = self, election_id += 1, persist state).
        2. Reset election timer.
        3. Broadcast RequestVote RPCs concurrently to peers.
        4. Collect unique votes (set preventing duplicate counting).
        5. Filter stale responses (election_id & term checks).
        6. Attempt atomic leader promotion if majority reached.
        """
        # Step 1: Transition to Candidate
        election_term, election_id = await self.node.transition_to_candidate()
        self.reset_election_timer()

        peer_addrs = self.peer_pool.peer_addresses
        cluster_size = len(peer_addrs) + 1
        majority = cluster_size // 2 + 1  # Dynamic majority (e.g., 2 for 3 nodes)

        # Unique vote collection set (Self vote = 1)
        votes_received: Set[str] = {self.node.node_id}

        last_idx = self.node.last_log_index
        last_term = self.node.last_log_term

        req = raft_pb2.RequestVoteRequest(
            term=election_term,
            candidate_id=self.node.node_id,
            last_log_index=last_idx,
            last_log_term=last_term
        )

        logger.info(
            f"ELECTION STARTED on Node [{self.node.node_id}] | Term: {election_term} | "
            f"Election ID: {election_id} | Quorum Target: {majority}/{cluster_size}"
        )

        # Broadcast RequestVote concurrently without holding state lock
        peer_clients = []
        for peer_addr in peer_addrs:
            client = self.peer_pool.get_client(peer_addr)
            peer_clients.append((peer_addr, client))

        if not peer_clients:
            # Single-node cluster case
            promoted = await self.node.atomic_leader_promotion(election_term, election_id, votes_received, majority, peer_addrs)
            if promoted:
                await self.start_heartbeat_loop()
            return

        # Collect votes with early quorum breakout (don't stall on unreachable peers)
        pending = {
            asyncio.create_task(client.send_request_vote(req, timeout=1.0)): peer_addr
            for peer_addr, client in peer_clients
        }

        while pending:
            done, _ = await asyncio.wait(pending.keys(), return_when=asyncio.FIRST_COMPLETED)
            for fut in done:
                peer_addr = pending.pop(fut)
                try:
                    res = fut.result()
                    if isinstance(res, raft_pb2.RequestVoteResponse):
                        # Higher-term response check -> step down immediately
                        if res.term > election_term:
                            logger.info(f"Discovered higher term {res.term} from peer {peer_addr} during election. Stepping down.")
                            await self.node.update_term_if_higher(res.term)
                            for rem in pending:
                                rem.cancel()
                            return

                        # Validate response is for THIS term & election
                        if res.term == election_term and res.vote_granted:
                            votes_received.add(peer_addr)
                            logger.info(f"Vote granted by peer {peer_addr} to [{self.node.node_id}] for Term {election_term} (Total votes: {len(votes_received)})")
                except Exception:
                    pass

            # Early quorum termination check
            if len(votes_received) >= majority:
                for rem in pending:
                    rem.cancel()
                break

        # Atomic Leader Promotion Check
        promoted = await self.node.atomic_leader_promotion(
            election_term=election_term,
            election_id=election_id,
            unique_votes=votes_received,
            majority=majority,
            peers=peer_addrs
        )
        if promoted:
            await self.start_heartbeat_loop()


raft_election_engine = RaftElectionEngine()
