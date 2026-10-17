import os
import shutil
import tempfile
import pytest
import asyncio
from unittest.mock import patch, AsyncMock

from app.db.session import SessionLocal, init_db
from app.db.models.user import User, UserRole
from app.db.models.file import FileMetadata, FileVersion, AppliedCommand
from app.db.models.audit import AuditLog
from app.proto import raft_pb2
from app.services.commands import FileOperationCommand, FileOperationType
from app.raft.node import RaftNode, NodeRole
from app.raft.state import RaftLogEntry, LeaderVolatileState
from app.raft.state_machine import state_machine_executor
from app.raft.engine import RaftElectionEngine
from app.raft.rpc_server import RaftConsensusServicer
from app.raft.rpc_client import RaftPeerClientPool
from app.raft.snapshot import SnapshotManager, RaftSnapshot


@pytest.fixture(autouse=True)
def clean_env():
    """Ensure clean database and storage environment before each test."""
    init_db()
    db = SessionLocal()
    try:
        db.query(AppliedCommand).delete()
        db.query(AuditLog).delete()
        db.query(FileVersion).delete()
        db.query(FileMetadata).delete()
        db.query(User).delete()
        db.commit()
    finally:
        db.close()

    storage_dir = os.path.join(os.getcwd(), "data", "storage")
    raft_dir = os.path.join(os.getcwd(), "data", "raft")
    for d in [storage_dir, raft_dir]:
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d, exist_ok=True)

    yield

    for d in [storage_dir, raft_dir]:
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)


# ==============================================================================
# INVARIANT 1: At most one vote granted per term
# ==============================================================================
@pytest.mark.asyncio
async def test_invariant_single_vote_per_term():
    """Verify that a node grants at most one vote per term (Raft §5.2)."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        node = RaftNode(node_id="node-1", base_dir=tmp_dir)
        await node.initialize()
        node.persistent_state.current_term = 1
        servicer = RaftConsensusServicer(node=node)

        # 1. Candidate A requests vote in Term 1 -> GRANTED
        req_a = raft_pb2.RequestVoteRequest(
            term=1,
            candidate_id="node-2",
            last_log_index=0,
            last_log_term=0
        )
        res_a = await servicer.RequestVote(req_a, None)
        assert res_a.vote_granted is True
        assert res_a.term == 1
        assert node.persistent_state.voted_for == "node-2"

        # 2. Candidate B requests vote in same Term 1 -> DENIED
        req_b = raft_pb2.RequestVoteRequest(
            term=1,
            candidate_id="node-3",
            last_log_index=0,
            last_log_term=0
        )
        res_b = await servicer.RequestVote(req_b, None)
        assert res_b.vote_granted is False
        assert node.persistent_state.voted_for == "node-2"  # Unchanged

        # 3. Candidate A repeats request in same Term 1 -> GRANTED (idempotent re-grant)
        res_a_repeat = await servicer.RequestVote(req_a, None)
        assert res_a_repeat.vote_granted is True


# ==============================================================================
# INVARIANT 2: Term monotonicity & stale-term rejection
# ==============================================================================
@pytest.mark.asyncio
async def test_invariant_term_monotonicity_and_stale_message_rejection():
    """Verify that terms strictly increase monotonically and stale messages are rejected (Raft §5.1)."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        node = RaftNode(node_id="node-1", base_dir=tmp_dir)
        await node.initialize()
        node.persistent_state.current_term = 5
        servicer = RaftConsensusServicer(node=node)

        # Stale RequestVote (term 4 < current 5)
        rv_req = raft_pb2.RequestVoteRequest(term=4, candidate_id="node-2", last_log_index=0, last_log_term=0)
        rv_res = await servicer.RequestVote(rv_req, None)
        assert rv_res.vote_granted is False
        assert rv_res.term == 5
        assert node.persistent_state.current_term == 5

        # Stale AppendEntries (term 4 < current 5)
        ae_req = raft_pb2.AppendEntriesRequest(term=4, leader_id="node-2", prev_log_index=0, prev_log_term=0, entries=[], leader_commit=0)
        ae_res = await servicer.AppendEntries(ae_req, None)
        assert ae_res.success is False
        assert ae_res.term == 5
        assert node.persistent_state.current_term == 5


# ==============================================================================
# INVARIANT 3: Candidate log up-to-date validation (Raft §5.4.1)
# ==============================================================================
def test_invariant_candidate_log_freshness():
    """Verify election safety log comparison rule: higher term wins; tie breaks by longer log."""
    node = RaftNode(node_id="node-1")
    # Simulate voter having last log entry at Term 2, Index 5
    cmd = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="f1")
    node.persistent_state.log = [RaftLogEntry(index=5, term=2, command=cmd)]

    # Candidate with higher term is fresher regardless of index
    assert node.is_log_up_to_date(candidate_last_log_term=3, candidate_last_log_index=1) is True

    # Candidate with lower term is less fresh regardless of index
    assert node.is_log_up_to_date(candidate_last_log_term=1, candidate_last_log_index=10) is False

    # Same term: candidate with shorter log is rejected
    assert node.is_log_up_to_date(candidate_last_log_term=2, candidate_last_log_index=4) is False

    # Same term: candidate with equal or longer log is accepted
    assert node.is_log_up_to_date(candidate_last_log_term=2, candidate_last_log_index=5) is True
    assert node.is_log_up_to_date(candidate_last_log_term=2, candidate_last_log_index=6) is True


# ==============================================================================
# INVARIANT 4: Leader with outdated term steps down immediately
# ==============================================================================
@pytest.mark.asyncio
async def test_invariant_leader_outdated_term_stepdown():
    """Verify that an active leader seeing a higher term steps down to follower (Raft §5.2)."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        node = RaftNode(node_id="node-1", base_dir=tmp_dir)
        await node.initialize()
        node.role = NodeRole.LEADER
        node.leader_id = "node-1"
        node.persistent_state.current_term = 2
        node.leader_state = LeaderVolatileState(next_index={"node-2": 1}, match_index={"node-2": 0})
        servicer = RaftConsensusServicer(node=node)

        # Incoming AppendEntries from a newly elected leader in Term 3
        ae_req = raft_pb2.AppendEntriesRequest(term=3, leader_id="node-2", prev_log_index=0, prev_log_term=0, entries=[], leader_commit=0)
        res = await servicer.AppendEntries(ae_req, None)

        assert res.term == 3
        assert node.role == NodeRole.FOLLOWER
        assert node.leader_id == "node-2"
        assert node.persistent_state.current_term == 3
        assert node.leader_state is None


# ==============================================================================
# INVARIANT 5: Log consistency check rejects mismatched prev_log entries
# ==============================================================================
@pytest.mark.asyncio
async def test_invariant_log_consistency_check():
    """Verify follower rejects AppendEntries if prev_log_index and prev_log_term mismatch (Raft §5.3)."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        node = RaftNode(node_id="node-1", base_dir=tmp_dir)
        await node.initialize()
        node.persistent_state.current_term = 2
        cmd = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="f1")
        # Follower has entries: (index=1, term=1), (index=2, term=1)
        node.persistent_state.log = [
            RaftLogEntry(index=1, term=1, command=cmd),
            RaftLogEntry(index=2, term=1, command=cmd)
        ]
        servicer = RaftConsensusServicer(node=node)

        # Case A: Prev log index exists, but term does not match (term 2 != term 1)
        ae_mismatch = raft_pb2.AppendEntriesRequest(
            term=2, leader_id="node-2", prev_log_index=2, prev_log_term=2, entries=[], leader_commit=0
        )
        res_mismatch = await servicer.AppendEntries(ae_mismatch, None)
        assert res_mismatch.success is False

        # Case B: Prev log index does not exist (gap: prev_log_index=5)
        ae_gap = raft_pb2.AppendEntriesRequest(
            term=2, leader_id="node-2", prev_log_index=5, prev_log_term=1, entries=[], leader_commit=0
        )
        res_gap = await servicer.AppendEntries(ae_gap, None)
        assert res_gap.success is False

        # Case C: Prev log matches perfectly (index=2, term=1)
        ae_match = raft_pb2.AppendEntriesRequest(
            term=2, leader_id="node-2", prev_log_index=2, prev_log_term=1, entries=[], leader_commit=0
        )
        res_match = await servicer.AppendEntries(ae_match, None)
        assert res_match.success is True


# ==============================================================================
# INVARIANT 6: Strict Raft §5.4.2 current-term commit restriction
# ==============================================================================
@pytest.mark.asyncio
async def test_invariant_raft_5_4_2_current_term_commit_restriction():
    """Verify leader cannot commit older term entries by replica count alone (Raft §5.4.2)."""
    node = RaftNode(node_id="node-1")
    node.role = NodeRole.LEADER
    node.persistent_state.current_term = 3
    node.volatile_state.commit_index = 0

    cmd = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="f1")
    # Log contains: Entry 1 from Term 2 (older term), Entry 2 from Term 3 (current term)
    entry1 = RaftLogEntry(index=1, term=2, command=cmd)
    entry2 = RaftLogEntry(index=2, term=3, command=cmd)
    node.persistent_state.log = [entry1, entry2]

    pool = RaftPeerClientPool(peers=["node-2:50052", "node-3:50053"])
    engine = RaftElectionEngine(node=node, peer_pool=pool)

    # Scenario A: Quorum achieved on Entry 1 ONLY (which is from Term 2 != current term 3)
    node.leader_state = LeaderVolatileState(
        next_index={"node-2:50052": 2, "node-3:50053": 1},
        match_index={"node-2:50052": 1, "node-3:50053": 0}  # Node 1 + Node 2 = 2 (Majority)
    )
    await engine._check_and_advance_commit_index()
    # MUST NOT commit entry 1 because entry1.term (2) != current_term (3)!
    assert node.volatile_state.commit_index == 0

    # Scenario B: Quorum achieved on Entry 2 (which IS from current term 3)
    node.leader_state.match_index["node-2:50052"] = 2
    await engine._check_and_advance_commit_index()
    # Now commits entry 2, and indirectly commits entry 1!
    assert node.volatile_state.commit_index == 2


# ==============================================================================
# INVARIANT 7: State machine in-order application
# ==============================================================================
@pytest.mark.asyncio
async def test_invariant_state_machine_in_order_application():
    """Verify that committed entries are applied in strict monotonically increasing index order."""
    node = RaftNode(node_id="node-1")
    node.persistent_state.current_term = 1
    node.role = NodeRole.LEADER

    cmd1 = FileOperationCommand(operation=FileOperationType.REGISTER_USER, file_id="u1", owner_id="u1", filename="u1", email="u1@test.com", username="u1", password_hash="pwd", role="USER")
    cmd2 = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="f1", owner_id="u1", filename="doc.txt", sha256="hash1", size_bytes=10)
    cmd3 = FileOperationCommand(operation=FileOperationType.RENAME, file_id="f1", owner_id="u1", filename="doc_renamed.txt", new_name="doc_renamed.txt")

    node.persistent_state.log = [
        RaftLogEntry(index=1, term=1, command=cmd1),
        RaftLogEntry(index=2, term=1, command=cmd2),
        RaftLogEntry(index=3, term=1, command=cmd3)
    ]
    node.volatile_state.commit_index = 3
    node.volatile_state.last_applied = 0

    applied_count = await state_machine_executor.apply_committed_entries(node)
    assert applied_count == 3
    assert node.volatile_state.last_applied == 3

    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == "u1").first()
        file_rec = db.query(FileMetadata).filter(FileMetadata.id == "f1").first()
        applied_cmds = db.query(AppliedCommand).order_by(AppliedCommand.raft_index.asc()).all()

        assert user is not None
        assert file_rec is not None
        assert file_rec.sanitized_name == "doc_renamed.txt"
        assert [c.raft_index for c in applied_cmds] == [1, 2, 3]
    finally:
        db.close()


# ==============================================================================
# INVARIANT 8: Snapshot compaction preserves logical boundaries
# ==============================================================================
def test_invariant_snapshot_compaction_boundaries():
    """Verify log compaction preserves last_included_index, term, and suffix entries."""
    node = RaftNode(node_id="node-1")
    node.persistent_state.current_term = 2

    cmd = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="f1")
    # Entries 1..15
    node.persistent_state.log = [RaftLogEntry(index=i, term=2, command=cmd) for i in range(1, 16)]

    # Compact up to index 10
    node.compact_log(last_included_index=10, last_included_term=2)

    assert node.last_included_index == 10
    assert node.last_included_term == 2
    assert len(node.persistent_state.log) == 5  # Entries 11..15 preserved
    assert [e.index for e in node.persistent_state.log] == [11, 12, 13, 14, 15]

    # Test boundary term lookups
    assert node.get_log_term_at(10) == 2
    assert node.get_log_term_at(11) == 2
    assert node.get_log_entry(10) is None  # Entry 10 is pruned
    assert node.get_log_entry(11) is not None  # Entry 11 exists in active log


# ==============================================================================
# INVARIANT 9: Conflict truncation removes uncommitted entries
# ==============================================================================
@pytest.mark.asyncio
async def test_invariant_conflict_truncation_replaces_uncommitted_suffix():
    """Verify follower truncates uncommitted log entries conflicting with leader (Raft §5.3)."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        node = RaftNode(node_id="node-1", base_dir=tmp_dir)
        await node.initialize()
        node.persistent_state.current_term = 2

        cmd = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="f1")
        # Follower has entry 1 (term 1), and speculative entry 2 (term 1)
        node.persistent_state.log = [
            RaftLogEntry(index=1, term=1, command=cmd),
            RaftLogEntry(index=2, term=1, command=cmd)
        ]
        servicer = RaftConsensusServicer(node=node)

        # Leader sends new entry at index 2 with Term 2
        cmd2 = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="f2")
        leader_entry = RaftLogEntry(index=2, term=2, command=cmd2)
        ae_req = raft_pb2.AppendEntriesRequest(
            term=2,
            leader_id="node-2",
            prev_log_index=1,
            prev_log_term=1,
            entries=[leader_entry.to_proto()],
            leader_commit=1
        )

        res = await servicer.AppendEntries(ae_req, None)
        assert res.success is True

        # Conflicting entry 2 (term 1) must be replaced by entry 2 (term 2)
        assert len(node.persistent_state.log) == 2
        assert node.persistent_state.log[1].term == 2
        assert node.persistent_state.log[1].command.file_id == "f2"


# ==============================================================================
# INVARIANT 10: Single leader per term invariant
# ==============================================================================
@pytest.mark.asyncio
async def test_invariant_single_leader_promotion_per_term():
    """Verify atomic leader promotion ensures only a candidate with majority can become leader."""
    node = RaftNode(node_id="node-1")
    term, election_id = await node.transition_to_candidate()

    peers = ["node-2:50052", "node-3:50053"]
    majority = 2

    # Case A: Insufficient votes (only 1 vote from self) -> Promotion fails
    promoted_insufficient = await node.atomic_leader_promotion(
        election_term=term,
        election_id=election_id,
        unique_votes={"node-1"},
        majority=majority,
        peers=peers
    )
    assert promoted_insufficient is False
    assert node.role == NodeRole.CANDIDATE

    # Case B: Stale term promotion attempt -> Promotion fails
    promoted_stale = await node.atomic_leader_promotion(
        election_term=term - 1,
        election_id=election_id,
        unique_votes={"node-1", "node-2"},
        majority=majority,
        peers=peers
    )
    assert promoted_stale is False

    # Case C: Valid quorum (2 votes for current term and election_id) -> Promotion succeeds
    promoted_valid = await node.atomic_leader_promotion(
        election_term=term,
        election_id=election_id,
        unique_votes={"node-1", "node-2"},
        majority=majority,
        peers=peers
    )
    assert promoted_valid is True
    assert node.role == NodeRole.LEADER
    assert node.leader_id == "node-1"
