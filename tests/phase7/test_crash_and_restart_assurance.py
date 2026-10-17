import os
import socket
import shutil
import tempfile
import pytest
import asyncio
from typing import List
from unittest.mock import patch, AsyncMock

from app.db.session import SessionLocal, init_db
from app.db.models.user import User, UserRole
from app.db.models.file import FileMetadata, FileVersion, AppliedCommand, PendingObjectCleanup
from app.db.models.audit import AuditLog
from app.services.commands import FileOperationCommand, FileOperationType
from app.raft.node import RaftNode, NodeRole
from app.raft.state import LeaderVolatileState, RaftLogEntry
from app.raft.engine import RaftElectionEngine
from app.raft.rpc_client import RaftPeerClientPool
from app.raft.rpc_server import GRPCServerManager
from app.raft.state_machine import state_machine_executor
from app.services.auditor import ReplicaAuditor
from app.proto import raft_pb2


def find_free_ports(count: int = 3) -> List[int]:
    """Dynamically allocates unique free TCP ports from OS kernel."""
    ports = []
    sockets = []
    for _ in range(count):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('127.0.0.1', 0))
        port = s.getsockname()[1]
        ports.append(port)
        sockets.append(s)
    for s in sockets:
        s.close()
    return ports


@pytest.fixture(autouse=True)
def clean_env():
    """Ensure clean database and storage environment before each test."""
    init_db()
    db = SessionLocal()
    try:
        db.query(PendingObjectCleanup).delete()
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


@pytest.mark.asyncio
async def test_follower_crash_and_restart_recovery():
    """Phase 3 Test 1: Follower process crashes, leader commits entries, follower restarts and catches up."""
    ports = find_free_ports(2)
    addr1 = f"127.0.0.1:{ports[0]}"
    addr2 = f"127.0.0.1:{ports[1]}"

    dir1 = tempfile.mkdtemp()
    dir2 = tempfile.mkdtemp()

    try:
        # Node 1: Leader (client-only engine)
        node1 = RaftNode(node_id="node-1", base_dir=dir1)
        await node1.initialize()
        node1.role = NodeRole.LEADER
        node1.persistent_state.current_term = 1
        node1.leader_state = LeaderVolatileState(next_index={addr2: 1}, match_index={addr2: 0})
        pool1 = RaftPeerClientPool(peers=[addr2])
        eng1 = RaftElectionEngine(node=node1, peer_pool=pool1)

        # Node 2: Follower server
        node2 = RaftNode(node_id="node-2", base_dir=dir2)
        await node2.initialize()
        eng2 = RaftElectionEngine(node=node2)
        srv2 = GRPCServerManager(port=ports[1], node=node2, election_engine=eng2)
        await srv2.start()

        # Step 1: Initial commit with both nodes online
        cmd1 = FileOperationCommand(operation=FileOperationType.REGISTER_USER, file_id="u1", owner_id="u1", filename="u1", email="u1@test.com", username="u1", password_hash="pwd", role="USER")
        await node1.append_log_entry(cmd1)
        res1 = await eng1.replicate_to_peers_and_await_applied(1, timeout=5.0)
        assert res1 is True
        assert node1.volatile_state.commit_index == 1

        # Step 2: SIMULATE FOLLOWER CRASH (stop server)
        await srv2.stop()

        # Step 3: Leader appends Entry 2 while follower is offline
        cmd2 = FileOperationCommand(operation=FileOperationType.UPLOAD, file_id="f1", owner_id="u1", filename="doc.txt", sha256="hash1", size_bytes=10)
        await node1.append_log_entry(cmd2)

        # Step 4: RESTART FOLLOWER (reinitialize from disk, restart gRPC server on same port)
        restarted_node2 = RaftNode(node_id="node-2", base_dir=dir2)
        await restarted_node2.initialize()
        assert restarted_node2.last_log_index == 1  # Loaded from disk
        eng2_new = RaftElectionEngine(node=restarted_node2)
        srv2_new = GRPCServerManager(port=ports[1], node=restarted_node2, election_engine=eng2_new)
        await srv2_new.start()

        # Refresh client channel in pool1
        old_c = pool1.clients.pop(addr2, None)
        if old_c:
            await old_c.close()

        # Step 5: Leader catches up follower
        await eng1._replicate_to_peer(addr2, target_index=2, max_retries=3)
        assert restarted_node2.last_log_index == 2
        assert node1.volatile_state.commit_index == 2

        # Follower receives leader's updated commit index in the subsequent heartbeat
        client = pool1.get_client(addr2)
        hb = raft_pb2.AppendEntriesRequest(
            term=node1.persistent_state.current_term,
            leader_id=node1.node_id,
            prev_log_index=2,
            prev_log_term=1,
            entries=[],
            leader_commit=node1.volatile_state.commit_index
        )
        res = await client.send_append_entries(hb, timeout=1.0)
        assert res.success is True
        assert restarted_node2.volatile_state.commit_index == 2

        # Cleanup
        await pool1.close_all()
        await srv2_new.stop(grace=0)

    finally:
        shutil.rmtree(dir1, ignore_errors=True)
        shutil.rmtree(dir2, ignore_errors=True)


@pytest.mark.asyncio
async def test_restart_while_pending_object_cleanup():
    """Phase 3 Test 2: Application restarts while object cleanups are PENDING; auditor drains on restart."""
    db = SessionLocal()
    cleanup_id = "cln-restart-1"
    obj_key = "users/u1/f1/v1"
    try:
        cln = PendingObjectCleanup(
            id=cleanup_id,
            file_id="f1",
            object_key=obj_key,
            status="PENDING",
            retry_count=0
        )
        db.add(cln)
        db.commit()
    finally:
        db.close()

    # Simulate fresh auditor initialization on application restart
    restarted_auditor = ReplicaAuditor(audit_interval_seconds=60)

    with patch("app.storage.minio_client.storage_service.delete_object") as mock_local_del, \
         patch("app.storage.replication_service.replication_service.delete_object_from_all_peers", new_callable=AsyncMock) as mock_peer_del:
        mock_peer_del.return_value = {"http://peer2:8002": True, "http://peer3:8003": True}

        drained = await restarted_auditor.process_pending_cleanups()
        assert drained == 1
        mock_local_del.assert_called_once_with(obj_key)

    db = SessionLocal()
    try:
        updated = db.query(PendingObjectCleanup).filter(PendingObjectCleanup.id == cleanup_id).first()
        assert updated.status == "COMPLETED"
    finally:
        db.close()


@pytest.mark.asyncio
async def test_idempotent_retry_after_client_timeout():
    """Phase 3 Test 3: Client times out while operation was committed; retrying same command returns consistent state."""
    db = SessionLocal()
    try:
        user = User(id="u-timeout", email="ut@test.com", username="ut", password_hash="pwd", role=UserRole.USER.value)
        f_meta = FileMetadata(
            id="f-timeout",
            owner_id="u-timeout",
            original_filename="doc.txt",
            sanitized_name="doc.txt",
            content_type="text/plain",
            size_bytes=10,
            current_sha256="hash",
            current_version=1,
            is_deleted=False
        )
        db.add(user)
        db.add(f_meta)
        db.commit()
    finally:
        db.close()

    node = RaftNode(node_id="node-1")
    node.persistent_state.current_term = 1
    node.role = NodeRole.LEADER

    cmd = FileOperationCommand(
        operation=FileOperationType.RENAME,
        file_id="f-timeout",
        owner_id="u-timeout",
        filename="doc_renamed.txt",
        new_name="doc_renamed.txt",
        metadata={"original_filename": "doc_renamed.txt"}
    )
    entry = RaftLogEntry(term=1, index=1, command=cmd)
    node.persistent_state.log.append(entry)
    node.volatile_state.commit_index = 1

    # First application (simulates commit succeeding on cluster before client times out)
    applied_1 = await state_machine_executor.apply_committed_entries(node)
    assert applied_1 == 1

    # Second application (simulates client retry of same command after timeout)
    applied_2 = await state_machine_executor.apply_committed_entries(node)
    assert node.volatile_state.last_applied == 1

    db = SessionLocal()
    try:
        f = db.query(FileMetadata).filter(FileMetadata.id == "f-timeout").first()
        assert f.sanitized_name == "doc_renamed.txt"
        audits = db.query(AuditLog).filter(AuditLog.resource_id == "f-timeout").all()
        # Exactly one audit record despite retry
        assert len(audits) == 1
    finally:
        db.close()
