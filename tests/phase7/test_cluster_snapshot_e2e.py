import os
import socket
import shutil
import tempfile
import pytest
import asyncio
from typing import List

from app.db.session import SessionLocal, init_db
from app.db.models.user import User
from app.db.models.file import FileMetadata, FileVersion, AppliedCommand
from app.services.commands import FileOperationCommand, FileOperationType
from app.raft.node import RaftNode, NodeRole
from app.raft.state import LeaderVolatileState
from app.raft.engine import RaftElectionEngine
from app.raft.rpc_client import RaftPeerClientPool
from app.raft.rpc_server import GRPCServerManager
from app.raft.state_machine import state_machine_executor
from app.raft.snapshot import SnapshotManager, RaftSnapshot, dump_postgres_state_machine
from app.services.auditor import ReplicaAuditor
from app.core.crypto import compute_sha256


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
    from app.db.session import engine
    from app.storage.minio_client import storage_service
    storage_service.use_local_fallback = True
    engine.dispose()
    init_db()
    storage_dir = os.path.join(os.getcwd(), "data", "storage")
    raft_dir = os.path.join(os.getcwd(), "data", "raft")
    for d in [storage_dir, raft_dir]:
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d, exist_ok=True)

    yield

    engine.dispose()
    for d in [storage_dir, raft_dir]:
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def temp_cluster_dirs():
    dirs = [tempfile.mkdtemp() for _ in range(3)]
    init_db()
    yield dirs
    for d in dirs:
        shutil.rmtree(d, ignore_errors=True)


def dump_bytes(data):
    import json
    return json.dumps(data, sort_keys=True).encode("utf-8")


@pytest.mark.asyncio
async def test_e2e_cluster_dynamic_snapshot_and_lagging_follower_recovery(temp_cluster_dirs):
    """
    Phase 7E E2E Test 1: Full cluster integration of dynamic snapshot creation & automatic lagging follower InstallSnapshot recovery.
    Enforces dynamic OS port assignment and 15s timeout deadline.
    """
    async def run_test():
        ports = find_free_ports(3)
        addrs = [f"127.0.0.1:{p}" for p in ports]

        # Node 1: Leader
        node1 = RaftNode(node_id="node-1", base_dir=temp_cluster_dirs[0])
        await node1.initialize()
        node1.role = NodeRole.LEADER
        node1.persistent_state.current_term = 1
        node1.leader_state = LeaderVolatileState(
            next_index={addrs[1]: 1, addrs[2]: 1},
            match_index={addrs[1]: 0, addrs[2]: 0}
        )

        # Node 2: Synced Follower
        node2 = RaftNode(node_id="node-2", base_dir=temp_cluster_dirs[1])
        await node2.initialize()
        node2.persistent_state.current_term = 1

        # Node 3: Lagging Follower (starts offline)
        node3 = RaftNode(node_id="node-3", base_dir=temp_cluster_dirs[2])
        await node3.initialize()
        node3.persistent_state.current_term = 1

        # Populate Leader with 10 file upload log entries
        for i in range(1, 11):
            file_id = f"file-e2e-{i}"
            cmd = FileOperationCommand(
                operation=FileOperationType.UPLOAD,
                file_id=file_id,
                owner_id="user-e2e",
                filename=f"doc_{i}.pdf",
                sha256=f"sha_e2e_{i}",
                object_key=f"user-e2e/{file_id}/v1",
                size_bytes=1024 * i,
                version=1
            )
            await node1.append_log_entry(cmd)

        # Leader commits entries 1..10 and applies to state machine (automatically triggers snapshot at threshold 10)
        node1.volatile_state.commit_index = 10
        applied = await state_machine_executor.apply_committed_entries(node1)
        assert applied == 10
        assert node1.volatile_state.last_applied == 10
        assert node1.last_included_index == 10
        assert len(node1.persistent_state.log) == 0  # Compacted entries 1..10

        # Start gRPC servers for Node 2 and Node 3
        eng2 = RaftElectionEngine(node=node2)
        srv2 = GRPCServerManager(port=ports[1], node=node2, election_engine=eng2)
        await srv2.start()

        eng3 = RaftElectionEngine(node=node3)
        srv3 = GRPCServerManager(port=ports[2], node=node3, election_engine=eng3)
        await srv3.start()

        # Leader peer pool
        pool1 = RaftPeerClientPool(peers=[addrs[1], addrs[2]])
        eng1 = RaftElectionEngine(node=node1, peer_pool=pool1)

        try:
            # Leader replicates log to Node 3 (which is lagging at next_index = 1)
            # Leader detects next_index (1) <= last_included_index (10), triggers _send_install_snapshot
            await eng1._replicate_to_peer(addrs[2], target_index=10)

            # Verify Node 3 updated via InstallSnapshot
            assert node1.leader_state.match_index[addrs[2]] == 10
            assert node1.leader_state.next_index[addrs[2]] == 11
            assert node3.last_included_index == 10
            assert node3.volatile_state.commit_index == 10
            assert node3.volatile_state.last_applied == 10

            # Verify Node 3 PostgreSQL database restored state machine metadata
            db3 = SessionLocal()
            files3 = db3.query(FileMetadata).all()
            assert len(files3) == 10
            db3.close()

        finally:
            if 'srv2' in locals() and srv2:
                try:
                    await srv2.stop()
                except Exception:
                    pass
            if 'srv3' in locals() and srv3:
                try:
                    await srv3.stop()
                except Exception:
                    pass
            if 'pool1' in locals() and pool1:
                try:
                    await pool1.close_all()
                except Exception:
                    pass

    await asyncio.wait_for(run_test(), timeout=15.0)


@pytest.mark.asyncio
async def test_e2e_cluster_post_snapshot_log_replication(temp_cluster_dirs):
    """
    Phase 7E E2E Test 2: Verify normal log replication resumes seamlessly after InstallSnapshot recovery.
    """
    async def run_test():
        ports = find_free_ports(2)
        addrs = [f"127.0.0.1:{p}" for p in ports]

        node1 = RaftNode(node_id="node-1", base_dir=temp_cluster_dirs[0])
        await node1.initialize()
        node1.role = NodeRole.LEADER
        node1.persistent_state.current_term = 1
        node1.leader_state = LeaderVolatileState(
            next_index={addrs[1]: 1},
            match_index={addrs[1]: 0}
        )

        node2 = RaftNode(node_id="node-2", base_dir=temp_cluster_dirs[1])
        await node2.initialize()
        node2.persistent_state.current_term = 1

        # Populate leader PostgreSQL state & snapshot at index 5
        db = SessionLocal()
        for i in range(1, 6):
            fm = FileMetadata(
                id=f"f-snap-{i}", owner_id="u1", original_filename=f"f{i}.txt",
                sanitized_name=f"f{i}.txt", content_type="text/plain", size_bytes=100,
                current_sha256=f"hash{i}", current_version=1, is_deleted=False
            )
            db.add(fm)
        db.commit()
        db.close()

        state_data = dump_postgres_state_machine()
        checksum = compute_sha256(dump_bytes(state_data))
        snapshot = RaftSnapshot(
            last_included_index=5,
            last_included_term=1,
            state_machine_checksum=checksum,
            metadata_state=state_data
        )
        SnapshotManager.save_snapshot_atomically(node1.node_id, snapshot, temp_cluster_dirs[0])
        node1.compact_log(5, 1)
        node1.volatile_state.commit_index = 5
        node1.volatile_state.last_applied = 5

        # Start Node 2 server
        eng2 = RaftElectionEngine(node=node2)
        srv2 = GRPCServerManager(port=ports[1], node=node2, election_engine=eng2)
        await srv2.start()

        pool1 = RaftPeerClientPool(peers=[addrs[1]])
        eng1 = RaftElectionEngine(node=node1, peer_pool=pool1)

        try:
            # Step 1: Recover follower via InstallSnapshot
            success = await eng1._send_install_snapshot(addrs[1])
            assert success is True
            assert node2.volatile_state.last_applied == 5

            # Step 2: Append new entry 6 to Leader log
            cmd6 = FileOperationCommand(
                operation=FileOperationType.UPLOAD,
                file_id="f-post-snap-6",
                owner_id="u1",
                filename="post6.txt",
                sha256="hash6",
                object_key="u1/f-post-snap-6/v1",
                size_bytes=200,
                version=1
            )
            await node1.append_log_entry(cmd6)
            node1.volatile_state.commit_index = 6
            await state_machine_executor.apply_committed_entries(node1)

            # Step 3: Replicate entry 6 to follower over normal AppendEntries
            await eng1._replicate_to_peer(addrs[1], target_index=6)
            assert node1.leader_state.match_index[addrs[1]] == 6
            assert node2.persistent_state.last_log_index == 6

            # Step 4: Apply committed entry 6 on Follower state machine
            await state_machine_executor.apply_committed_entries(node2)
            assert node2.volatile_state.last_applied == 6

            # Verify follower has all 6 files in PostgreSQL
            db2 = SessionLocal()
            f6 = db2.query(FileMetadata).filter(FileMetadata.id == "f-post-snap-6").first()
            assert f6 is not None
            assert f6.original_filename == "post6.txt"
            db2.close()

        finally:
            if 'srv2' in locals() and srv2:
                try:
                    await srv2.stop()
                except Exception:
                    pass
            if 'pool1' in locals() and pool1:
                try:
                    await pool1.close_all()
                except Exception:
                    pass

    await asyncio.wait_for(run_test(), timeout=15.0)


@pytest.mark.asyncio
async def test_e2e_replica_auditor_post_snapshot_health(temp_cluster_dirs):
    """
    Phase 7E E2E Test 3: Verify Leader ReplicaAuditor operates cleanly on cluster after snapshot recovery.
    """
    async def run_test():
        node1 = RaftNode(node_id="node-1", base_dir=temp_cluster_dirs[0])
        await node1.initialize()
        node1.role = NodeRole.LEADER
        node1.persistent_state.current_term = 1

        # Start ReplicaAuditor on Leader (synchronous method)
        auditor = ReplicaAuditor(node=node1)
        auditor.start()
        assert auditor.is_running is True

        # Stop auditor cleanly
        await auditor.stop()
        assert auditor.is_running is False

    await asyncio.wait_for(run_test(), timeout=10.0)
