# PROJECT ASSURANCE ENGINEERING PLAN: SECURE DISTRIBUTED DROPBOX

**Project:** Secure Distributed Dropbox Using Raft Consensus  
**Target Path:** `C:\Users\Chira\.gemini\antigravity\scratch\secure-raft-dropbox`  
**Date:** October 17, 2026  
**Auditor / QA Lead:** Senior Distributed Systems Reliability Engineer, Raft Protocol Specialist, Application Security Engineer  

---

## 1. Executive Strategy & Objectives

The primary objective of this assurance engagement is to execute a rigorous, reproducible, evidence-backed evaluation of the **Secure Distributed Dropbox Using Raft Consensus** system. The assurance process tests critical Raft consensus safety invariants, distributed transaction semantics, durable object cleanup, multi-node replication consistency, fault tolerance, application security, API concurrency, and user-facing browser workflows.

Every finding, metric, and assurance gate will be established on **reproducible empirical execution** rather than prior historical reports.

---

## 2. Non-Negotiable Safety Protocols & Invariants

To guarantee data preservation and production environment integrity, all testing activities adhere strictly to the following safety rules:

1. **Volume & Data Preservation:** 
   - Never execute `docker compose down -v`.
   - Never wipe PostgreSQL database volumes (`postgres_data_1/2/3`), MinIO data volumes (`minio_data_1/2/3`), or persistent Raft state volumes (`raft_data_1/2/3`).
   - Preserve all existing demo accounts, user files, audit histories, and configuration files.
2. **Cluster Isolation & Fault Injection Safeguards:**
   - No destructive chaos testing (e.g., volume deletion, forced disk corruption) will be performed on the current data-bearing cluster containers.
   - Protocol-level fault testing (dropped RPCs, delayed RPCs, term conflicts, log divergence) will utilize isolated dynamic in-process Raft node fixtures with temporary ephemeral directories and kernel-allocated dynamic TCP ports.
   - Live cluster runtime tests will use isolated test-specific namespaces, unique UUID credentials, and targeted test files that are safely cleaned up via the Raft state machine API.
3. **Credential & Secret Protection:**
   - All JWT tokens, passwords, session cookies, Authorization headers, and sensitive environment secrets will be strictly redacted in all test logs and generated assurance artifacts.
4. **Deterministic Evaluation & Ambiguity Resolution:**
   - Every timeout is treated as potentially ambiguous until the committed state of the Raft log and PostgreSQL state machine is inspected.
   - Tests will use explicit health checks, bounded polling, and state assertions rather than arbitrary sleeps.
   - Status classifications: `PASS`, `FAIL`, `PARTIAL`, `NOT TESTED`, `BLOCKED`, `NOT APPLICABLE`.

---

## 3. Architecture & Assurance Baseline (Phase 1 Inspection)

### 3.1 Subsystem Implementations Verified
- **Raft Consensus Protocol (`backend/app/raft/`):**
  - Custom asynchronous Raft engine using `grpc.aio` Protocol Buffers (`raft.proto`).
  - Persistent state (`PersistentRaftState`): `current_term`, `voted_for`, `log`, `last_included_index`, `last_included_term`.
  - Volatile state (`VolatileRaftState`): `commit_index`, `last_applied`.
  - Leader state (`LeaderVolatileState`): `next_index`, `match_index` per peer.
  - Election timeout: Randomized between 1000ms - 2000ms (`ELECTION_TIMEOUT_MIN_MS` to `MAX_MS`); heartbeat interval: 150ms.
  - Quorum rule: Majority `(len(peers) + 1) // 2 + 1`. In a 3-node cluster, quorum is 2.
  - Commit restriction: Strictly adheres to Raft §5.4.2 (only commits current-term entries by replica counting; prior terms committed indirectly).
- **State Machine Application (`backend/app/raft/state_machine.py`):**
  - Monotonic `last_applied` execution applying committed `FileOperationCommand` entries to PostgreSQL.
  - Idempotency tracking via `AppliedCommand` table (`raft_index`, `term`, `command_type`, `file_id`) preventing double-application across container restarts or log replays.
  - Decoupled Storage I/O: Zero synchronous MinIO network calls inside database transactions. Deletions transactionally record `PendingObjectCleanup` rows.
- **Storage & Replication (`backend/app/storage/`):**
  - Binary object storage via MinIO S3 instances.
  - `ReplicationService` fan-out replication across peer MinIO instances post-commit.
  - `ReplicaAuditor` periodic background sweep checking SHA-256 against PostgreSQL ground truth, autonomously healing replicas, and draining pending object cleanups.
- **Security & RBAC (`backend/app/core/` & `backend/app/api/`):**
  - Argon2id password hashing (`$argon2id$v=19$m=65536,t=3,p=4$...`).
  - JWT tokens stored in HttpOnly, SameSite cookies and Bearer headers.
  - Strict server-side RBAC (Users access only owned files; Admins access all files).
  - Path traversal protection via `sanitize_filename()` removing control characters and directory separators.
- **Frontend SPA (`frontend/`):**
  - React 18 + TypeScript + Vite.
  - Automatic leader failover: Intercepts `503 NOT_LEADER` with `leader_id`, dynamically updates active node URL, and retries request.
  - Topology polling and connection state indicator.

### 3.2 Baseline Verification Execution Results
- **Backend Test Suite:**
  - Command: `$env:PYTHONPATH="backend"; .venv\Scripts\python.exe -m pytest -v`
  - Result: **98 Passed, 0 Failed, 0 Skipped, 1 Warning in 18.07s (Exit Code: 0)**
  - Warning Analysis: `.venv\Lib\site-packages\fastapi\testclient.py:1: StarletteDeprecationWarning: Using httpx with starlette.testclient is deprecated; install httpx2 instead.`
    - *Root Cause:* Starlette 0.40+ deprecation warning emitted when `fastapi.testclient` imports `starlette.testclient.TestClient`. It is an upstream informational notice and does not affect runtime reliability or test correctness.
- **Frontend Production Build:**
  - Command: `npm run build` in `frontend/`
  - Result: **TypeScript compilation (`tsc -b`) and Vite build succeeded in 1.28s (Exit Code: 0)**. Output: `dist/index.html` (0.55 kB), `dist/assets/index-DKLr-Gla.css` (16.25 kB), `dist/assets/index-K0bsdyKq.js` (165.07 kB).

---

## 4. Phase-by-Phase Execution Methodology

### Phase 2: Raft Safety & Protocol Correctness
- Author and execute executable property-based tests in `tests/phase7/test_raft_invariants.py`:
  1. Single vote per term guarantee.
  2. Stale-term message rejection and term monotonicity.
  3. Candidate log up-to-date validation (term comparison precedence over index).
  4. Outdated leader step-down on encountering higher term.
  5. Log consistency check rejecting mismatched `prev_log_index` / `prev_log_term`.
  6. Strict Raft §5.4.2 current-term commit enforcement.
  7. In-order state-machine application (`last_applied` strictly monotonic).
  8. Idempotent command handling across duplicate logs and replay scenarios.
  9. Snapshot compaction boundary preservation (`last_included_index`, `last_included_term`).
  10. Restart and recovery boundary synchronization.
  11. Non-oscillating election behavior under stable networks.
  12. Zero write commits when cluster lacks majority quorum.
  13. Conflict truncation preventing uncommitted log overwriting of committed history.
  14. Single leader invariant per term across cluster state.

### Phase 3: Crash and Restart Reliability Testing
- Controlled lifecycle tests using isolated in-process dynamic cluster fixtures:
  - Follower process crash and restart.
  - Leader crash and restart with automatic failover and catch-up.
  - Transient dependency unavailability (PostgreSQL / MinIO fallback and recovery).
  - Restart while object cleanups are pending.
  - Verification of metadata convergence and byte-exact SHA-256 matching.

### Phase 4: Network Partition & Quorum Verification
- Simulated bounded network faults:
  - Scenario A: Minority follower isolation (majority continues operating).
  - Scenario B: Leader isolated from followers (uncommitted write rejection; follower majority elects new leader).
  - Scenario C: Complete loss of majority (system halts commits; returns 503; committed data preserved).
  - Scenario D: Post-partition healing and log reconciliation.
  - Scenario E: Delayed, dropped, and reordered RPC messages.

### Phase 5: Data Integrity & Replica Healing
- End-to-end data integrity tests:
  - Byte-for-byte upload and download verification with SHA-256 matching.
  - Multi-node replication verification across all 3 MinIO instances.
  - Simulated replica corruption and autonomous self-healing via `ReplicaAuditor`.
  - Soft-delete semantics and durable background object cleanup verification.

### Phase 6: Application Security & RBAC Assurance
- Automated security test suite in `tests/phase7/test_security_assurance.py`:
  - Authentication: Invalid credentials, missing token, expired token, tampered token.
  - Authorization: Cross-user file access isolation (403 Forbidden), admin privileges.
  - Input Validation: Path traversal payloads (`../../etc/passwd`, null bytes, control characters), oversized uploads (413).
  - Session & Cookies: HttpOnly and SameSite cookie verification.

### Phase 7: Browser End-to-End Workflow Testing
- Set up lightweight browser test runner / Playwright test suite.
- Verify end-to-end workflows against running cluster:
  - UI initialization, login, file upload, download, rename, and deletion.
  - Automatic leader failover feedback in UI when primary node becomes unavailable.
  - Clear user-facing error states when quorum is lost.

### Phase 8: Concurrency & Stress Testing
- Concurrent operations:
  - Multiple simultaneous client uploads.
  - Racing rename and delete requests.
  - Client retries under transient 503 conditions.
  - Deterministic conflict resolution.

### Phase 9: Observability & Operational Readiness
- Prometheus metrics scraping verification:
  - Check `/metrics` endpoint across all 3 nodes.
  - Verify Prometheus targets status (`UP`) and scrape duration.
  - Grafana dashboard review and host port 3000 collision resolution.

### Phase 10: Regression Verification & Final Assurance Deliverables
- Re-run full test suite, build checks, and generate final assurance documentation under `reports/assurance/`.
