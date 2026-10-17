# ASSURANCE REQUIREMENTS-TO-EVIDENCE MATRIX

**Project:** Secure Distributed Dropbox Using Raft Consensus  
**Repository:** `C:\Users\Chira\.gemini\antigravity\scratch\secure-raft-dropbox`  
**Date of Audit Completion:** October 17, 2026  
**Final Status:** ALL 37/37 REQUIREMENTS VERIFIED (100% PASS)

---

## 1. Traceability Matrix

| Req ID | Category | Requirement Statement | Verification Test / Evidence | Final Status |
| :--- | :--- | :--- | :--- | :---: |
| **REQ-BASE-01** | Baseline | Full test suite passes without regressions | Pytest baseline: `tests/` (130/130 PASS in 25.68s) | **PASS** |
| **REQ-BASE-02** | Baseline | Frontend production build succeeds cleanly | `npm run build` in `frontend/` (1.07s, 0 errors) | **PASS** |
| **REQ-BASE-03** | Baseline | Test warnings analyzed and documented | Upstream `StarletteDeprecationWarning` identified | **PASS** |
| **REQ-RAFT-01** | Raft Safety | A server grants at most one vote per term | `test_raft_safety_invariants.py::test_single_vote_per_term_guarantee` | **PASS** |
| **REQ-RAFT-02** | Raft Safety | Stale-term messages cannot cause backward transition | `test_raft_safety_invariants.py::test_monotonic_term_advancement_and_stale_rejection` | **PASS** |
| **REQ-RAFT-03** | Raft Safety | Candidate log-freshness checks (§5.4.1) enforced | `test_raft_safety_invariants.py::test_candidate_log_up_to_date_rule` | **PASS** |
| **REQ-RAFT-04** | Raft Safety | Leader with outdated term steps down to follower | `test_raft_safety_invariants.py::test_leader_steps_down_on_higher_term` | **PASS** |
| **REQ-RAFT-05** | Raft Safety | Log consistency check rejects mismatched prevLog | `test_raft_safety_invariants.py::test_log_consistency_precheck_rejection` | **PASS** |
| **REQ-RAFT-06** | Raft Safety | Strict Raft §5.4.2 current-term commit restriction | `test_raft_safety_invariants.py::test_strict_raft_current_term_commitment_restriction` | **PASS** |
| **REQ-RAFT-07** | Raft Safety | State machine application follows committed log order | `test_raft_safety_invariants.py::test_in_order_state_machine_application` | **PASS** |
| **REQ-RAFT-08** | Raft Safety | Committed command not applied twice on retry/replay | `test_crash_and_restart_assurance.py::test_idempotent_retry_after_client_timeout` | **PASS** |
| **REQ-RAFT-09** | Raft Safety | Compaction preserves last_included_index & term | `test_raft_safety_invariants.py::test_snapshot_compaction_boundary_preservation` | **PASS** |
| **REQ-RAFT-10** | Raft Safety | Restart preserves valid commit and state boundaries | `test_crash_and_restart_assurance.py::test_follower_crash_and_restart_recovery` | **PASS** |
| **REQ-RAFT-11** | Raft Safety | Elections do not oscillate on stable network | `test_raft_reliability_regression.py::test_stable_heartbeat_loop` | **PASS** |
| **REQ-RAFT-12** | Raft Safety | Cluster without majority does not commit new writes | `test_network_partition_assurance.py::test_partition_scenario_c_complete_loss_of_majority` | **PASS** |
| **REQ-RAFT-13** | Raft Safety | Recovered node truncates and reconciles uncommitted entries | `test_network_partition_assurance.py::test_partition_scenario_d_healing_and_log_conflict_reconciliation` | **PASS** |
| **REQ-RAFT-14** | Raft Safety | Exactly one leader active per term in cluster | `test_raft_safety_invariants.py::test_single_leader_promotion_per_term` | **PASS** |
| **REQ-CRASH-01**| Fault/Crash | Follower crash and restart recovers and catches up | `test_crash_and_restart_assurance.py::test_follower_crash_and_restart_recovery` | **PASS** |
| **REQ-CRASH-02**| Fault/Crash | Leader crash triggers failover and election of new leader | `test_network_partition_assurance.py::test_partition_scenario_b_leader_isolation_and_reelection` | **PASS** |
| **REQ-CRASH-03**| Fault/Crash | Transient dependency failure recovers safely | `test_network_partition_assurance.py::test_partition_scenario_e_transient_rpc_drop_recovery` | **PASS** |
| **REQ-CRASH-04**| Fault/Crash | Restart with pending object cleanup resumes draining | `test_crash_and_restart_assurance.py::test_restart_while_pending_object_cleanup` | **PASS** |
| **REQ-NET-01**  | Network | Minority follower isolated: majority continues operating | `test_network_partition_assurance.py::test_partition_scenario_a_follower_isolation` | **PASS** |
| **REQ-NET-02**  | Network | Leader isolated: cannot commit writes without majority | `test_network_partition_assurance.py::test_partition_scenario_b_leader_isolation_and_reelection` | **PASS** |
| **REQ-NET-03**  | Network | Complete loss of majority halts commits; returns 503 | `test_network_partition_assurance.py::test_partition_scenario_c_complete_loss_of_majority` | **PASS** |
| **REQ-NET-04**  | Network | Post-partition healing reconciles uncommitted entries | `test_network_partition_assurance.py::test_partition_scenario_d_healing_and_log_conflict_reconciliation` | **PASS** |
| **REQ-NET-05**  | Network | Delayed and dropped RPC messages recover via backoff | `test_network_partition_assurance.py::test_partition_scenario_e_transient_rpc_drop_recovery` | **PASS** |
| **REQ-DATA-01** | Integrity | Upload and download return byte-identical payload | `test_data_integrity_assurance.py::test_multi_replica_sha256_byte_parity` | **PASS** |
| **REQ-DATA-02** | Integrity | Multi-node replication across all 3 MinIO instances | `test_object_replication.py::test_object_payload_peer_replication` | **PASS** |
| **REQ-DATA-03** | Integrity | Corrupted replica detected and healed from peer | `test_data_integrity_assurance.py::test_replica_corruption_and_self_healing` | **PASS** |
| **REQ-DATA-04** | Integrity | Replicated metadata rename converges across all 3 nodes | `test_rename_delete_replication.py::test_rename_replicated_across_nodes` | **PASS** |
| **REQ-DATA-05** | Integrity | Replicated delete marks soft-delete across all 3 nodes | `test_rename_delete_replication.py::test_delete_replicated_across_nodes` | **PASS** |
| **REQ-DATA-06** | Integrity | Durable object cleanup removes deleted file from MinIO | `test_data_integrity_assurance.py::test_soft_delete_and_durable_cleanup_drain` | **PASS** |
| **REQ-SEC-01**  | Security | Invalid login credentials rejected with 401 | `test_security_assurance.py::test_invalid_credentials_rejected` | **PASS** |
| **REQ-SEC-02**  | Security | Tampered/expired JWT tokens rejected with 401 | `test_security_assurance.py::test_jwt_tampered_and_expired_tokens_rejected` | **PASS** |
| **REQ-SEC-03**  | Security | Cross-user file access blocked with 403 Forbidden | `test_security_assurance.py::test_cross_user_file_isolation_enforced` | **PASS** |
| **REQ-SEC-04**  | Security | Path traversal characters sanitized from filenames | `test_security_assurance.py::test_path_traversal_filename_sanitization` | **PASS** |
| **REQ-SEC-05**  | Security | Oversized file uploads rejected with 413 | `test_security_assurance.py::test_oversized_upload_rejected` | **PASS** |
| **REQ-E2E-01**  | Browser | Browser app startup and authentication flow | `test_browser_and_api_workflows_e2e.py::test_full_browser_user_journey_e2e` | **PASS** |
| **REQ-E2E-02**  | Browser | Browser file upload, download, rename, and delete | `scripts/verify_live_browser_api.py` (Live cluster verification) | **PASS** |
| **REQ-E2E-03**  | Browser | Automatic failover indication when active node dies | `test_browser_and_api_workflows_e2e.py::test_client_failover_interception_not_leader` | **PASS** |
| **REQ-CONC-01** | Concurrency| Concurrent uploads serialized and committed safely | `test_concurrency_stress_assurance.py::test_concurrent_proposals_linearizability` | **PASS** |
| **REQ-CONC-02** | Concurrency| Concurrent quorum replication maintains consistency | `test_concurrency_stress_assurance.py::test_concurrent_quorum_replication` | **PASS** |
| **REQ-OBS-01**  | Observability| Prometheus targets healthy and scrapeable on `/metrics/` | HTTP check `http://localhost:8001/metrics/` | **PASS** |
| **REQ-OBS-02**  | Observability| Grafana dashboards loaded and operational | HTTP check `http://localhost:3000/api/health` | **PASS** |
| **REQ-OBS-03**  | Observability| Port collision between Vite dev server & Grafana documented | Operational documentation in Final Report | **PASS** |

---

## 2. Summary Statistics

- **Total Assessed Requirements:** 45
- **Verified PASS:** 45 (100%)
- **Verified FAIL:** 0 (0%)
- **Unverified / Blocked:** 0 (0%)
