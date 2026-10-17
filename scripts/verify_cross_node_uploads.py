import os
import hashlib
import jwt
from datetime import datetime, timedelta, timezone
import httpx

JWT_SECRET = "nbQCpsrwNhr90Moenf5-7YH5blzXuWQkoL1Gg-mNXpC_m2CJ0cYBbzI1Go51pOE5"
USER_ID = "c8a354d5-7878-468e-9eb4-5d9e415cebfa"

# Create fresh token for user Chirag
expire = datetime.now(timezone.utc) + timedelta(minutes=60)
token = jwt.encode({"sub": USER_ID, "exp": expire}, JWT_SECRET, algorithm="HS256")
headers = {"Authorization": f"Bearer {token}"}

# Test File 1: raft-demo-test.txt
file1_id = "b4517d0b-610f-4c46-8cf0-2e75546094f5"
with open("raft-demo-test.txt", "rb") as f:
    local_file1_bytes = f.read()
file1_expected_sha256 = hashlib.sha256(local_file1_bytes).hexdigest()

# Test File 2: raft-failover-write-test.txt
file2_id = "dba95720-7e45-4c09-af6b-08a11c473036"
with open("raft-failover-write-test.txt", "rb") as f:
    local_file2_bytes = f.read()
file2_expected_sha256 = hashlib.sha256(local_file2_bytes).hexdigest()

print("--- CROSS-NODE FAILOVER UPLOAD VERIFICATION ---")
print(f"File 1 ID: {file1_id} | Expected SHA-256: {file1_expected_sha256}")
print(f"File 2 ID: {file2_id} | Expected SHA-256: {file2_expected_sha256}")

with httpx.Client(timeout=10.0) as client:
    for node_port in [8001, 8002, 8003]:
        base_url = f"http://localhost:{node_port}/api/v1"

        # Check metadata visibility
        meta1_res = client.get(f"{base_url}/files", headers=headers)
        assert meta1_res.status_code == 200, f"Port {node_port} failed list files: {meta1_res.text}"
        files = {f["id"]: f for f in meta1_res.json()}

        assert file1_id in files, f"File 1 missing from node port {node_port}"
        assert file2_id in files, f"File 2 missing from node port {node_port}"
        print(f"[OK] Port {node_port}: Both files visible in metadata list!")

        # Check download File 1
        dl1 = client.get(f"{base_url}/files/{file1_id}/download", headers=headers)
        assert dl1.status_code == 200, f"Port {node_port} dl1 failed: {dl1.status_code}"
        dl1_hash = hashlib.sha256(dl1.content).hexdigest()
        assert dl1_hash == file1_expected_sha256
        print(f"[OK] Port {node_port}: File 1 downloaded, SHA-256 matches bit-for-bit!")

        # Check download File 2 (The post-failover write!)
        dl2 = client.get(f"{base_url}/files/{file2_id}/download", headers=headers)
        assert dl2.status_code == 200, f"Port {node_port} dl2 failed: {dl2.status_code}"
        dl2_hash = hashlib.sha256(dl2.content).hexdigest()
        assert dl2_hash == file2_expected_sha256
        print(f"[OK] Port {node_port}: File 2 (post-failover) downloaded, SHA-256 matches bit-for-bit!")

print("--- ALL CROSS-NODE VISIBILITY & INTEGRITY CHECKS CONFIRMED! ---")
