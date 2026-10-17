import sys
import uuid
import hashlib
import httpx

BASE_URL_LEADER = "http://localhost:8001"
BASE_URL_FOLLOWER = "http://localhost:8002"

print("--- STARTING LIVE CLUSTER WORKFLOW AUDIT ---")

with httpx.Client(timeout=10.0) as client:
    # 1. Health checks
    for p in [8001, 8002, 8003]:
        r = client.get(f"http://localhost:{p}/api/v1/health")
        assert r.status_code == 200, f"Port {p} health failed: {r.text}"
        print(f"[OK] Port {p} is HEALTHY: {r.json()['node_id']}")

    # 2. Registration
    uid = uuid.uuid4().hex[:6]
    username = f"liveuser_{uid}"
    email = f"{username}@test.com"
    password = "LivePassword123!"

    reg_res = client.post(f"{BASE_URL_LEADER}/api/v1/auth/register", json={
        "email": email,
        "username": username,
        "password": password
    })
    assert reg_res.status_code == 201, f"Registration failed: {reg_res.text}"
    print(f"[OK] Registered live user: {username} (HTTP 201)")

    # 3. Login
    login_res = client.post(f"{BASE_URL_LEADER}/api/v1/auth/login", json={
        "username_or_email": username,
        "password": password
    })
    assert login_res.status_code == 200, f"Login failed: {login_res.text}"
    token = login_res.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    print(f"[OK] Authenticated live user with JWT (HTTP 200)")

    # 4. Upload file on Leader
    test_bytes = b"LIVE_AUDIT_VERIFIED_PAYLOAD_CONTENT_OCTOBER_2026"
    expected_sha256 = hashlib.sha256(test_bytes).hexdigest()

    files = {"file": ("audit_sample.bin", test_bytes, "application/octet-stream")}
    upload_res = client.post(f"{BASE_URL_LEADER}/api/v1/files", files=files, headers=headers)
    assert upload_res.status_code == 201, f"Upload failed: {upload_res.text}"
    file_data = upload_res.json()
    file_id = file_data["id"]
    assert file_data["current_sha256"] == expected_sha256
    print(f"[OK] Uploaded file {file_id} via Leader (HTTP 201, SHA-256 match)")

    # 5. Non-Leader Follower write interception test
    files_follower = {"file": ("err.bin", b"xyz", "application/octet-stream")}
    f_res = client.post(f"{BASE_URL_FOLLOWER}/api/v1/files", files=files_follower, headers=headers)
    assert f_res.status_code == 503, f"Expected 503 from follower, got {f_res.status_code}"
    detail = f_res.json()["detail"]
    assert detail["error"] == "NOT_LEADER"
    print(f"[OK] Follower correctly intercepted write with HTTP 503 NOT_LEADER (Leader is {detail.get('leader_id')})")

    # 6. Download file through Leader
    dl_res = client.get(f"{BASE_URL_LEADER}/api/v1/files/{file_id}/download", headers=headers)
    assert dl_res.status_code == 200
    assert dl_res.content == test_bytes
    assert hashlib.sha256(dl_res.content).hexdigest() == expected_sha256
    print(f"[OK] Downloaded file via API: byte parity and SHA-256 verified")

    # 7. Rename file on Leader
    rename_res = client.patch(f"{BASE_URL_LEADER}/api/v1/files/{file_id}", json={"new_filename": "renamed_audit_sample.bin"}, headers=headers)
    assert rename_res.status_code == 200
    assert rename_res.json()["sanitized_name"] == "renamed_audit_sample.bin"
    print(f"[OK] Renamed file via Raft state machine: (HTTP 200)")

    # 8. Delete file on Leader
    del_res = client.delete(f"{BASE_URL_LEADER}/api/v1/files/{file_id}", headers=headers)
    assert del_res.status_code in [200, 204]
    print(f"[OK] Soft-deleted file via Raft state machine: (HTTP {del_res.status_code})")

    # 9. Verify subsequent download returns 404
    dl_check = client.get(f"{BASE_URL_LEADER}/api/v1/files/{file_id}/download", headers=headers)
    assert dl_check.status_code == 404
    print(f"[OK] Subsequent download returns HTTP 404 as expected")

print("--- ALL LIVE CLUSTER WORKFLOW AUDITS PASSED ---")
