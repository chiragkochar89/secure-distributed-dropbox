from pathlib import Path


def test_proto_file_exists_and_valid():
    proto_path = Path(__file__).parents[2] / "proto" / "raft.proto"
    assert proto_path.exists(), "proto/raft.proto must exist"

    content = proto_path.read_text(encoding="utf-8")
    assert 'syntax = "proto3";' in content
    assert "service RaftConsensus" in content
    assert "rpc RequestVote" in content
    assert "rpc AppendEntries" in content
    assert "rpc Ping" in content
    assert "message LogEntry" in content
    assert "message RequestVoteRequest" in content
    assert "message RequestVoteResponse" in content
    assert "message AppendEntriesRequest" in content
    assert "message AppendEntriesResponse" in content
    assert "message PingRequest" in content
    assert "message PingResponse" in content

    # CRITICAL INVARIANT: File bytes MUST NOT be in LogEntry
    assert "bytes content" not in content
    assert "bytes file" not in content
