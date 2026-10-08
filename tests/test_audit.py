import json
import os
import stat

from nlm_gateway.audit import Audit, digest


def test_audit_lines_and_mode(tmp_path):
    a = Audit(tmp_path / "audit.jsonl")
    a.write("submit", consumer="research", url_hashes=[digest("https://secret.example/x")], uploads=1)
    a.write("job_done", consumer="research", job_id="j1")
    lines = [json.loads(l) for l in (tmp_path / "audit.jsonl").read_text().splitlines()]
    assert [l["event"] for l in lines] == ["submit", "job_done"] and "ts" in lines[0]
    assert "secret.example" not in (tmp_path / "audit.jsonl").read_text()
    assert stat.S_IMODE(os.stat(tmp_path / "audit.jsonl").st_mode) == 0o600


def test_digest_stable_and_short():
    assert digest("a") == digest("a") != digest("b") and len(digest("a")) == 16
