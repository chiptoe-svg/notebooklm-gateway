import json
import logging
import os
import stat
import threading

import pytest

from nlm_gateway.consumers import Registry, hash_token, new_token


@pytest.fixture
def reg(cfg):
    return Registry(cfg.state_dir / "consumers.json")


def test_token_shape_and_hash():
    t = new_token()
    assert t.startswith("nlm_") and len(t) == 4 + 43
    assert hash_token(t) == hash_token(t) and hash_token(t) != hash_token(new_token())


def test_pair_stores_only_hash_with_0600(reg):
    token = reg.pair("research", ["podcast"], "research", 3, 10, note="research group")
    raw = reg.path.read_text()
    assert token not in raw and hash_token(token) in raw
    assert stat.S_IMODE(os.stat(reg.path).st_mode) == 0o600
    assert stat.S_IMODE(os.stat(reg.path.parent).st_mode) == 0o700


def test_authenticate(reg):
    token = reg.pair("research", ["podcast", "report"], "research", 3, 10)
    c = reg.authenticate(token)
    assert c.id == "research" and c.scopes == {"podcast", "report"}
    assert reg.authenticate("nlm_wrong") is None
    assert reg.authenticate(None) is None
    assert reg.authenticate("Bearer " + token) is None  # caller must strip the scheme


def test_revoke_is_immediate(reg):
    token = reg.pair("research", ["podcast"], "research", 3, 10)
    assert reg.revoke("research") is True
    assert reg.authenticate(token) is None
    assert reg.revoke("research") is False


def test_registry_edits_by_another_process_are_seen(reg):
    token = reg.pair("a", ["podcast"], "a", 1, 1)
    data = json.loads(reg.path.read_text())
    data["consumers"] = []
    reg.path.write_text(json.dumps(data))
    assert reg.authenticate(token) is None


@pytest.mark.parametrize("kwargs", [
    dict(id="Bad Id", scopes=["podcast"], notebook_label="x"),
    dict(id="ok", scopes=[], notebook_label="x"),
    dict(id="ok", scopes=["video"], notebook_label="x"),
    dict(id="ok", scopes=["podcast"], notebook_label=""),
    dict(id="ok", scopes=["podcast"], notebook_label="   "),
    dict(id="ok", scopes=["podcast"], notebook_label="x" * 101),
    dict(id="ok", scopes=["podcast"], notebook_label="a\nb"),
    dict(id="ok", scopes=["podcast"], notebook_label="a\x00b"),
    dict(id="ok\n", scopes=["podcast"], notebook_label="x"),
])
def test_pair_validation(reg, kwargs):
    with pytest.raises(ValueError):
        reg.pair(daily_audio_cap=1, daily_report_cap=1, **kwargs)


def test_duplicate_id_rejected(reg):
    reg.pair("a", ["podcast"], "a", 1, 1)
    with pytest.raises(ValueError):
        reg.pair("a", ["podcast"], "a2", 1, 1)


def test_get(reg):
    reg.pair("a", ["podcast"], "alpha", 1, 1)
    assert reg.get("a").notebook_label == "alpha" and reg.get("zz") is None


def test_concurrent_pairs_all_survive(reg):
    n = 20
    barrier = threading.Barrier(n)
    errors = []

    def worker(i):
        try:
            barrier.wait()
            reg.pair(f"c{i}", ["podcast"], f"c{i}", 1, 1)
        except Exception as e:  # collected and asserted below
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert sorted(c.id for c in reg.load()) == sorted(f"c{i}" for i in range(n))


def test_corrupt_registry_refuses_all_tokens(reg, caplog):
    token = reg.pair("a", ["podcast"], "a", 1, 1)
    reg.path.write_text('{"consumers": [{"id": "a", "tok')  # truncated JSON
    with caplog.at_level(logging.ERROR, logger="nlm_gateway.consumers"):
        assert reg.authenticate(token) is None
    assert [r.getMessage() for r in caplog.records] == ["consumer registry unreadable; refusing all tokens"]
    assert "tok" not in caplog.text.replace("refusing all tokens", "")


def test_record_missing_key_refuses_all_tokens(reg):
    token = reg.pair("a", ["podcast"], "a", 1, 1)
    data = json.loads(reg.path.read_text())
    del data["consumers"][0]["token_hash"]
    reg.path.write_text(json.dumps(data))
    assert reg.authenticate(token) is None


def test_null_token_hash_refuses_all_tokens(reg, caplog):
    token = reg.pair("a", ["podcast"], "a", 1, 1)
    data = json.loads(reg.path.read_text())
    data["consumers"][0]["token_hash"] = None
    reg.path.write_text(json.dumps(data))
    with caplog.at_level(logging.ERROR, logger="nlm_gateway.consumers"):
        assert reg.authenticate(token) is None
    assert [r.getMessage() for r in caplog.records] == ["consumer registry unreadable; refusing all tokens"]


@pytest.mark.parametrize("bad", ["{", '{"consumers": [{"id": 1}]}', '{"consumers": "x"}', "[]", ""])
def test_get_fails_closed_on_corrupt_registry(reg, caplog, bad):
    reg.pair("a", ["podcast"], "a", 1, 1)
    reg.path.write_text(bad)
    with caplog.at_level(logging.ERROR, logger="nlm_gateway.consumers"):
        assert reg.get("a") is None
    assert [r.getMessage() for r in caplog.records] == ["consumer registry unreadable; treating consumer as unknown"]


def test_label_is_a_notebook_name_and_list_scope_exists(reg):
    reg.pair("a", ["podcast", "report", "list"], "  To Review  ", 1, 1)
    reg.pair("b", ["list"], "Reading · 2026", 0, 0)  # '·' is allowed in notebook names
    assert reg.get("a").notebook_label == "To Review" and reg.get("a").scopes == {"podcast", "report", "list"}
    assert reg.get("b").notebook_label == "Reading · 2026"
    reg.pair("c", ["podcast"], "x" * 100, 1, 1)


def test_update_changes_fields_but_keeps_token(reg):
    token = reg.pair("a", ["podcast"], "research", 3, 10, note="n")
    before = reg.get("a")
    assert reg.update("a", scopes=["podcast", "report", "list"], notebook_label="To Review", daily_audio_cap=5, daily_report_cap=7) is True
    c = reg.authenticate(token)
    assert c is not None and c.token_hash == before.token_hash and c.created_at == before.created_at and c.note == "n"
    assert c.scopes == {"podcast", "report", "list"} and c.notebook_label == "To Review"
    assert (c.daily_audio_cap, c.daily_report_cap) == (5, 7)


def test_update_partial_and_unknown(reg):
    reg.pair("a", ["podcast"], "research", 3, 10)
    reg.pair("b", ["report"], "other", 1, 1)
    assert reg.update("a", notebook_label="To Review") is True
    a, b = reg.get("a"), reg.get("b")
    assert a.notebook_label == "To Review" and a.scopes == {"podcast"} and a.daily_audio_cap == 3
    assert b.notebook_label == "other"
    assert reg.update("zz", notebook_label="x") is False


@pytest.mark.parametrize("kwargs", [dict(scopes=[]), dict(scopes=["video"]), dict(notebook_label=""), dict(notebook_label="a\tb"),
                                    dict(notebook_label="x" * 101)])
def test_update_validation(reg, kwargs):
    reg.pair("a", ["podcast"], "research", 3, 10)
    raw = reg.path.read_text()
    with pytest.raises(ValueError):
        reg.update("a", **kwargs)
    assert reg.path.read_text() == raw


def test_update_cli(cfg):
    import subprocess
    import sys
    from pathlib import Path
    script = Path(__file__).resolve().parent.parent / "scripts" / "consumers.py"
    reg = Registry(cfg.state_dir / "consumers.json")
    token = reg.pair("a", ["podcast"], "research", 3, 10)
    r = subprocess.run([sys.executable, str(script), "--state-dir", str(cfg.state_dir), "update", "--id", "a",
                        "--scopes", "podcast,list", "--label", "To Review", "--audio-cap", "4"], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    c = reg.authenticate(token)
    assert c.scopes == {"podcast", "list"} and c.notebook_label == "To Review" and c.daily_audio_cap == 4 and c.daily_report_cap == 10
    r = subprocess.run([sys.executable, str(script), "--state-dir", str(cfg.state_dir), "update", "--id", "zz", "--label", "x"],
                       capture_output=True, text=True)
    assert r.returncode == 1 and "no such consumer" in r.stdout


def test_new_scopes_accepted(reg):
    token = reg.pair("laptop", ["read", "studio", "notes", "list"], "To Review", 0, 0)
    assert reg.authenticate(token).scopes == {"read", "studio", "notes", "list"}
