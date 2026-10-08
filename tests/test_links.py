import os
import stat
from types import SimpleNamespace

import pytest

from nlm_gateway.links import download_filename, load_or_create_key, make_token, share_url, verify_token

NOW = 1_760_000_000


def job(kind="podcast"):
    return SimpleNamespace(id="abcdef0123456789abcdef0123456789", consumer_id="research", kind=kind)


def test_key_persisted_0600(tmp_path):
    k1 = load_or_create_key(tmp_path / "link.key")
    assert k1 == load_or_create_key(tmp_path / "link.key") and len(k1) == 32
    assert stat.S_IMODE(os.stat(tmp_path / "link.key").st_mode) == 0o600


def test_filenames():
    assert download_filename(job()) == "podcast-abcdef01.m4a"
    assert download_filename(job("report")) == "report-abcdef01.md"


def test_roundtrip_and_tampering(tmp_path):
    key = load_or_create_key(tmp_path / "k")
    j = job()
    fn = download_filename(j)
    tok = make_token(key, j.id, j.consumer_id, NOW + 3600, fn)
    owner = {j.id: "research"}.get
    assert verify_token(key, tok, fn, owner, NOW) == j.id
    assert verify_token(key, tok, "other.m4a", owner, NOW) is None                      # filename bound
    assert verify_token(key, tok, fn, owner, NOW + 3601) is None                        # expired
    jid, exp, mac = tok.split(".")
    assert verify_token(key, f"{jid}.{int(exp) + 9999}.{mac}", fn, owner, NOW) is None  # exp bound
    assert verify_token(key, f"{jid}.{exp}.{'0' * len(mac)}", fn, owner, NOW) is None   # mac
    assert verify_token(key, tok, fn, lambda _id: None, NOW) is None                    # job gone / consumer revoked
    assert verify_token(key, tok, fn, lambda _id: "someone-else", NOW) is None          # consumer bound
    assert verify_token(key, "garbage", fn, owner, NOW) is None
    assert verify_token(os.urandom(32), tok, fn, owner, NOW) is None                    # other key


def test_share_url_shape(tmp_path, cfg):
    url, exp = share_url(cfg, load_or_create_key(tmp_path / "k"), job(), 24, NOW)
    assert exp == NOW + 24 * 3600
    assert url.startswith("https://gw.example/notebooklm/dl/abcdef0123456789abcdef0123456789.")
    assert url.endswith("/podcast-abcdef01.m4a")


def test_malformed_attacker_input_returns_none(tmp_path):
    key = load_or_create_key(tmp_path / "k")
    j = job()
    fn = download_filename(j)
    tok = make_token(key, j.id, j.consumer_id, NOW + 3600, fn)
    jid, exp, mac = tok.split(".")
    owner = {j.id: "research"}.get
    assert verify_token(key, f"{jid}.².{mac}", fn, owner, NOW) is None                  # non-ASCII digit exp
    assert verify_token(key, f"{jid}.{exp}.{'é' * len(mac)}", fn, owner, NOW) is None   # non-ASCII mac
    assert verify_token(key, f"{jid}.{exp}.{mac}\udcff", fn, owner, NOW) is None        # lone surrogate mac
    assert verify_token(key, f"{jid}é.{exp}.{mac}", fn, owner, NOW) is None             # non-ASCII job id


def test_empty_key_file_raises(tmp_path):
    p = tmp_path / "link.key"
    p.write_bytes(b"")
    with pytest.raises(RuntimeError, match="corrupt"):
        load_or_create_key(p)


def test_existing_valid_key_returned_unchanged(tmp_path):
    p = tmp_path / "link.key"
    raw = os.urandom(32)
    p.write_bytes(raw)
    os.chmod(p, 0o600)
    assert load_or_create_key(p) == raw
    assert p.read_bytes() == raw
    assert sorted(x.name for x in tmp_path.iterdir()) == ["link.key"]   # no temp leftovers


def test_oversized_exp_returns_none(tmp_path):
    key = load_or_create_key(tmp_path / "k")
    j = job()
    fn = download_filename(j)
    tok = make_token(key, j.id, j.consumer_id, NOW + 3600, fn)
    jid, exp, mac = tok.split(".")
    owner = {j.id: "research"}.get
    assert verify_token(key, f"{jid}.{'9' * 5000}.{mac}", fn, owner, NOW) is None


def test_studio_filenames():
    j = SimpleNamespace(id="abcdef0123456789abcdef0123456789", consumer_id="r", kind="slides", params={"format": "pdf"})
    assert download_filename(j) == "slides-abcdef01.pdf"
    j.params = {"format": "pptx"}
    assert download_filename(j) == "slides-abcdef01.pptx"
    j.kind, j.params = "infographic", {}
    assert download_filename(j) == "infographic-abcdef01.png"


def test_source_ref_roundtrip_and_binding(tmp_path):
    from nlm_gateway.links import make_source_ref, verify_source_ref
    key = load_or_create_key(tmp_path / "k")
    ref = make_source_ref(key, "nb-1", "src-1", "notes", NOW + 3600)
    assert verify_source_ref(key, ref, "notes", NOW) == ("nb-1", "src-1")
    assert verify_source_ref(key, ref, "other", NOW) is None             # consumer bound
    assert verify_source_ref(key, ref, "notes", NOW + 3601) is None      # expired
    assert verify_source_ref(key, ref[:-1] + ("0" if ref[-1] != "0" else "1"), "notes", NOW) is None
    assert verify_source_ref(b"\x01" * 32, ref, "notes", NOW) is None    # other key
    for junk in ("", "x", "a.b", "a.b.c", "....", "é.1.x", "e30.99999999999999.ab"):
        assert verify_source_ref(key, junk, "notes", NOW) is None


def test_source_ref_not_a_dl_token(tmp_path):
    """Same key, different domain: a /dl/ token never verifies as a source ref and vice versa."""
    from nlm_gateway.links import make_source_ref, verify_source_ref
    key = load_or_create_key(tmp_path / "k")
    tok = make_token(key, "nb-1", "notes", NOW + 3600, "src-1")
    assert verify_source_ref(key, tok, "notes", NOW) is None
    ref = make_source_ref(key, "nb-1", "src-1", "notes", NOW + 3600)
    assert verify_token(key, ref, "x", lambda _id: "notes", NOW) is None
