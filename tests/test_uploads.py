import os

import httpx
import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from nlm_gateway.errors import GatewayError, UploadTooLarge
from nlm_gateway.uploads import receive_upload, safe_filename, sniff_ok

PDF = b"%PDF-1.7\n" + b"x" * 100


@pytest.mark.parametrize("raw,expected", [
    ("report.pdf", "report.pdf"),
    ("../../etc/passwd.pdf", "passwd.pdf"),
    ("..\\..\\win\\evil.docx", "evil.docx"),
    ("na\x00me\x07.pdf", "name.pdf"),
    ("...hidden.md", "hidden.md"),
    ("x" * 300 + ".pdf", "x" * 100 + ".pdf"),
])
def test_safe_filename(raw, expected):
    assert safe_filename(raw) == expected


@pytest.mark.parametrize("raw", ["", "noext", "../", "\x00\x01"])
def test_safe_filename_rejects(raw):
    with pytest.raises(GatewayError):
        safe_filename(raw)


def test_sniff():
    assert sniff_ok(".pdf", PDF)
    assert not sniff_ok(".pdf", b"PK\x03\x04")
    assert sniff_ok(".docx", b"PK\x03\x04rest")
    assert sniff_ok(".m4a", b"\x00\x00\x00\x18ftypM4A ")
    assert sniff_ok(".txt", "héllo".encode())
    assert not sniff_ok(".txt", b"abc\x00def")
    assert sniff_ok(".md", "é".encode() * 2048 + "é".encode()[:1])  # multibyte cut at the head boundary


def app_for(cfg, store, consumer):
    async def put(request):
        try:
            up = await receive_upload(request, cfg, store, consumer, 1000.0)
        except UploadTooLarge as e:
            return JSONResponse(e.to_dict(), status_code=413)
        except GatewayError as e:
            return JSONResponse(e.to_dict(), status_code=400)
        return JSONResponse({"upload_id": up.id, "path": up.path, "filename": up.filename, "size": up.size_bytes})
    return Starlette(routes=[Route("/u", put, methods=["PUT"])])


async def put(app, filename, body):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        return await c.put("/u", params={"filename": filename}, content=body)


async def test_receive_ok_stored_under_opaque_name(cfg, store, make_consumer):
    r = await put(app_for(cfg, store, make_consumer()), "../../secret-name.pdf", PDF)
    d = r.json()
    assert r.status_code == 200 and d["filename"] == "secret-name.pdf" and d["size"] == len(PDF)
    assert os.path.basename(d["path"]) == d["upload_id"] and "secret" not in d["path"]
    assert os.stat(d["path"]).st_mode & 0o077 == 0


async def test_receive_rejects_mismatch_and_cleans_up(cfg, store, make_consumer):
    r = await put(app_for(cfg, store, make_consumer()), "a.pdf", b"PK\x03\x04zip")
    assert r.status_code == 400
    assert list((cfg.state_dir / "uploads").iterdir()) == []


async def test_receive_rejects_type_and_empty(cfg, store, make_consumer):
    app = app_for(cfg, store, make_consumer())
    assert (await put(app, "a.exe", b"MZ")).status_code == 400
    assert (await put(app, "a.pdf", b"")).status_code == 400


async def test_receive_size_cap(cfg, store, make_consumer):
    from dataclasses import replace
    small = replace(cfg, upload_max_bytes=50)
    r = await put(app_for(small, store, make_consumer()), "a.pdf", PDF)
    assert r.status_code == 413
    assert list((cfg.state_dir / "uploads").iterdir()) == []


async def chunked(parts):
    for p in parts:
        yield p


async def test_receive_streaming_size_cap_without_content_length(cfg, store, make_consumer):
    from dataclasses import replace
    small = replace(cfg, upload_max_bytes=50)
    body = chunked([PDF[:40], PDF[40:]])  # async generator: httpx sends no Content-Length
    r = await put(app_for(small, store, make_consumer()), "a.pdf", body)
    assert r.status_code == 413
    assert list((cfg.state_dir / "uploads").iterdir()) == []


async def test_receive_store_failure_removes_file(cfg, store, make_consumer, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("db down")
    monkeypatch.setattr(store, "add_upload", boom)
    with pytest.raises(RuntimeError):
        await put(app_for(cfg, store, make_consumer()), "a.pdf", PDF)
    assert list((cfg.state_dir / "uploads").iterdir()) == []


async def test_type_error_does_not_echo_filename(cfg, store, make_consumer):
    r = await put(app_for(cfg, store, make_consumer()), "secretname.exe", b"MZ")
    assert r.status_code == 400
    assert "secretname" not in r.text and ".exe" not in r.text


async def test_pending_cap_from_config(cfg, store, make_consumer):
    from dataclasses import replace
    app = app_for(replace(cfg, max_pending_uploads=3), store, make_consumer())
    for _ in range(3):
        assert (await put(app, "a.pdf", PDF)).status_code == 200
    r = await put(app, "a.pdf", PDF)
    assert r.status_code == 400 and r.json()["message"].endswith("(max 3)")
    assert len(list((cfg.state_dir / "uploads").iterdir())) == 3
