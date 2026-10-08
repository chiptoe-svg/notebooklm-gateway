import asyncio
import contextlib
import json
import socket
import threading
import time

import httpx
import pytest
import uvicorn
from mcp import ClientSession
import httpx2
from mcp.client.streamable_http import streamable_http_client

from nlm_gateway.http_app import create_app

PDF = b"%PDF-1.7 hello"


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture
def server(gw):
    full = gw.registry.pair("research", ["podcast", "report"], "research", 3, 10)
    report_only = gw.registry.pair("reports", ["report"], "reports", 0, 10)
    lister = gw.registry.pair("lister", ["report", "list"], "To Review", 0, 10)
    reader = gw.registry.pair("reader", ["read"], "To Review", 0, 0)
    studio = gw.registry.pair("studio", ["studio"], "To Review", 0, 0)
    notes = gw.registry.pair("notes", ["notes"], "To Review", 0, 0)
    notes2 = gw.registry.pair("notes2", ["notes"], "To Review", 0, 0)
    port = free_port()
    srv = uvicorn.Server(uvicorn.Config(create_app(gw), host="127.0.0.1", port=port, lifespan="on", log_level="warning"))
    t = threading.Thread(target=srv.run, daemon=True)
    t.start()
    for _ in range(100):
        if srv.started:
            break
        time.sleep(0.05)
    yield {"base": f"http://127.0.0.1:{port}/notebooklm", "full": full, "report_only": report_only, "lister": lister, "reader": reader,
           "studio": studio, "notes": notes, "notes2": notes2, "gw": gw}
    srv.should_exit = True
    t.join(5)


@contextlib.asynccontextmanager
async def mcp_client(url, tok):
    # mcp 2.x: headers go on a caller-supplied httpx2 client (1.x took headers=); yields (read, write)
    async with httpx2.AsyncClient(headers=auth(tok), timeout=30) as hc:
        async with streamable_http_client(url, http_client=hc) as streams:
            yield streams


def auth(tok):
    return {"Authorization": f"Bearer {tok}"}


async def test_healthz_public_and_no_secrets(server):
    r = httpx.get(f"{server['base']}/healthz")
    assert r.status_code == 200 and r.json()["auth"] == "ok" and "token" not in r.text


async def test_auth_required(server):
    for path, method in (("/mcp", "POST"), ("/uploads?filename=a.pdf", "PUT"), ("/jobs/x/download", "GET")):
        assert httpx.request(method, server["base"] + path).status_code == 401
        assert httpx.request(method, server["base"] + path, headers=auth("nlm_bogus")).status_code == 401


async def test_mcp_body_cap(server):
    r = httpx.post(f"{server['base']}/mcp", headers={**auth(server["full"]), "content-type": "application/json"},
                   content=b"x" * (1024 * 1024 + 1))
    assert r.status_code == 413


async def test_full_flow_over_real_mcp_client(server):
    base, tok = server["base"], server["full"]
    up = httpx.put(f"{base}/uploads", params={"filename": "paper.pdf"}, content=PDF, headers=auth(tok)).json()
    async with mcp_client(f"{base}/mcp", tok) as (r, w):
        async with ClientSession(r, w) as s:
            await s.initialize()
            names = [t.name for t in (await s.list_tools()).tools]
            assert names == ["podcast_from_sources", "report_from_sources", "job_status", "quota_status", "share_link"]
            t0 = time.monotonic()
            res = await s.call_tool("podcast_from_sources", {"urls": ["https://a.example/x"], "upload_ids": [up["upload_id"]]})
            assert time.monotonic() - t0 < 60 and not res.is_error
            job = json.loads(res.content[0].text)
            for _ in range(100):
                st = json.loads((await s.call_tool("job_status", {"job_id": job["job_id"]})).content[0].text)
                if st["status"] in ("done", "failed"):
                    break
                await asyncio.sleep(0.1)
            assert st["status"] == "done", st
            bad = await s.call_tool("job_status", {"job_id": "nope"})
            assert bad.is_error and json.loads(bad.content[0].text)["code"] == "not_found"
            link = json.loads((await s.call_tool("share_link", {"job_id": job["job_id"]})).content[0].text)
    dl = httpx.get(st["download_url"].replace("https://gw.example/notebooklm", base), headers=auth(tok))
    assert dl.status_code == 200 and dl.headers["content-type"] == "audio/mp4" and "attachment" in dl.headers["content-disposition"]
    assert dl.content == server["gw"].nlm.client.audio_bytes
    signed = link["url"].replace("https://gw.example/notebooklm", base)
    assert httpx.get(signed).status_code == 200                                      # no bearer needed
    assert httpx.get(signed, headers={"Range": "bytes=0-9"}).status_code == 206       # resumable
    assert httpx.head(signed).status_code == 200
    assert httpx.get(signed[:-3] + "mp3").status_code == 404                         # filename bound
    other = server["report_only"]
    assert httpx.get(st["download_url"].replace("https://gw.example/notebooklm", base), headers=auth(other)).status_code == 404
    server["gw"].registry.revoke("research")
    assert httpx.get(signed).status_code == 404                                      # revocation kills links
    assert list((server["gw"].cfg.state_dir / "tmp").iterdir()) == []


async def test_scope_filtered_tool_list(server):
    async with mcp_client(f"{server['base']}/mcp", server["report_only"]) as (r, w):
        async with ClientSession(r, w) as s:
            await s.initialize()
            names = [t.name for t in (await s.list_tools()).tools]
            assert "podcast_from_sources" not in names and "report_from_sources" in names and "list_notebooks" not in names
            res = await s.call_tool("podcast_from_sources", {"urls": ["https://x"]})
            assert res.is_error and json.loads(res.content[0].text)["code"] == "not_found"
            res = await s.call_tool("list_notebooks", {})
            assert res.is_error and json.loads(res.content[0].text)["code"] == "not_found"


async def test_list_notebooks_over_real_mcp_client(server):
    nb = await server["gw"].nlm.client.notebooks.create("Reading list")
    async with mcp_client(f"{server['base']}/mcp", server["lister"]) as (r, w):
        async with ClientSession(r, w) as s:
            await s.initialize()
            names = [t.name for t in (await s.list_tools()).tools]
            assert names == ["report_from_sources", "job_status", "quota_status", "share_link", "list_notebooks"]
            t0 = time.monotonic()
            res = await s.call_tool("list_notebooks", {})
            assert time.monotonic() - t0 < 60 and not res.is_error
            assert json.loads(res.content[0].text) == {"notebooks": [
                {"title": "Reading list", "sources": 0, "url": f"https://notebooklm.example/notebook/{nb.id}"}]}


async def test_upload_errors(server):
    base, tok = server["base"], server["full"]
    assert httpx.put(f"{base}/uploads", params={"filename": "a.exe"}, content=b"MZ", headers=auth(tok)).status_code == 400


async def _signed_link(server):
    base, tok = server["base"], server["full"]
    async with mcp_client(f"{base}/mcp", tok) as (r, w):
        async with ClientSession(r, w) as s:
            await s.initialize()
            res = await s.call_tool("podcast_from_sources", {"urls": ["https://a.example/x"]})
            job = json.loads(res.content[0].text)
            for _ in range(100):
                st = json.loads((await s.call_tool("job_status", {"job_id": job["job_id"]})).content[0].text)
                if st["status"] == "done":
                    break
                await asyncio.sleep(0.1)
            assert st["status"] == "done", st
            link = json.loads((await s.call_tool("share_link", {"job_id": job["job_id"]})).content[0].text)
    return link["url"].replace("https://gw.example/notebooklm", base), job["job_id"]


async def test_bad_range_leaves_no_temp_files(server):
    signed, _ = await _signed_link(server)
    tmp = server["gw"].cfg.state_dir / "tmp"
    assert httpx.get(signed, headers={"Range": "bytes=999999-"}).status_code == 416
    assert httpx.get(signed, headers={"Range": "garbage"}).status_code == 400
    assert not tmp.exists() or list(tmp.iterdir()) == []


async def test_head_does_not_fetch_or_audit(server):
    signed, job_id = await _signed_link(server)
    gw = server["gw"]
    n = len(gw.nlm.client.downloads)
    audit_before = gw.cfg.state_dir.joinpath("audit.jsonl").read_text()
    r = httpx.head(signed)
    assert r.status_code == 200 and r.headers["content-type"].startswith("audio/mp4")
    assert "attachment" in r.headers["content-disposition"] and r.headers["accept-ranges"] == "bytes"
    assert int(r.headers["content-length"]) == gw.store.get_job(job_id).size_bytes
    assert len(gw.nlm.client.downloads) == n
    assert gw.cfg.state_dir.joinpath("audit.jsonl").read_text() == audit_before


async def test_chunked_mcp_body_over_cap_413(server):
    def chunks():
        for _ in range(1100):
            yield b"x" * 1024
    r = httpx.post(f"{server['base']}/mcp", headers={**auth(server["full"]), "content-type": "application/json"}, content=chunks())
    assert r.status_code == 413


@pytest.mark.parametrize("bad", ["{", '{"consumers": [{"id": 1}]}', '{"consumers": "x"}', "[]", ""])
async def test_corrupt_registry_signed_link_404(server, bad):
    signed, _ = await _signed_link(server)
    server["gw"].registry.path.write_text(bad)
    assert httpx.get(signed).status_code == 404
    assert httpx.head(signed).status_code == 404


def _count_concurrent_downloads(gw):
    """Wrap the fake's download_audio (runs on the server's loop) to record peak concurrency."""
    client, real = gw.nlm.client, gw.nlm.client.artifacts.download_audio
    stats = {"now": 0, "peak": 0, "calls": 0}

    async def counted(*a, **kw):
        stats["now"] += 1
        stats["calls"] += 1
        stats["peak"] = max(stats["peak"], stats["now"])
        try:
            await asyncio.sleep(0.2)
            return await real(*a, **kw)
        finally:
            stats["now"] -= 1
    client.artifacts.download_audio = counted
    return stats


async def test_signed_link_fetches_one_at_a_time(server):
    signed, _ = await _signed_link(server)
    stats = _count_concurrent_downloads(server["gw"])
    async with httpx.AsyncClient(timeout=30) as c:
        rs = await asyncio.gather(*(c.get(signed) for _ in range(5)))
    assert [r.status_code for r in rs] == [200] * 5
    assert stats["calls"] == 5 and stats["peak"] == 1


async def test_google_downloads_capped_at_two(server):
    gw = server["gw"]
    now = gw.now()
    ids = []
    for _ in range(4):
        j = gw.store.create_job("research", "podcast", {"format": "brief", "length": "short", "language": "en"}, [], [], now)
        gw.store.update_job(j.id, now, status="done", notebook_id="nb", task_id="t", artifact_id="a", size_bytes=10, mime="audio/mp4")
        ids.append(j.id)
    stats = _count_concurrent_downloads(gw)
    async with httpx.AsyncClient(timeout=30, headers=auth(server["full"])) as c:
        rs = await asyncio.gather(*(c.get(f"{server['base']}/jobs/{i}/download") for i in ids))
    assert [r.status_code for r in rs] == [200] * 4
    assert stats["calls"] == 4 and stats["peak"] == 2


async def test_audit_failure_after_fetch_leaves_no_temp_file(server):
    signed, _ = await _signed_link(server)
    gw = server["gw"]
    real = gw.audit.write

    def boom(event, **kw):
        if event == "signed_download":
            raise OSError("disk full")
        return real(event, **kw)
    gw.audit.write = boom
    assert httpx.get(signed).status_code == 500
    tmp = gw.cfg.state_dir / "tmp"
    assert not tmp.exists() or list(tmp.iterdir()) == []


async def test_pending_upload_cap(server):
    base, tok = server["base"], server["full"]
    for _ in range(10):
        assert httpx.put(f"{base}/uploads", params={"filename": "a.pdf"}, content=PDF, headers=auth(tok)).status_code == 200
    r = httpx.put(f"{base}/uploads", params={"filename": "a.pdf"}, content=PDF, headers=auth(tok))
    assert r.status_code == 429 and r.json() == {"code": "bad_request", "message": "too many pending uploads; use or let them expire (max 10)"}
    assert len(list((server["gw"].cfg.state_dir / "uploads").iterdir())) == 10
    other = server["report_only"]  # the cap is per consumer
    assert httpx.put(f"{base}/uploads", params={"filename": "a.pdf"}, content=PDF, headers=auth(other)).status_code == 200


COMMON = ["job_status", "quota_status", "share_link"]
SCOPED = {"reader": ["list_sources", "ask_notebook"], "studio": ["slides_from_sources", "infographic_from_sources"],
          "notes": ["read_note", "create_note", "get_source_text"]}


async def test_tools_per_new_scope_over_real_mcp_client(server):
    for who, own in SCOPED.items():
        async with mcp_client(f"{server['base']}/mcp", server[who]) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                names = [t.name for t in (await s.list_tools()).tools]
                assert names == COMMON + own, who
                for other, theirs in SCOPED.items():
                    if other == who:
                        continue
                    for name in theirs:
                        res = await s.call_tool(name, {"notebook": "x"})
                        assert res.is_error and json.loads(res.content[0].text)["code"] == "not_found", (who, name)


async def test_read_and_studio_over_real_mcp_client(server):
    gw = server["gw"]
    nb = await gw.nlm.client.notebooks.create("Reading list")
    src = gw.nlm.client._add(nb.id, "Fresnel lens")
    gw.nlm.client.ask_refs = [(src.id, "excerpt")]
    async with mcp_client(f"{server['base']}/mcp", server["reader"]) as (r, w):
        async with ClientSession(r, w) as s:
            await s.initialize()
            res = await s.call_tool("list_sources", {"notebook": "Reading list"})
            assert json.loads(res.content[0].text)["sources"][0]["title"] == "Fresnel lens"
            res = await s.call_tool("ask_notebook", {"notebook": "Reading list", "question": "How?"})
            assert json.loads(res.content[0].text)["citations"] == [{"source_title": "Fresnel lens", "excerpt": "excerpt"}]
    async with mcp_client(f"{server['base']}/mcp", server["studio"]) as (r, w):
        async with ClientSession(r, w) as s:
            await s.initialize()
            res = await s.call_tool("slides_from_sources", {"urls": ["https://a.example/x"], "format": "pptx"})
            job = json.loads(res.content[0].text)
            for _ in range(100):
                st = json.loads((await s.call_tool("job_status", {"job_id": job["job_id"]})).content[0].text)
                if st["status"] in ("done", "failed"):
                    break
                await asyncio.sleep(0.1)
            assert st["status"] == "done", st
    dl = httpx.get(st["download_url"].replace("https://gw.example/notebooklm", server["base"]), headers=auth(server["studio"]))
    assert dl.status_code == 200 and dl.content == gw.nlm.client.slides_bytes["pptx"]
    assert dl.headers["content-type"] == "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    assert f'filename="slides-{job["job_id"][:8]}.pptx"' in dl.headers["content-disposition"]


async def _source_text_url(server, nb_title="Reading & list", src_title="Long source", text=None):
    gw, base = server["gw"], server["base"]
    nb = await gw.nlm.client.notebooks.create(nb_title)
    src = gw.nlm.client._add(nb.id, src_title)
    src.text = text if text is not None else "ü" * (gw.cfg.inline_text_max + 10)
    async with mcp_client(f"{base}/mcp", server["notes"]) as (r, w):
        async with ClientSession(r, w) as s:
            await s.initialize()
            res = await s.call_tool("get_source_text", {"notebook": nb_title, "source_title": src_title})
            out = json.loads(res.content[0].text)
    assert out["truncated"] is True
    return out["download_url"].replace("https://gw.example/notebooklm", base), nb, src


async def test_source_text_download_route(server):
    gw, base = server["gw"], server["base"]
    url, nb, src = await _source_text_url(server)
    assert "Reading" not in url and "Long" not in url and "?" not in url
    r = httpx.get(url, headers=auth(server["notes"]))
    assert r.status_code == 200 and r.headers["content-type"] == "text/plain; charset=utf-8" and r.text == src.text
    assert httpx.get(url).status_code == 401
    assert httpx.get(url, headers=auth(server["reader"])).status_code == 404       # bearer without scope notes
    assert httpx.get(url, headers=auth(server["notes2"])).status_code == 404       # ref minted for another consumer
    ref = url.rsplit("/", 1)[1]
    tampered = ref[:-1] + ("0" if ref[-1] != "0" else "1")
    assert httpx.get(f"{base}/sources/text/{tampered}", headers=auth(server["notes"])).status_code == 404
    for junk in ("x", "a.b.c", "....", "%E2%80%AE"):
        assert httpx.get(f"{base}/sources/text/{junk}", headers=auth(server["notes"])).status_code == 404
    assert httpx.get(f"{base}/sources/text?notebook=Reading+%26+list&source=Long+source",
                     headers=auth(server["notes"])).status_code == 404                     # old title-based form is gone
    gw.nlm.auth_ok = False
    assert httpx.get(url, headers=auth(server["notes"])).status_code == 503
    gw.nlm.auth_ok = True
    tmp = gw.cfg.state_dir / "tmp"
    assert not tmp.exists() or list(tmp.iterdir()) == []
    events = [json.loads(x) for x in (gw.cfg.state_dir / "audit.jsonl").read_text().splitlines()]
    dl = [e for e in events if e["event"] == "source_text_download"]
    assert len(dl) == 1 and dl[0]["consumer"] == "notes" and dl[0]["chars"] == len(src.text) and "Long" not in json.dumps(events)


async def test_source_text_ref_expires(server):
    gw = server["gw"]
    url, _, _ = await _source_text_url(server)
    assert httpx.get(url, headers=auth(server["notes"])).status_code == 200
    real_now = gw.now
    gw.now = lambda: real_now() + 3601
    try:
        assert httpx.get(url, headers=auth(server["notes"])).status_code == 404
    finally:
        gw.now = real_now


async def test_source_text_deleted_source_404(server):
    gw = server["gw"]
    url, nb, src = await _source_text_url(server)
    await gw.nlm.client.sources.delete(nb.id, src.id)
    assert httpx.get(url, headers=auth(server["notes"])).status_code == 404


async def test_source_text_head_405_no_fetch_no_audit(server):
    gw = server["gw"]
    url, _, _ = await _source_text_url(server)
    calls = len(gw.nlm.client.fulltext_calls)
    audit_before = gw.cfg.state_dir.joinpath("audit.jsonl").read_text()
    assert httpx.head(url, headers=auth(server["notes"])).status_code == 405
    assert len(gw.nlm.client.fulltext_calls) == calls
    assert gw.cfg.state_dir.joinpath("audit.jsonl").read_text() == audit_before


@pytest.mark.parametrize("exc,status", [("rate", 429), ("quota", 429)])
async def test_source_text_rate_limit_maps_to_429(server, exc, status):
    from nlm_gateway.errors import GatewayError
    from notebooklm import RateLimitError
    gw = server["gw"]
    url, _, _ = await _source_text_url(server)

    async def limited(*a, **kw):
        raise RateLimitError("slow down") if exc == "rate" else GatewayError("quota_exceeded", "x")
    gw.nlm.client.sources.get_fulltext = limited
    r = httpx.get(url, headers=auth(server["notes"]))
    assert r.status_code == status and r.json()["code"] in ("rate_limited", "quota_exceeded")


async def test_source_text_route_cleans_up_on_audit_failure(server):
    gw = server["gw"]
    url, _, _ = await _source_text_url(server, "NB", "S")
    real = gw.audit.write

    def boom(event, **kw):
        if event == "source_text_download":
            raise OSError("disk full")
        return real(event, **kw)
    gw.audit.write = boom
    assert httpx.get(url, headers=auth(server["notes"])).status_code == 500
    tmp = gw.cfg.state_dir / "tmp"
    assert not tmp.exists() or list(tmp.iterdir()) == []
