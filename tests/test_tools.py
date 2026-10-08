import pytest

from nlm_gateway import tools
from nlm_gateway.errors import GatewayError

PDF = b"%PDF-1.7 hi"


async def run_all(gw):
    while (job := gw.store.claim_next(gw.now())) is not None:
        await gw.runner.run_job(job)


async def test_podcast_submit_and_status(gw, make_consumer):
    c = make_consumer()
    out = await tools.podcast_from_sources(gw, c, {"urls": ["https://a.example/x"], "format": "debate", "length": "long"})
    assert out["status"] == "queued" and out["notebook_url"].startswith("https://notebooklm.example/notebook/")
    st = await tools.job_status(gw, c, {"job_id": out["job_id"]})
    assert st["status"] == "queued" and "download_url" not in st
    await run_all(gw)
    st = await tools.job_status(gw, c, {"job_id": out["job_id"]})
    assert st["status"] == "done" and st["download_url"] == f"https://gw.example/notebooklm/jobs/{out['job_id']}/download"
    assert st["bytes"] > 0 and st["mime"] == "audio/mp4" and "share_url" not in st


async def test_big_file_gets_share_url(gw, make_consumer):
    c = make_consumer()
    gw.nlm.client.audio_bytes = b"\x00" * (gw.cfg.big_file_bytes + 1)
    out = await tools.podcast_from_sources(gw, c, {"urls": ["https://a.example/x"]})
    await run_all(gw)
    st = await tools.job_status(gw, c, {"job_id": out["job_id"]})
    assert st["share_url"].startswith("https://gw.example/notebooklm/dl/") and "share_expires_at" in st


async def test_report_text_inline(gw, make_consumer):
    c = make_consumer()
    out = await tools.report_from_sources(gw, c, {"urls": ["https://a.example/x"], "kind": "blog", "return_text": True})
    await run_all(gw)
    st = await tools.job_status(gw, c, {"job_id": out["job_id"]})
    assert st["text"] == gw.nlm.client.report_text and st["mime"].startswith("text/markdown")


@pytest.mark.parametrize("args", [
    {},
    {"urls": []},
    {"urls": ["ftp://x"]},
    {"urls": ["https://x"], "format": "lecture"},
    {"urls": ["https://x"], "length": "huge"},
    {"urls": ["https://x"], "language": "english!"},
    {"urls": ["https://x"], "instructions": "x" * 2001},
    {"urls": "https://x"},
    {"urls": ["https://x"] * 51},
])
async def test_bad_requests(gw, make_consumer, args):
    with pytest.raises(GatewayError) as e:
        await tools.podcast_from_sources(gw, make_consumer(), args)
    assert e.value.code == "bad_request"
    assert gw.store.count_billable("podcast", 0) == 0 and gw.nlm.client.creates == []


async def test_scope_enforced(gw, make_consumer):
    with pytest.raises(GatewayError) as e:
        await tools.podcast_from_sources(gw, make_consumer(scopes=("report",)), {"urls": ["https://x"]})
    assert e.value.code == "not_found"


async def test_upload_reuse_and_foreign_upload_rejected(gw, make_consumer):  # Review Focus 4
    a, b = make_consumer(id="a"), make_consumer(id="b")
    p = gw.cfg.state_dir / "u1"
    p.write_bytes(PDF)
    up = gw.store.add_upload("a", "x.pdf", p, len(PDF), gw.now(), 7200)
    with pytest.raises(GatewayError) as e:
        await tools.podcast_from_sources(gw, b, {"upload_ids": [up.id]})
    assert e.value.code == "bad_request"
    await tools.podcast_from_sources(gw, a, {"upload_ids": [up.id]})
    with pytest.raises(GatewayError):
        await tools.podcast_from_sources(gw, a, {"upload_ids": [up.id]})
    assert len(gw.nlm.client.creates) == 1  # the rejected calls never touched Google
    assert gw.store.count_billable("podcast", 0, "a") == 1 and gw.store.count_billable("podcast", 0, "b") == 0


async def test_quota_exceeded_before_google(gw, make_consumer):
    c = make_consumer(audio=1)
    await tools.podcast_from_sources(gw, c, {"urls": ["https://x"]})
    with pytest.raises(GatewayError) as e:
        await tools.podcast_from_sources(gw, c, {"urls": ["https://y"]})
    assert e.value.code == "quota_exceeded"


async def test_auth_expired_fails_fast(gw, make_consumer):
    gw.nlm.auth_ok = False
    with pytest.raises(GatewayError) as e:
        await tools.podcast_from_sources(gw, make_consumer(), {"urls": ["https://x"]})
    assert e.value.code == "auth_expired" and gw.store.count_billable("podcast", 0) == 0


async def test_job_status_isolation(gw, make_consumer):
    out = await tools.podcast_from_sources(gw, make_consumer(id="a"), {"urls": ["https://x"]})
    with pytest.raises(GatewayError) as e:
        await tools.job_status(gw, make_consumer(id="b"), {"job_id": out["job_id"]})
    assert e.value.code == "not_found"


async def test_share_link(gw, make_consumer):
    c = make_consumer()
    out = await tools.podcast_from_sources(gw, c, {"urls": ["https://x"]})
    with pytest.raises(GatewayError):
        await tools.share_link(gw, c, {"job_id": out["job_id"]})  # not done yet
    await run_all(gw)
    s = await tools.share_link(gw, c, {"job_id": out["job_id"], "ttl_hours": 9999})
    assert s["expires_at"] <= int(gw.now()) + 168 * 3600 and s["bytes"] > 0


async def test_quota_status(gw, make_consumer):
    c = make_consumer()
    await tools.podcast_from_sources(gw, c, {"urls": ["https://x"]})
    q = await tools.quota_status(gw, c, {})
    assert q["audio"] == {"used": 1, "cap": 3}


def test_tool_specs():
    names = [t.name for t in tools.TOOLS]
    assert names[:6] == ["podcast_from_sources", "report_from_sources", "job_status", "quota_status", "share_link", "list_notebooks"]
    assert {t.name: t.scope for t in tools.TOOLS}["list_notebooks"] == "list"
    for t in tools.TOOLS:
        assert "podcast" in t.description.lower() or "report" in t.description.lower() or "notebooklm" in t.description.lower()
        assert t.input_schema["type"] == "object"


async def test_claim_uploads_unexpected_error_fails_the_job(gw, make_consumer, monkeypatch):
    import sqlite3

    def boom(*a, **kw):
        raise sqlite3.OperationalError("database is locked")
    monkeypatch.setattr(gw.store, "claim_uploads", boom)
    with pytest.raises(GatewayError):
        await tools.podcast_from_sources(gw, make_consumer(), {"urls": ["https://x"]})
    assert gw.store.count_status("new") == 0 and gw.store.count_billable("podcast", 0) == 0


async def test_cancel_during_resolve_propagates(gw, make_consumer):
    import asyncio
    gate = asyncio.Event()

    async def stuck_list():
        await gate.wait()
    gw.nlm.client.notebooks.list = stuck_list
    t = asyncio.create_task(tools.podcast_from_sources(gw, make_consumer(), {"urls": ["https://x"]}))
    await asyncio.sleep(0.05)
    t.cancel()
    (res,) = await asyncio.gather(t, return_exceptions=True)
    assert isinstance(res, asyncio.CancelledError)  # not converted into a GatewayError; recover() handles the 'new' job


async def test_default_notebook_is_the_consumer_label(gw, make_consumer):
    c = make_consumer(label="To Review")
    out = await tools.podcast_from_sources(gw, c, {"urls": ["https://a.example/x"]})
    assert gw.store.get_job(out["job_id"]).notebook_title == "To Review" and gw.nlm.client.creates == ["To Review"]


async def test_named_notebook_reuses_existing_or_creates(gw, make_consumer):
    c = make_consumer(label="To Review")
    mine = await gw.nlm.client.notebooks.create("Lighthouses")
    gw.nlm.client.creates.clear()
    out = await tools.report_from_sources(gw, c, {"urls": ["https://a.example/x"], "notebook": "  Lighthouses "})
    job = gw.store.get_job(out["job_id"])
    assert job.notebook_id == mine.id and job.notebook_title == "Lighthouses" and gw.nlm.client.creates == []
    out = await tools.report_from_sources(gw, c, {"urls": ["https://a.example/y"], "notebook": "Tide pools"})
    assert gw.store.get_job(out["job_id"]).notebook_title == "Tide pools" and gw.nlm.client.creates == ["Tide pools"]


SECRET = "zq-secret-zq"


@pytest.mark.parametrize("key,value", [
    ("notebook", ""), ("notebook", "   "), ("notebook", SECRET + "x" * 100), ("notebook", SECRET + "\na"),
    ("notebook", SECRET + "\x07"), ("notebook", 5), ("notebook", [SECRET]),
    ("title", ""), ("title", "  "), ("title", SECRET + "x" * 100), ("title", SECRET + "\ra"), ("title", 7),
])
async def test_bad_notebook_or_title_rejected_without_echo(gw, make_consumer, key, value):
    with pytest.raises(GatewayError) as e:
        await tools.podcast_from_sources(gw, make_consumer(), {"urls": ["https://x"], key: value})
    assert e.value.code == "bad_request" and SECRET not in e.value.message and "xxxx" not in e.value.message
    assert gw.store.count_billable("podcast", 0) == 0 and gw.nlm.client.creates == []


async def test_title_stored_in_params(gw, make_consumer):
    c = make_consumer()
    out = await tools.podcast_from_sources(gw, c, {"urls": ["https://x"], "title": "  Lighthouses — history and tech "})
    assert gw.store.get_job(out["job_id"]).params["title"] == "Lighthouses — history and tech"
    out = await tools.report_from_sources(gw, c, {"urls": ["https://x"]})
    assert gw.store.get_job(out["job_id"]).params["title"] is None


async def test_list_notebooks(gw, make_consumer):
    import json
    from datetime import datetime, timezone
    cl = gw.nlm.client
    old = await cl.notebooks.create("Old one")
    new = await cl.notebooks.create("Reading · notes")
    undated = await cl.notebooks.create("Undated")
    undated.created_at = None
    cl._add(new.id, "https://secret.example/a")
    cl._add(new.id, "file.pdf")
    out = await tools.list_notebooks(gw, make_consumer(scopes=("list",)), {})
    assert out == {"notebooks": [
        {"title": "Reading · notes", "sources": 2, "url": f"https://notebooklm.example/notebook/{new.id}"},
        {"title": "Old one", "sources": 0, "url": f"https://notebooklm.example/notebook/{old.id}"},
        {"title": "Undated", "sources": 0, "url": f"https://notebooklm.example/notebook/{undated.id}"},
    ]}
    assert old.created_at < new.created_at <= datetime.now(timezone.utc)
    audit = (gw.cfg.state_dir / "audit.jsonl").read_text()
    last = json.loads(audit.splitlines()[-1])
    assert last["event"] == "list_notebooks" and last["count"] == 3
    assert "Reading" not in audit and "secret" not in audit and "notebooklm.example" not in audit


async def test_list_notebooks_async_share_url(gw, make_consumer):
    cl = gw.nlm.client
    await cl.notebooks.create("A")

    async def share(nb_id, artifact_id=None):
        return f"https://async.example/{nb_id}"
    cl.notebooks.get_share_url = share
    out = await tools.list_notebooks(gw, make_consumer(scopes=("list",)), {})
    assert out["notebooks"][0]["url"].startswith("https://async.example/")


async def test_list_notebooks_needs_list_scope(gw, make_consumer):
    await gw.nlm.client.notebooks.create("A")
    with pytest.raises(GatewayError) as e:
        await tools.list_notebooks(gw, make_consumer(scopes=("podcast", "report")), {})
    assert e.value.code == "not_found"


async def test_list_notebooks_auth_expired(gw, make_consumer):
    gw.nlm.auth_ok = False
    with pytest.raises(GatewayError) as e:
        await tools.list_notebooks(gw, make_consumer(scopes=("list",)), {})
    assert e.value.code == "auth_expired"


async def test_list_notebooks_bounded_wait(gw, make_consumer, monkeypatch):
    import asyncio

    async def stuck():
        await asyncio.Event().wait()
    gw.nlm.client.notebooks.list = stuck
    monkeypatch.setattr(tools, "LIST_TIMEOUT_S", 0.05)
    with pytest.raises(GatewayError) as e:
        await tools.list_notebooks(gw, make_consumer(scopes=("list",)), {})
    assert e.value.code == "generation_timeout" and "list" in e.value.message


def test_list_timeout_under_60s():
    assert tools.LIST_TIMEOUT_S <= 50


async def test_job_status_returns_title(gw, make_consumer):
    c = make_consumer()
    out = await tools.podcast_from_sources(gw, c, {"urls": ["https://a.example/x"], "title": "Lighthouses — history and tech"})
    assert (await tools.job_status(gw, c, {"job_id": out["job_id"]}))["title"] is None
    await run_all(gw)
    st = await tools.job_status(gw, c, {"job_id": out["job_id"]})
    assert st["status"] == "done" and st["title"] == "Lighthouses — history and tech"


STUDIO = ("studio",)


@pytest.mark.parametrize("fmt,mime,ext", [(None, "application/pdf", "pdf"),
                                          ("pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation", "pptx")])
async def test_slides_submit_status_download(gw, make_consumer, fmt, mime, ext):
    c = make_consumer(scopes=STUDIO, audio=0, report=0)
    args = {"urls": ["https://a.example/x"], "title": "Deck", "instructions": "short"}
    if fmt:
        args["format"] = fmt
    out = await tools.slides_from_sources(gw, c, args)
    assert out["status"] == "queued" and out["notebook_url"].startswith("https://notebooklm.example/notebook/")
    job = gw.store.get_job(out["job_id"])
    assert job.kind == "slides" and job.params == {"format": ext, "language": "en", "instructions": "short", "title": "Deck"}
    await run_all(gw)
    st = await tools.job_status(gw, c, {"job_id": out["job_id"]})
    assert st["status"] == "done" and st["mime"] == mime and st["title"] == "Deck" and st["kind"] == "slides"
    assert st["download_url"].endswith(f"/jobs/{out['job_id']}/download")


async def test_infographic_submit_and_status(gw, make_consumer):
    c = make_consumer(scopes=STUDIO, audio=0, report=0)
    out = await tools.infographic_from_sources(gw, c, {"urls": ["https://a.example/x"], "language": "de"})
    job = gw.store.get_job(out["job_id"])
    assert job.kind == "infographic" and job.params == {"language": "de", "instructions": None, "title": None}
    await run_all(gw)
    st = await tools.job_status(gw, c, {"job_id": out["job_id"]})
    assert st["status"] == "done" and st["mime"] == "image/png"


async def test_big_slides_get_share_url(gw, make_consumer):
    c = make_consumer(scopes=STUDIO)
    gw.nlm.client.slides_bytes["pdf"] = b"%PDF" + b"\x00" * gw.cfg.big_file_bytes
    out = await tools.slides_from_sources(gw, c, {"urls": ["https://a.example/x"]})
    await run_all(gw)
    st = await tools.job_status(gw, c, {"job_id": out["job_id"]})
    assert st["share_url"].endswith(".pdf")


@pytest.mark.parametrize("tool", ["slides_from_sources", "infographic_from_sources"])
async def test_studio_needs_studio_scope(gw, make_consumer, tool):
    with pytest.raises(GatewayError) as e:
        await getattr(tools, tool)(gw, make_consumer(scopes=("podcast", "report", "list", "read", "notes")), {"urls": ["https://x"]})
    assert e.value.code == "not_found" and gw.nlm.client.creates == []


async def test_slides_bad_format(gw, make_consumer):
    with pytest.raises(GatewayError) as e:
        await tools.slides_from_sources(gw, make_consumer(scopes=STUDIO), {"urls": ["https://x"], "format": "key"})
    assert e.value.code == "bad_request" and gw.nlm.client.creates == []


@pytest.mark.parametrize("tool,kind,field", [("slides_from_sources", "slides", "account_slides_daily"),
                                              ("infographic_from_sources", "infographic", "account_infographic_daily")])
async def test_studio_account_cap(gw, make_consumer, tool, kind, field):
    from dataclasses import replace
    gw.cfg = replace(gw.cfg, **{field: 1})
    c = make_consumer(scopes=STUDIO, audio=0, report=0)
    await getattr(tools, tool)(gw, c, {"urls": ["https://x"]})
    with pytest.raises(GatewayError) as e:
        await getattr(tools, tool)(gw, c, {"urls": ["https://y"]})
    assert e.value.code == "quota_exceeded" and gw.store.count_billable(kind, 0) == 1


async def test_report_account_cap_enforced(gw, make_consumer):
    from dataclasses import replace
    gw.cfg = replace(gw.cfg, account_report_daily=1)
    c = make_consumer(report=50)
    await tools.report_from_sources(gw, c, {"urls": ["https://x"]})
    with pytest.raises(GatewayError) as e:
        await tools.report_from_sources(gw, make_consumer(id="other", report=50), {"urls": ["https://y"]})
    assert e.value.code == "quota_exceeded" and "account" in e.value.message


def test_studio_tool_specs():
    specs = {t.name: t for t in tools.TOOLS}
    for name in ("slides_from_sources", "infographic_from_sources"):
        t = specs[name]
        assert t.scope == "studio" and "notebook_url" in t.description and "45 MB" in t.description
        assert {"urls", "upload_ids", "notebook", "title", "instructions", "language"} <= set(t.input_schema["properties"])
    assert specs["slides_from_sources"].input_schema["properties"]["format"]["enum"] == ["pdf", "pptx"]
    assert "format" not in specs["infographic_from_sources"].input_schema["properties"]
