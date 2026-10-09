"""Task 17: add_sources (scope add, job kind "sources"): no generation, no quota, never any rollback."""
import json
import logging

import pytest
from notebooklm import AuthError

from nlm_gateway import tools
from nlm_gateway.errors import GatewayError
from tests.fakes import FakeNotebook

PDF = b"%PDF-1.7 hello"
SENTINEL = "https://secret-sentinel.example/abc"


def adder(make_consumer, **kw):
    return make_consumer(id=kw.pop("id", "adder"), scopes=kw.pop("scopes", ("add",)), label="To Review", **kw)


def upload(gw, consumer, name="paper.pdf"):
    p = gw.cfg.state_dir / f"tmp-{name}"
    p.write_bytes(PDF)
    return gw.store.add_upload(consumer.id, name, p, len(PDF), gw.now(), 7200)


async def run_all(gw):
    while (job := gw.store.claim_next(gw.now())) is not None:
        await gw.runner.run_job(job)


async def status(gw, c, job_id):
    return await tools.job_status(gw, c, {"job_id": job_id})


async def test_scope_gating(gw, make_consumer):
    with pytest.raises(GatewayError) as e:
        await tools.add_sources(gw, make_consumer(scopes=("podcast", "report", "studio")), {"urls": ["https://x.example/"]})
    assert e.value.code == "not_found" and gw.nlm.client.creates == [] and gw.store.count_status("queued") == 0
    spec = {t.name: t for t in tools.TOOLS}["add_sources"]
    assert spec.scope == "add"


async def test_adds_to_existing_handmade_notebook_without_creating(gw, make_consumer):
    c = adder(make_consumer)
    fake = gw.nlm.client
    fake.notebooks_by_id["nb-hand"] = FakeNotebook("nb-hand", "Reading list")
    out = await tools.add_sources(gw, c, {"urls": ["https://a.example/1"], "notebook": "Reading list"})
    assert out == {"job_id": out["job_id"], "notebook_url": "https://notebooklm.example/notebook/nb-hand", "status": "queued"}
    await run_all(gw)
    assert fake.creates == [] and [s.title for s in fake.source_data["nb-hand"]] == ["https://a.example/1"]
    st = await status(gw, c, out["job_id"])
    assert st["status"] == "done" and st["added"] == 1 and st["failed"] == []


async def test_missing_name_is_created_and_default_is_label(gw, make_consumer):
    c = adder(make_consumer)
    await tools.add_sources(gw, c, {"urls": ["https://a.example/1"], "notebook": "New topic"})
    await tools.add_sources(gw, c, {"urls": ["https://a.example/2"]})
    assert gw.nlm.client.creates == ["New topic", "To Review"]
    await run_all(gw)
    assert gw.nlm.client.generated == [] and gw.nlm.client.renames == [] and gw.nlm.client.downloads == []


@pytest.mark.parametrize("args", [
    {}, {"urls": []}, {"urls": ["ftp://x"]}, {"urls": "https://x"}, {"urls": ["https://x"] * 51},
    {"urls": ["https://x"], "notebook": ""}, {"urls": ["https://x"], "notebook": "x" * 101},
])
async def test_bad_requests(gw, make_consumer, args):
    with pytest.raises(GatewayError) as e:
        await tools.add_sources(gw, adder(make_consumer), args)
    assert e.value.code == "bad_request" and gw.nlm.client.creates == []


async def test_partial_failure_continues_and_keeps_added(gw, make_consumer):
    c = adder(make_consumer)
    fake = gw.nlm.client
    fake.reject_urls = {"https://bad.example/"}
    out = await tools.add_sources(gw, c, {"urls": ["https://ok.example/0", "https://bad.example/", "https://ok.example/2"]})
    await run_all(gw)
    st = await status(gw, c, out["job_id"])
    assert st["status"] == "done" and st["added"] == 2 and st["failed"] == [1] and "error" not in st
    assert not {"download_url", "bytes", "title", "mime"} & set(st)
    job = gw.store.get_job(out["job_id"])
    assert fake.deleted_sources == [] and len(fake.source_data[job.notebook_id]) == 2 and job.urls == []
    assert gw.store.get_notebook(job.notebook_title)[1] == 2  # the failed source's slot is released


async def test_all_fail_is_source_rejected_without_deletes(gw, make_consumer):
    c = adder(make_consumer)
    fake = gw.nlm.client
    fake.reject_urls = {"https://bad.example/1", "https://bad.example/2"}
    out = await tools.add_sources(gw, c, {"urls": sorted(fake.reject_urls)})
    await run_all(gw)
    st = await status(gw, c, out["job_id"])
    assert st["status"] == "failed" and st["error"]["code"] == "source_rejected" and st["added"] == 0 and st["failed"] == [0, 1]
    job = gw.store.get_job(out["job_id"])
    assert fake.deleted_sources == [] and gw.store.get_notebook(job.notebook_title)[1] == 0


async def test_auth_lost_midway_keeps_earlier_adds(gw, make_consumer):
    c = adder(make_consumer)
    fake = gw.nlm.client
    orig = fake.sources.add_url
    n = 0

    async def add(nb, url, **kw):
        nonlocal n
        n += 1
        if n == 2:
            gw.nlm.auth_ok = False
            raise AuthError("expired")
        return await orig(nb, url, **kw)
    fake.sources.add_url = add
    up = upload(gw, c)
    out = await tools.add_sources(gw, c, {"urls": ["https://a.example/1", "https://a.example/2", "https://a.example/3"],
                                          "upload_ids": [up.id]})
    await run_all(gw)
    job = gw.store.get_job(out["job_id"])
    st = await status(gw, c, out["job_id"])
    assert st["status"] == "failed" and st["error"]["code"] == "auth_expired" and st["added"] == 1 and st["failed"] == []
    assert n == 2  # the remaining adds were aborted
    assert fake.deleted_sources == [] and len(fake.source_data[job.notebook_id]) == 1
    assert not job.rollback_pending and len(job.source_ids) == 1
    assert gw.store.get_notebook(job.notebook_title)[1] == 1  # 3 unadded slots released
    assert gw.store.get_upload(up.id) is None and not (gw.cfg.state_dir / "tmp-paper.pdf").exists()
    gw.nlm.auth_ok = True
    await gw.runner.retry_rollbacks()
    assert fake.deleted_sources == []


async def test_recover_running_sources_job_keeps_sources(gw, queue_job, make_consumer):
    c = adder(make_consumer)
    job = await queue_job(c, urls=["https://a.example/1", "https://a.example/2"], kind="sources", params={})
    gw.store.update_job(job.id, gw.now(), source_ids=["src-x"])
    await gw.runner.recover()
    j = gw.store.get_job(job.id)
    assert j.status == "failed" and j.error_code == "gateway_restarted" and j.source_ids == ["src-x"]
    assert gw.nlm.client.deleted_sources == [] and not j.rollback_pending
    assert gw.store.get_notebook(j.notebook_title)[1] == 1


async def test_retry_rollbacks_never_touches_sources_jobs(gw, queue_job, make_consumer):
    c = adder(make_consumer)
    job = await queue_job(c, urls=["https://a.example/1"], kind="sources", params={})
    gw.store.update_job(job.id, gw.now(), source_ids=["src-x"], status="failed", rollback_pending=True)  # must never happen
    await gw.runner.retry_rollbacks()
    assert gw.nlm.client.deleted_sources == [] and gw.store.get_job(job.id).source_ids == ["src-x"]


async def test_fail_never_rolls_back_a_sources_job(gw, queue_job, make_consumer):
    c = adder(make_consumer)
    job = await queue_job(c, urls=["https://a.example/1"], kind="sources", params={})
    gw.store.update_job(job.id, gw.now(), source_ids=["src-x"])
    await gw.runner._fail(job.id, GatewayError("upstream_changed", "x"))
    assert gw.nlm.client.deleted_sources == [] and gw.store.get_job(job.id).source_ids == ["src-x"]


async def test_no_quota_counted_and_share_link_refused(gw, make_consumer):
    c = adder(make_consumer, scopes=("add", "podcast", "report"), audio=0, report=0)
    before = await tools.quota_status(gw, c, {})
    out = await tools.add_sources(gw, c, {"urls": ["https://a.example/1"]})  # works with zero generation quota
    await run_all(gw)
    assert (await status(gw, c, out["job_id"]))["status"] == "done"
    assert await tools.quota_status(gw, c, {}) == before
    for kind in ("podcast", "report", "slides", "infographic"):
        assert gw.store.count_billable(kind, 0) == 0
    with pytest.raises(GatewayError) as e:
        await tools.share_link(gw, c, {"job_id": out["job_id"]})
    assert e.value.code == "bad_request" and "nothing to download" in e.value.message


async def test_uploads_dropped_after_add_and_after_failed_add(gw, make_consumer):
    c = adder(make_consumer)
    good, bad = upload(gw, c, "good.pdf"), upload(gw, c, "bad.pdf")
    gw.nlm.client.reject_files = {"bad.pdf"}
    out = await tools.add_sources(gw, c, {"urls": ["https://a.example/1"], "upload_ids": [good.id, bad.id]})
    await run_all(gw)
    st = await status(gw, c, out["job_id"])
    assert st["status"] == "done" and st["added"] == 2 and st["failed"] == [2]  # urls first, then upload_ids
    for up, name in ((good, "good.pdf"), (bad, "bad.pdf")):
        assert gw.store.get_upload(up.id) is None and not (gw.cfg.state_dir / f"tmp-{name}").exists()


async def test_failed_indices_follow_upload_ids_order(gw, make_consumer):
    c = adder(make_consumer)
    ups = [upload(gw, c, f"f{i}.pdf") for i in range(4)]
    gw.nlm.client.reject_files = {"f1.pdf"}
    order = [ups[3], ups[1], ups[0], ups[2]]
    out = await tools.add_sources(gw, c, {"upload_ids": [u.id for u in order]})
    await run_all(gw)
    assert (await status(gw, c, out["job_id"]))["failed"] == [1]


async def test_audit_and_logs_carry_no_urls_or_filenames(gw, make_consumer, caplog):
    c = adder(make_consumer)
    gw.nlm.client.reject_urls = {SENTINEL + "/bad"}
    gw.nlm.client.reject_files = {"secret-sentinel.pdf"}
    up = upload(gw, c, "secret-sentinel.pdf")
    with caplog.at_level(logging.DEBUG, logger="nlm_gateway"):
        out = await tools.add_sources(gw, c, {"urls": [SENTINEL, SENTINEL + "/bad"], "upload_ids": [up.id],
                                              "notebook": "sentinel notebook"})
        await run_all(gw)
    st = await status(gw, c, out["job_id"])
    audit = (gw.cfg.state_dir / "audit.jsonl").read_text()
    assert "sentinel" not in audit and "sentinel" not in caplog.text and "sentinel" not in json.dumps(st)
    events = {e["event"]: e for e in map(json.loads, audit.splitlines())}
    assert events["submit"]["kind"] == "sources" and events["submit"]["urls"] == 2 and events["submit"]["uploads"] == 1
    assert "url_hashes" not in events["submit"]
    assert events["job_done"]["added"] == 1 and events["job_done"]["failed"] == 2


async def test_all_fail_message_has_no_source_text(gw, make_consumer):
    c = adder(make_consumer)
    gw.nlm.client.reject_urls = {SENTINEL}
    out = await tools.add_sources(gw, c, {"urls": [SENTINEL]})
    await run_all(gw)
    st = await status(gw, c, out["job_id"])
    assert st["error"]["code"] == "source_rejected" and "sentinel" not in json.dumps(st)
    assert "sentinel" not in (gw.cfg.state_dir / "audit.jsonl").read_text()


async def test_each_upload_dropped_as_soon_as_google_has_it(gw, make_consumer):
    c = adder(make_consumer)
    first, second = upload(gw, c, "first.pdf"), upload(gw, c, "second.pdf")
    fake = gw.nlm.client
    orig = fake.sources.add_file
    seen = {}

    async def add_file(nb, path, **kw):
        if kw.get("title") == "second.pdf":
            seen["first_row"] = gw.store.get_upload(first.id)
            seen["first_file"] = (gw.cfg.state_dir / "tmp-first.pdf").exists()
        return await orig(nb, path, **kw)
    fake.sources.add_file = add_file
    await tools.add_sources(gw, c, {"upload_ids": [first.id, second.id]})
    await run_all(gw)
    assert seen == {"first_row": None, "first_file": False}
