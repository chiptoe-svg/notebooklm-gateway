import asyncio
import json

from notebooklm import AuthError


PDF = b"%PDF-1.7 hello"


def upload(gw, consumer, name="paper.pdf"):
    p = gw.cfg.state_dir / f"tmp-{name}"
    p.write_bytes(PDF)
    return gw.store.add_upload(consumer.id, name, p, len(PDF), gw.now(), 7200)


async def test_podcast_happy_path(gw, queue_job, make_consumer):
    c = make_consumer()
    up = upload(gw, c)
    job = await queue_job(c, urls=["https://a.example/1"], upload_ids=[up.id])
    await gw.runner.run_job(job)
    done = gw.store.get_job(job.id)
    fake = gw.nlm.client
    assert done.status == "done" and done.accepted and done.task_id == done.artifact_id
    assert done.size_bytes == len(fake.audio_bytes) and done.mime == "audio/mp4"
    kind, nb, kw = fake.generated[0]
    assert kind == "audio" and nb == done.notebook_id and kw["source_ids"] == done.source_ids and len(done.source_ids) == 2
    assert kw["audio_format"].name == "BRIEF" and kw["audio_length"].name == "SHORT" and kw["language"] == "en"
    assert gw.store.get_upload(up.id) is None and not (gw.cfg.state_dir / "tmp-paper.pdf").exists()
    assert done.urls == []  # URLs are cleared once the job finishes
    assert list((gw.cfg.state_dir / "tmp").iterdir()) == []  # measuring left no host copy


async def test_report_return_text(gw, queue_job, make_consumer):
    c = make_consumer()
    job = await queue_job(c, kind="report", params={"kind": "study_guide", "language": "en", "instructions": "focus", "return_text": True})
    await gw.runner.run_job(job)
    done = gw.store.get_job(job.id)
    _, _, kw = gw.nlm.client.generated[0]
    assert kw["report_format"].name == "STUDY_GUIDE" and kw["extra_instructions"] == "focus"
    assert done.status == "done" and done.text == gw.nlm.client.report_text


async def test_report_text_too_large_not_inlined(gw, queue_job, make_consumer):
    gw.nlm.client.report_text = "x" * (gw.cfg.inline_text_max + 1)
    job = await queue_job(make_consumer(), kind="report", params={"kind": "briefing", "language": "en", "instructions": None, "return_text": True})
    await gw.runner.run_job(job)
    assert gw.store.get_job(job.id).text is None


async def test_partial_source_failure_rolls_back_only_this_job(gw, queue_job, make_consumer):  # Review Focus 3
    c = make_consumer()
    earlier = await queue_job(c, urls=["https://earlier.example/"])
    await gw.runner.run_job(earlier)
    earlier_sources = gw.store.get_job(earlier.id).source_ids
    gw.nlm.client.reject_urls = {"https://bad.example/"}
    job = await queue_job(c, urls=["https://ok.example/", "https://bad.example/", "https://never.example/"])
    await gw.runner.run_job(job)
    failed = gw.store.get_job(job.id)
    assert failed.status == "failed" and failed.error_code == "source_rejected" and "#1" in failed.error_message
    added = [x.id for x in gw.nlm.client.source_data[failed.notebook_id] if x.title == "https://ok.example/"]
    assert added == [] and len(gw.nlm.client.deleted_sources) == 1 and failed.source_ids == [] and not failed.rollback_pending
    assert not set(earlier_sources) & set(gw.nlm.client.deleted_sources)
    assert gw.store.get_notebook(failed.notebook_title)[1] == 1  # only the earlier job's reservation remains
    assert len(gw.nlm.client.generated) == 1  # the failed job never generated


async def test_generation_refused_not_billable(gw, queue_job, make_consumer):
    gw.nlm.client.refuse_generation = True
    job = await queue_job(make_consumer())
    await gw.runner.run_job(job)
    j = gw.store.get_job(job.id)
    assert j.status == "failed" and j.error_code == "rate_limited" and not j.accepted
    assert gw.store.count_billable("podcast", 0) == 0


async def test_generation_failed_after_accept_keeps_sources(gw, queue_job, make_consumer):
    gw.nlm.client.fail_generation = True
    job = await queue_job(make_consumer())
    await gw.runner.run_job(job)
    j = gw.store.get_job(job.id)
    assert j.status == "failed" and j.error_code == "generation_failed" and j.accepted
    assert gw.nlm.client.deleted_sources == []
    assert gw.store.count_billable("podcast", 0) == 1


async def test_timeout(gw, queue_job, make_consumer):
    gw.nlm.client.hang = True
    job = await queue_job(make_consumer())
    await gw.runner.run_job(job)
    assert gw.store.get_job(job.id).error_code == "generation_timeout"


async def test_auth_lost_mid_queue(gw, queue_job, make_consumer):
    job = await queue_job(make_consumer())
    gw.nlm.auth_ok = False
    await gw.runner.run_job(job)
    assert gw.store.get_job(job.id).error_code == "auth_expired"


async def test_recover_after_restart(gw, queue_job, make_consumer):
    c = make_consumer()
    not_started = await queue_job(c)             # running, nothing at Google yet
    gw.store.update_job(not_started.id, gw.now(), source_ids=["src-x"])
    accepted = await queue_job(c)                # running, Google accepted generation
    gw.store.update_job(accepted.id, gw.now(), task_id="t-123", accepted=True)
    await gw.runner.recover()
    await gw.runner.drain()
    a = gw.store.get_job(not_started.id)
    assert a.status == "failed" and a.error_code == "gateway_restarted" and "src-x" in gw.nlm.client.deleted_sources
    b = gw.store.get_job(accepted.id)
    assert b.status == "done" and b.artifact_id == "t-123"


async def test_sweep_expired_uploads(gw, make_consumer):
    c = make_consumer()
    up = upload(gw, c)
    await gw.runner.sweep_uploads(gw.now() + 7201)
    assert gw.store.get_upload(up.id) is None and not (gw.cfg.state_dir / "tmp-paper.pdf").exists()


async def test_audit_never_contains_urls_or_filenames(gw, queue_job, make_consumer):
    c = make_consumer()
    up = upload(gw, c, name="secret-name.pdf")
    job = await queue_job(c, urls=["https://secret.example/abc"], upload_ids=[up.id])
    await gw.runner.run_job(job)
    text = (gw.cfg.state_dir / "audit.jsonl").read_text()
    events = [json.loads(x)["event"] for x in text.splitlines()]
    assert "secret" not in text and events[-2:] == ["job_done", "job_renamed"]  # the default title holds the URL; audit must not


async def test_auth_lost_mid_job_then_retry_rollbacks(gw, queue_job, make_consumer):
    c = make_consumer()
    job = await queue_job(c, urls=["https://a.example/1", "https://a.example/2", "https://a.example/3"])
    client = gw.nlm.client
    orig = client.sources.add_url
    n = 0

    async def add(nb, url, **kw):
        nonlocal n
        n += 1
        if n == 3:
            gw.nlm.auth_ok = False
            raise AuthError("expired")
        return await orig(nb, url, **kw)

    client.sources.add_url = add
    await gw.runner.run_job(job)
    j = gw.store.get_job(job.id)
    assert j.status == "failed" and j.error_code == "auth_expired"
    assert j.rollback_pending and len(j.source_ids) == 2 and client.deleted_sources == []
    assert gw.store.get_notebook(j.notebook_title)[1] == 2  # the never-added slot is released; 2 added sources stay reserved
    gw.nlm.auth_ok = True
    await gw.runner.retry_rollbacks()
    j2 = gw.store.get_job(job.id)
    assert not j2.rollback_pending and j2.source_ids == [] and len(client.deleted_sources) == 2
    assert gw.store.get_notebook(j2.notebook_title)[1] == 0


async def test_transient_delete_failure_keeps_only_that_id(gw, queue_job, make_consumer):
    job = await queue_job(make_consumer(), urls=["https://a.example/1", "https://b.example/2", "https://bad.example/"])
    client = gw.nlm.client
    client.reject_urls = {"https://bad.example/"}
    orig = client.sources.delete
    calls = []

    async def delete(nb, sid):
        calls.append(sid)
        if len(calls) == 1:
            raise RuntimeError("transient")
        return await orig(nb, sid)

    client.sources.delete = delete
    await gw.runner.run_job(job)
    j = gw.store.get_job(job.id)
    assert j.rollback_pending and j.source_ids == [calls[0]] and client.deleted_sources == [calls[1]]
    assert gw.store.get_notebook(j.notebook_title)[1] == 1  # only the undeleted source stays reserved
    await gw.runner.retry_rollbacks()
    j = gw.store.get_job(job.id)
    assert not j.rollback_pending and j.source_ids == [] and set(client.deleted_sources) == set(calls)
    assert gw.store.get_notebook(j.notebook_title)[1] == 0


async def test_fail_is_idempotent(gw, queue_job, make_consumer):
    from nlm_gateway.errors import GatewayError
    c = make_consumer()
    keep = await queue_job(c, urls=["https://keep.example/"])
    gw.store.update_job(keep.id, gw.now(), status="done")
    job = await queue_job(c, urls=["https://x.example/"])
    err = GatewayError("source_rejected", "x")
    await gw.runner._fail(job.id, err)
    await gw.runner._fail(job.id, err)
    await gw.runner.recover()
    assert gw.store.get_notebook(gw.store.get_job(job.id).notebook_title)[1] == 1  # only `keep` remains reserved


async def test_worker_survives_exception(gw, queue_job, make_consumer, monkeypatch):
    c = make_consumer()
    real = gw.store.claim_next
    state = {"n": 0}

    def flaky(now):
        state["n"] += 1
        if state["n"] == 1:
            raise OSError("db busy")
        return real(now)

    gw.runner.cfg = gw.cfg.__class__(**{**gw.cfg.__dict__, "workers": 1})
    j1 = await queue_job(c)
    j2 = await queue_job(c)
    # queue_job claims; put both back to queued for the worker
    gw.store.update_job(j1.id, gw.now(), status="queued")
    gw.store.update_job(j2.id, gw.now(), status="queued")
    monkeypatch.setattr(gw.store, "claim_next", flaky)
    await gw.runner.start()
    try:
        for _ in range(60):
            if gw.store.get_job(j2.id).status in ("done", "failed"):
                break
            await asyncio.sleep(0.05)
    finally:
        await gw.runner.stop()
    assert gw.store.get_job(j2.id).status == "done"


async def test_generate_raises_keeps_sources(gw, queue_job, make_consumer):
    async def boom(*a, **kw):
        raise RuntimeError("network died")
    gw.nlm.client.artifacts.generate_audio = boom
    job = await queue_job(make_consumer())
    await gw.runner.run_job(job)
    j = gw.store.get_job(job.id)
    assert j.status == "failed" and j.error_code == "upstream_changed" and not j.accepted
    assert gw.nlm.client.deleted_sources == [] and len(j.source_ids) == 1 and not j.rollback_pending
    assert gw.store.get_notebook(j.notebook_title)[1] == 1


async def test_cancel_during_generate_then_recover_keeps_sources(gw, queue_job, make_consumer):
    gate = asyncio.Event()  # never set

    async def hang(*a, **kw):
        await gate.wait()

    gw.nlm.client.artifacts.generate_audio = hang
    job = await queue_job(make_consumer())
    task = asyncio.create_task(gw.runner.run_job(job))
    for _ in range(50):
        await asyncio.sleep(0)
        if gw.store.get_job(job.id).generation_requested:
            break
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert gw.store.get_job(job.id).status == "running"
    await gw.runner.recover()
    j = gw.store.get_job(job.id)
    assert j.status == "failed" and j.error_code == "gateway_restarted"
    assert gw.nlm.client.deleted_sources == [] and len(j.source_ids) == 1
    assert gw.store.get_notebook(j.notebook_title)[1] == 1


async def test_retry_rollbacks_continues_past_bad_job(gw, queue_job, make_consumer, monkeypatch):
    c = make_consumer()
    jobs = []
    for _ in range(2):
        job = await queue_job(c, urls=["https://a.example/1"])
        gw.store.update_job(job.id, gw.now(), source_ids=["s-" + job.id], status="failed", rollback_pending=True)
        jobs.append(job)
    real = gw.store.update_job
    state = {"n": 0}

    def flaky(*a, **kw):
        if "rollback_pending" in kw:
            state["n"] += 1
            if state["n"] == 1:
                raise OSError("db")
        return real(*a, **kw)

    monkeypatch.setattr(gw.store, "update_job", flaky)
    await gw.runner.retry_rollbacks()
    assert len(gw.nlm.client.deleted_sources) == 2
    assert [gw.store.get_job(j.id).rollback_pending for j in jobs].count(False) == 1


async def test_interrupted_submit_is_recovered_and_swept(cfg, make_consumer):
    """A crash while submit() awaits Google leaves a 'new' job with claimed uploads; recover()+sweep clean it up."""
    import time
    from nlm_gateway import tools
    from nlm_gateway.gateway import build_gateway
    from tests.fakes import FakeNLM
    gw = build_gateway(cfg, FakeNLM())
    c = make_consumer()
    d = cfg.state_dir / "uploads"
    d.mkdir(exist_ok=True)
    p = d / "up_crash"
    p.write_bytes(PDF)
    up = gw.store.add_upload(c.id, "secret-name.pdf", p, len(PDF), gw.now(), gw.cfg.upload_ttl_s, upload_id="up_crash")
    hang = asyncio.Event()  # never set: submit is stuck inside notebooks.resolve

    async def stuck_list():
        await hang.wait()
    gw.nlm.client.notebooks.list = stuck_list
    t = asyncio.create_task(tools.submit(gw, c, "podcast", {"upload_ids": [up.id]}))
    await asyncio.sleep(0.05)
    # the process dies here (no except blocks run); a new gateway starts on the same state dir
    gw2 = build_gateway(cfg, FakeNLM())
    await gw2.runner.recover()
    await gw2.runner.sweep_uploads(time.time() + 10 * 86400)
    (job_id,) = [r[0] for r in gw2.store._db.execute("SELECT id FROM jobs").fetchall()]
    j = gw2.store.get_job(job_id)
    assert j.status == "failed" and j.error_code == "gateway_restarted" and j.urls == [] and j.finished_at
    assert gw2.store.get_upload(up.id) is None and not p.exists()
    assert gw2.store.count_billable("podcast", 0, c.id) == 0
    t.cancel()
    await asyncio.gather(t, return_exceptions=True)


async def test_sweep_removes_uploads_of_terminal_jobs(gw, queue_job, make_consumer):
    c = make_consumer()
    dead, live = upload(gw, c, "a.pdf"), upload(gw, c, "b.pdf")
    j1 = await queue_job(c, upload_ids=[dead.id])
    j2 = await queue_job(c, upload_ids=[live.id])
    gw.store.update_job(j1.id, gw.now(), status="failed")
    await gw.runner.sweep_uploads(gw.now())
    assert gw.store.get_upload(dead.id) is None and not (gw.cfg.state_dir / "tmp-a.pdf").exists()
    assert gw.store.get_upload(live.id) is not None and (gw.cfg.state_dir / "tmp-b.pdf").exists()
    assert gw.store.get_job(j2.id).status == "running"


async def _accepted_job(gw, queue_job, c):
    j = await queue_job(c)
    gw.store.update_job(j.id, gw.now(), task_id="t-1", accepted=True, generation_requested=True)
    return j


async def test_reattach_waits_for_auth(gw, queue_job, make_consumer):
    c = make_consumer()
    j = await _accepted_job(gw, queue_job, c)
    gw.nlm.auth_ok = False  # e.g. network not up yet at boot
    await gw.runner.recover()
    await asyncio.wait_for(gw.runner.drain(), 5)
    j2 = gw.store.get_job(j.id)
    assert j2.status == "running" and j2.accepted and j2.task_id == "t-1"
    gw.nlm.auth_ok = True
    gw.runner.reattach_stalled()  # the maintenance step
    await asyncio.wait_for(gw.runner.drain(), 5)
    assert gw.store.get_job(j.id).status == "done"
    assert gw.store.count_billable("podcast", 0, c.id) == 1


async def test_network_error_after_accept_keeps_job_running(gw, queue_job, make_consumer):
    from notebooklm import NetworkError
    client = gw.nlm.client
    real = client.artifacts.wait_for_completion

    async def down(*a, **kw):
        raise NetworkError("connection reset")
    client.artifacts.wait_for_completion = down
    job = await queue_job(make_consumer())
    await gw.runner.run_job(job)
    j = gw.store.get_job(job.id)
    assert j.status == "running" and j.accepted and gw.nlm.client.deleted_sources == []
    client.artifacts.wait_for_completion = real
    gw.runner.reattach_stalled()
    await asyncio.wait_for(gw.runner.drain(), 5)
    assert gw.store.get_job(job.id).status == "done"


async def test_reattach_timeout_stays_terminal(gw, queue_job, make_consumer):
    j = await _accepted_job(gw, queue_job, make_consumer())
    gw.nlm.client.hang = True
    await gw.runner.recover()
    await asyncio.wait_for(gw.runner.drain(), 5)
    assert gw.store.get_job(j.id).error_code == "generation_timeout"


async def test_reattach_stalled_does_not_duplicate(gw, queue_job, make_consumer):
    client = gw.nlm.client
    gate = asyncio.Event()
    real = client.artifacts.wait_for_completion

    async def slow(*a, **kw):
        await gate.wait()
        return await real(*a, **kw)
    client.artifacts.wait_for_completion = slow
    await _accepted_job(gw, queue_job, make_consumer())
    gw.runner.reattach_stalled()
    await asyncio.sleep(0)
    gw.runner.reattach_stalled()
    gate.set()
    await asyncio.wait_for(gw.runner.drain(), 5)
    assert len(client.downloads) == 1


async def test_maintenance_reattaches_once_auth_returns(gw, queue_job, make_consumer, monkeypatch):
    from nlm_gateway import jobs
    j = await _accepted_job(gw, queue_job, make_consumer())
    monkeypatch.setattr(jobs, "MAINTENANCE_S", 0.01)
    task = asyncio.create_task(gw.runner._maintenance())
    try:
        for _ in range(200):
            if gw.store.get_job(j.id).status == "done":
                break
            await asyncio.sleep(0.01)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await gw.runner.stop()
    assert gw.store.get_job(j.id).status == "done"


SENTINEL = "https://sentinel.example/private-path"


def _audit_fails_once_on(gw, monkeypatch, event):
    real, state = gw.audit.write, {"n": 0}

    def flaky(ev, **kw):
        if ev == event and state["n"] == 0:
            state["n"] += 1
            raise OSError("disk full")
        return real(ev, **kw)
    monkeypatch.setattr(gw.audit, "write", flaky)
    return state


async def test_source_error_does_not_chain_upstream_exception(gw, queue_job, make_consumer, monkeypatch):
    import traceback
    gw.nlm.client.reject_urls.add(SENTINEL)  # the fake's error message contains the URL
    _audit_fails_once_on(gw, monkeypatch, "job_failed")
    job = await queue_job(make_consumer(), urls=[SENTINEL])
    try:
        await gw.runner.run_job(job)
    except OSError as e:
        rendered = "".join(traceback.format_exception(e))
    else:
        raise AssertionError("expected the audit failure to escape")
    assert "disk full" in rendered and "sentinel" not in rendered


async def test_worker_log_has_no_exception_text(gw, queue_job, make_consumer, monkeypatch, caplog):
    import logging

    async def boom(*a, **kw):
        raise RuntimeError(f"upstream said: could not use {SENTINEL}")
    gw.nlm.client.artifacts.generate_audio = boom
    state = _audit_fails_once_on(gw, monkeypatch, "job_failed")
    job = await queue_job(make_consumer())
    gw.store.update_job(job.id, gw.now(), status="queued")
    with caplog.at_level(logging.DEBUG, logger="nlm_gateway"):
        task = asyncio.create_task(gw.runner._worker())
        try:
            for _ in range(100):
                if state["n"] and any("worker" in r.getMessage() for r in caplog.records):
                    break
                await asyncio.sleep(0.01)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert any("worker iteration failed" in r.getMessage() for r in caplog.records)
    assert "sentinel" not in caplog.text and all(r.exc_info is None for r in caplog.records)


async def _pending_rollback_job(gw, queue_job, c, sids=("s-1",)):
    job = await queue_job(c, urls=["https://a.example/1"])  # reserves 1 slot
    gw.store.update_job(job.id, gw.now(), source_ids=list(sids), status="failed", rollback_pending=True)
    return gw.store.get_job(job.id)


async def test_rollback_retry_cap(gw, queue_job, make_consumer):
    job = await _pending_rollback_job(gw, queue_job, make_consumer())

    async def down(*a, **kw):
        raise RuntimeError("still failing")
    gw.nlm.client.sources.delete = down
    for _ in range(49):
        await gw.runner.retry_rollbacks()
    j = gw.store.get_job(job.id)
    assert j.rollback_pending and j.rollback_attempts == 49
    await gw.runner.retry_rollbacks()
    j = gw.store.get_job(job.id)
    assert not j.rollback_pending and j.rollback_attempts == 50 and j.source_ids == ["s-1"]
    assert gw.store.get_notebook(j.notebook_title)[1] == 1  # reservation kept: count stays high (safe)
    last = json.loads((gw.cfg.state_dir / "audit.jsonl").read_text().splitlines()[-1])
    assert last["event"] == "rollback_abandoned" and last["attempts"] == 50 and last["sources"] == 1
    await gw.runner.retry_rollbacks()
    assert gw.store.get_job(job.id).rollback_attempts == 50  # no longer retried


async def test_rollback_delete_of_missing_source_is_success(gw, queue_job, make_consumer):
    from notebooklm import SourceNotFoundError
    job = await _pending_rollback_job(gw, queue_job, make_consumer())

    async def gone(nb, sid):
        raise SourceNotFoundError(sid)
    gw.nlm.client.sources.delete = gone
    await gw.runner.retry_rollbacks()
    j = gw.store.get_job(job.id)
    assert not j.rollback_pending and j.source_ids == [] and gw.store.get_notebook(j.notebook_title)[1] == 0


# -- logical titles ----------------------------------------------------------
from datetime import datetime, timezone  # noqa: E402

import logging  # noqa: E402

import pytest  # noqa: E402

OCT8 = datetime(2026, 10, 8, 15, 0, tzinfo=timezone.utc).timestamp()


async def test_explicit_title_applied_by_rename(gw, queue_job, make_consumer):
    job = await queue_job(make_consumer(), params={"format": "deep_dive", "length": "default", "language": "en",
                                                   "instructions": None, "title": "Lighthouses — history and tech"})
    await gw.runner.run_job(job)
    done = gw.store.get_job(job.id)
    assert done.status == "done" and done.title == "Lighthouses — history and tech"
    assert gw.nlm.client.renames == [(done.notebook_id, done.artifact_id, "Lighthouses — history and tech", False)]


async def test_default_title_format(gw, queue_job, make_consumer):
    gw.runner.now = lambda: OCT8
    job = await queue_job(make_consumer(), urls=["https://a.example/x", "https://b.example/y"],
                          params={"format": "deep_dive", "length": "default", "language": "en", "instructions": None, "title": None})
    await gw.runner.run_job(job)
    done = gw.store.get_job(job.id)
    assert done.title == "https://a.example/x · Deep dive · Oct 8" and gw.nlm.client.renames[0][2] == done.title
    assert done.finished_at == OCT8


async def test_default_title_date_uses_configured_timezone(gw, queue_job, make_consumer):
    gw.runner.now = lambda: datetime(2026, 10, 9, 2, 0, tzinfo=timezone.utc).timestamp()  # still Oct 8 in New York
    job = await queue_job(make_consumer(), urls=["https://a.example/x"])
    await gw.runner.run_job(job)
    assert gw.store.get_job(job.id).title == "https://a.example/x · Brief · Oct 8"


@pytest.mark.parametrize("kind,params,label", [
    ("podcast", {"format": "deep_dive", "length": "default"}, "Deep dive"),
    ("podcast", {"format": "brief", "length": "default"}, "Brief"),
    ("podcast", {"format": "critique", "length": "default"}, "Critique"),
    ("podcast", {"format": "debate", "length": "default"}, "Debate"),
    ("report", {"kind": "briefing", "return_text": False}, "Briefing"),
    ("report", {"kind": "study_guide", "return_text": False}, "Study guide"),
    ("report", {"kind": "blog", "return_text": False}, "Blog"),
])
async def test_default_title_labels(gw, queue_job, make_consumer, kind, params, label):
    gw.runner.now = lambda: OCT8
    job = await queue_job(make_consumer(), urls=["https://a.example/x"], kind=kind,
                          params={**params, "language": "en", "instructions": None})
    await gw.runner.run_job(job)
    assert gw.store.get_job(job.id).title == f"https://a.example/x · {label} · Oct 8"


async def test_default_title_truncates_and_falls_back(gw, queue_job, make_consumer):
    gw.runner.now = lambda: OCT8
    long_url = "https://a.example/" + "y" * 100
    job = await queue_job(make_consumer(), urls=[long_url])
    await gw.runner.run_job(job)
    assert gw.store.get_job(job.id).title == f"{long_url[:60]} · Brief · Oct 8"
    gw.nlm.client.source_get_error = RuntimeError("boom")
    job = await queue_job(make_consumer(), urls=["https://a.example/z"])
    await gw.runner.run_job(job)
    assert gw.store.get_job(job.id).title == "Sources · Brief · Oct 8"


async def test_upload_source_title_used(gw, queue_job, make_consumer):
    gw.runner.now = lambda: OCT8
    c = make_consumer()
    up = upload(gw, c, name="paper.pdf")
    job = await queue_job(c, urls=[], upload_ids=[up.id])
    await gw.runner.run_job(job)
    assert gw.store.get_job(job.id).title == "paper.pdf · Brief · Oct 8"


async def test_rename_failure_does_not_fail_the_job(gw, queue_job, make_consumer, caplog):
    gw.nlm.client.rename_error = RuntimeError("rename https://secret.example/abc failed: Secret Title")
    job = await queue_job(make_consumer(), urls=["https://secret.example/abc"],
                          params={"format": "brief", "length": "short", "language": "en", "instructions": None, "title": "Secret Title"})
    with caplog.at_level(logging.DEBUG):
        await gw.runner.run_job(job)
    done = gw.store.get_job(job.id)
    assert done.status == "done" and done.title is None and done.size_bytes > 0
    assert "secret" not in caplog.text.lower() and "RuntimeError" in caplog.text
    lines = [json.loads(x) for x in (gw.cfg.state_dir / "audit.jsonl").read_text().splitlines()]
    assert "secret" not in json.dumps(lines).lower()
    assert lines[-1] == {"ts": lines[-1]["ts"], "event": "rename_failed", "job_id": job.id, "renamed": False}
    assert [x["event"] for x in lines].count("job_done") == 1


async def test_rename_success_audited_without_title(gw, queue_job, make_consumer):
    job = await queue_job(make_consumer(), urls=["https://secret.example/abc"],
                          params={"format": "brief", "length": "short", "language": "en", "instructions": None, "title": "Secret Title"})
    await gw.runner.run_job(job)
    text = (gw.cfg.state_dir / "audit.jsonl").read_text()
    last = json.loads(text.splitlines()[-1])
    assert last == {"ts": last["ts"], "event": "job_renamed", "job_id": job.id, "renamed": True} and "secret" not in text.lower()


SLIDES = {"format": "pdf", "language": "en", "instructions": "focus", "title": None}
INFO = {"language": "es", "instructions": None, "title": None}


@pytest.mark.parametrize("kind,params,mime,body", [
    ("slides", SLIDES, "application/pdf", "pdf"),
    ("slides", {**SLIDES, "format": "pptx"}, "application/vnd.openxmlformats-officedocument.presentationml.presentation", "pptx"),
    ("infographic", INFO, "image/png", None),
])
async def test_studio_job_lifecycle(gw, queue_job, make_consumer, kind, params, mime, body):
    gw.runner.now = lambda: OCT8
    job = await queue_job(make_consumer(), urls=["https://a.example/x"], kind=kind, params=params)
    await gw.runner.run_job(job)
    done = gw.store.get_job(job.id)
    fake = gw.nlm.client
    expected = fake.infographic_bytes if body is None else fake.slides_bytes[body]
    assert done.status == "done" and done.mime == mime and done.size_bytes == len(expected) and done.accepted
    gen_kind, nb, kw = fake.generated[0]
    assert gen_kind == kind and nb == done.notebook_id and kw["source_ids"] == done.source_ids
    assert kw["language"] == params["language"] and kw["instructions"] == params["instructions"]
    if body is not None:
        assert fake.slide_formats == [body]
    label = "Slides" if kind == "slides" else "Infographic"
    assert done.title == f"https://a.example/x · {label} · Oct 8" and fake.renames[0][2] == done.title
    assert list((gw.cfg.state_dir / "tmp").iterdir()) == []


@pytest.mark.parametrize("kind,params", [("slides", SLIDES), ("infographic", INFO)])
async def test_studio_rollback_invariants(gw, queue_job, make_consumer, kind, params):
    c = make_consumer()
    gw.nlm.client.reject_urls = {"https://bad.example/"}
    job = await queue_job(c, urls=["https://ok.example/", "https://bad.example/"], kind=kind, params=params)
    await gw.runner.run_job(job)
    failed = gw.store.get_job(job.id)
    assert failed.status == "failed" and failed.error_code == "source_rejected" and failed.source_ids == []
    assert len(gw.nlm.client.deleted_sources) == 1 and gw.store.get_notebook(failed.notebook_title)[1] == 0
    assert gw.nlm.client.generated == [] and gw.store.count_billable(kind, 0) == 0
    gw.nlm.client.reject_urls = set()
    gw.nlm.client.refuse_generation = True
    job = await queue_job(c, kind=kind, params=params)
    await gw.runner.run_job(job)
    assert gw.store.get_job(job.id).error_code == "rate_limited" and gw.store.count_billable(kind, 0) == 0
    gw.nlm.client.refuse_generation = False
    gw.nlm.client.fail_generation = True
    job = await queue_job(c, kind=kind, params=params)
    await gw.runner.run_job(job)
    j = gw.store.get_job(job.id)
    assert j.error_code == "generation_failed" and j.accepted and j.source_ids  # accepted: sources kept, billable
    assert gw.store.count_billable(kind, 0) == 1
