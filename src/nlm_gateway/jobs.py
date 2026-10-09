"""Job state machine: new -> queued -> running -> done | failed.

Invariant: a job's sources are removed from its notebook iff the job fails
before Google accepted generation; earlier jobs' sources are never touched.
Jobs of kind "sources" (add_sources) never generate and never remove anything.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from notebooklm import AudioFormat, AudioLength, NetworkError, ReportFormat, SourceNotFoundError

from .errors import GatewayError, classify
from .fetch import fetch_to_temp, mime_for

log = logging.getLogger("nlm_gateway.jobs")

REPORT_KINDS = {"briefing": ReportFormat.BRIEFING_DOC, "study_guide": ReportFormat.STUDY_GUIDE, "blog": ReportFormat.BLOG_POST}
MAINTENANCE_S = 300
ROLLBACK_MAX_ATTEMPTS = 50  # then stop retrying; the reservation is kept (count stays high = safe)
# Default artifact title: "<first source title> · <Label> · <Mon D>"
TITLE_LABELS = {"deep_dive": "Deep dive", "brief": "Brief", "critique": "Critique", "debate": "Debate",
                "briefing": "Briefing", "study_guide": "Study guide", "blog": "Blog",
                "slides": "Slides", "infographic": "Infographic"}
# After acceptance these leave the job running for a later re-attach instead of failing it.
_TRANSIENT_CODES = frozenset({"auth_expired", "rate_limited"})


class JobRunner:
    def __init__(self, cfg, store, nlm, notebooks, audit):
        self.cfg, self.store, self.nlm, self.notebooks, self.audit = cfg, store, nlm, notebooks, audit
        self._wake = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._reattach: list[asyncio.Task] = []
        self._active: set[str] = set()  # job ids being run or re-attached in this process
        self.now = time.time  # clock for finished_at and the default title date (tests replace it)

    # -- lifecycle -------------------------------------------------------
    async def start(self) -> None:
        await self.recover()
        for _ in range(self.cfg.workers):
            self._tasks.append(asyncio.create_task(self._worker()))
        self._tasks.append(asyncio.create_task(self._maintenance()))

    async def stop(self) -> None:
        for t in self._tasks + self._reattach:
            t.cancel()
        await asyncio.gather(*self._tasks, *self._reattach, return_exceptions=True)

    async def drain(self) -> None:
        """Test helper: wait for re-attach tasks started by recover()."""
        await asyncio.gather(*self._reattach)

    def wake(self) -> None:
        self._wake.set()

    async def _worker(self) -> None:
        while True:
            try:
                job = self.store.claim_next(time.time())
                if job is None:
                    self._wake.clear()
                    try:
                        await asyncio.wait_for(self._wake.wait(), timeout=5)
                    except TimeoutError:
                        pass
                    continue
                await self.run_job(job)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # type name only: exception text/tracebacks can carry URLs
                log.error("worker iteration failed: %s", type(e).__name__)
                await asyncio.sleep(1)

    async def _maintenance(self) -> None:
        while True:
            await asyncio.sleep(MAINTENANCE_S)
            try:
                await self.sweep_uploads(time.time())
                if self.nlm.auth_ok:
                    await self.retry_rollbacks()
                await self.nlm.health_check()
                if self.nlm.auth_ok:
                    self.reattach_stalled()
            except Exception as e:
                log.error("maintenance pass failed: %s", type(e).__name__)

    # -- the state machine -------------------------------------------------
    async def run_job(self, job) -> None:
        if job.kind == "sources":
            return await self._run_sources(job)
        requested = False
        self._active.add(job.id)
        try:
            client = self.nlm.require()
            added: list[str] = list(job.source_ids)
            for i, url in enumerate(job.urls):
                try:
                    src = await client.sources.add_url(job.notebook_id, url, wait=True, wait_timeout=300)
                except Exception as e:
                    raise self._source_error(e, i) from None
                added.append(src.id)
                self.store.update_job(job.id, time.time(), source_ids=added)
            for j, up in enumerate(self.store.uploads_for_job(job.id)):
                i = len(job.urls) + j
                try:
                    src = await client.sources.add_file(job.notebook_id, Path(up.path), wait=True, wait_timeout=600, title=up.filename)
                except Exception as e:
                    raise self._source_error(e, i) from None
                added.append(src.id)
                self.store.update_job(job.id, time.time(), source_ids=added)
                self._drop_upload(up)
            # From here Google may be generating from these sources: only an explicit refusal rolls back.
            self.store.update_job(job.id, time.time(), generation_requested=True)
            requested = True
            status = await self._generate(client, job, added)
            if status.is_failed:
                code = "rate_limited" if status.error_code == "USER_DISPLAYABLE_ERROR" else "generation_failed"
                await self._fail(job.id, GatewayError(code, "Google refused to start generation (often the daily limit); try again later."))
                return
            self.store.update_job(job.id, time.time(), task_id=status.task_id, accepted=True)
            await self._await_result(job.id)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            await self._fail(job.id, classify(e), rollback=not requested)
        finally:
            self._active.discard(job.id)

    async def _run_sources(self, job) -> None:
        """Add every source, recording failures by index and continuing; auth/rate trouble aborts the rest.
        Nothing is ever deleted: added sources stay, whatever happens."""
        added, failed = list(job.source_ids), list(job.failed_indices)
        err: GatewayError | None = None
        self._active.add(job.id)
        try:
            client = self.nlm.require()
            ups = {u.id: u for u in self.store.uploads_for_job(job.id)}
            items = [(i, url, None) for i, url in enumerate(job.urls)]
            items += [(len(job.urls) + j, None, ups.get(uid)) for j, uid in enumerate(job.upload_ids)]
            for i, url, up in items:
                if url is None and up is None:
                    continue  # upload no longer on disk (should not happen); not counted either way
                try:
                    if url is not None:
                        src = await client.sources.add_url(job.notebook_id, url, wait=True, wait_timeout=300)
                    else:
                        src = await client.sources.add_file(job.notebook_id, Path(up.path), wait=True, wait_timeout=600,
                                                            title=up.filename)
                except Exception as e:
                    g = self._source_error(e, i)
                    if g.code != "source_rejected":
                        err = g
                        break
                    failed.append(i)
                    self.store.update_job(job.id, time.time(), failed_indices=failed)
                else:
                    added.append(src.id)
                    self.store.update_job(job.id, time.time(), source_ids=added)
                if up is not None:
                    self._drop_upload(up)
            if err is None and not added:
                err = GatewayError("source_rejected", "None of the sources could be added.")
        except asyncio.CancelledError:
            raise  # left running; recover() ends it without removing anything
        except Exception as e:
            err = classify(e)
        finally:
            self._active.discard(job.id)
        self._end_sources(job.id, err)

    def _end_sources(self, job_id: str, err: GatewayError | None) -> None:
        """Finish a "sources" job: keep every added source, release the slots of the rest, drop leftover uploads."""
        job = self.store.get_job(job_id)
        if job is None or job.status in ("failed", "done"):
            return
        now = time.time()
        fields = {"status": "done"} if err is None else {"status": "failed", "error_code": err.code, "error_message": err.message}
        self.store.update_job(job_id, now, urls=[], finished_at=now, **fields)
        if job.notebook_title:
            self.notebooks.release(job.notebook_title, job.planned_sources - len(job.source_ids))
        for up in self.store.uploads_for_job(job_id):
            self._drop_upload(up)
        counts = {"kind": job.kind, "notebook_id": job.notebook_id, "added": len(job.source_ids),
                  "failed": len(job.failed_indices)}
        if err is None:
            self.audit.write("job_done", consumer=job.consumer_id, job_id=job_id, **counts)
        else:
            self.audit.write("job_failed", consumer=job.consumer_id, job_id=job_id, code=err.code, **counts)

    @staticmethod
    def _transient(exc: BaseException) -> bool:
        return classify(exc).code in _TRANSIENT_CODES or isinstance(exc, (NetworkError, ConnectionError))

    async def _await_result(self, job_id: str) -> None:
        """Wait for an accepted job and record it; auth/rate/network trouble defers to a later re-attach."""
        try:
            await self._finish(job_id)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if self._transient(e):
                log.warning("job %s: waiting for the result deferred (%s); will re-attach", job_id, classify(e).code)
                return
            await self._fail(job_id, classify(e))

    def _source_error(self, exc: BaseException, index: int) -> GatewayError:
        g = classify(exc)
        if g.code in ("auth_expired", "rate_limited"):
            return g
        return GatewayError("source_rejected", f"Source #{index} could not be added (numbering: urls first, then upload_ids).")

    async def _generate(self, client, job, source_ids):
        p = job.params
        if job.kind == "podcast":
            return await client.artifacts.generate_audio(
                job.notebook_id, source_ids=source_ids, instructions=p.get("instructions") or None,
                audio_format=AudioFormat[p["format"].upper()], audio_length=AudioLength[p["length"].upper()],
                language=p["language"])
        if job.kind == "slides":
            return await client.artifacts.generate_slide_deck(
                job.notebook_id, source_ids=source_ids, language=p["language"], instructions=p.get("instructions") or None)
        if job.kind == "infographic":
            return await client.artifacts.generate_infographic(
                job.notebook_id, source_ids=source_ids, language=p["language"], instructions=p.get("instructions") or None)
        return await client.artifacts.generate_report(
            job.notebook_id, report_format=REPORT_KINDS[p["kind"]], source_ids=source_ids,
            language=p["language"], extra_instructions=p.get("instructions") or None)

    async def _finish(self, job_id: str) -> None:
        client = self.nlm.require()
        job = self.store.get_job(job_id)
        final = await client.artifacts.wait_for_completion(job.notebook_id, job.task_id, timeout=self.cfg.generation_timeout_s)
        if final.is_failed:
            raise GatewayError("generation_failed", "NotebookLM reported the generation as failed.")
        job = self.store.get_job(job_id)
        path = await fetch_to_temp(client, job, self.cfg.state_dir / "tmp")
        try:
            size = path.stat().st_size
            text = None
            if job.kind == "report" and job.params.get("return_text") and size <= self.cfg.inline_text_max:
                text = path.read_text(errors="replace")
        finally:
            path.unlink(missing_ok=True)
        finished = self.now()
        self.store.update_job(job_id, finished, status="done", artifact_id=job.task_id, size_bytes=size,
                              mime=mime_for(job), text=text, urls=[], finished_at=finished)
        self.audit.write("job_done", consumer=job.consumer_id, job_id=job_id, kind=job.kind, bytes=size,
                         notebook_id=job.notebook_id)
        await self._apply_title(client, job, finished)

    async def _apply_title(self, client, job, finished: float) -> None:
        """Rename the finished artifact; failure never fails the job. Titles derive from caller
        content, so neither the title nor the exception text is logged or audited."""
        try:
            title = job.params.get("title") or await self._default_title(client, job, finished)
            await client.artifacts.rename(job.notebook_id, job.task_id, title, return_object=False)
        except Exception as e:
            log.warning("job %s: artifact rename failed (%s)", job.id, type(e).__name__)
            self.audit.write("rename_failed", job_id=job.id, renamed=False)
            return
        self.store.update_job(job.id, time.time(), title=title)
        self.audit.write("job_renamed", job_id=job.id, renamed=True)

    async def _default_title(self, client, job, finished: float) -> str:
        first = "Sources"
        if job.source_ids:
            try:
                src_title = (await client.sources.get(job.notebook_id, job.source_ids[0])).title
                first = (src_title or "").strip()[:60] or first
            except Exception:
                pass  # keep "Sources"
        key = job.params["format"] if job.kind == "podcast" else job.params["kind"] if job.kind == "report" else job.kind
        label = TITLE_LABELS[key]
        when = datetime.fromtimestamp(finished, ZoneInfo(self.cfg.timezone))
        return f"{first} · {label} · {when:%b} {when.day}"

    async def _fail(self, job_id: str, err: GatewayError, rollback: bool = True) -> None:
        job = self.store.get_job(job_id)
        if job is None or job.status in ("failed", "done"):
            return
        rollback = rollback and job.kind != "sources"  # add-only jobs never remove anything
        remaining: list[str] = []
        if not job.accepted and rollback:
            remaining = await self._delete_sources(job.notebook_id, list(job.source_ids))
        rolled_back = not job.accepted and rollback
        self.store.update_job(job_id, time.time(), status="failed", error_code=err.code, error_message=err.message,
                              urls=[], finished_at=time.time(),
                              **({"source_ids": remaining, "rollback_pending": bool(remaining)} if rolled_back else {}))
        if rolled_back:
            if job.notebook_title:
                self.notebooks.release(job.notebook_title, job.planned_sources - len(remaining))
            for up in self.store.uploads_for_job(job_id):
                self._drop_upload(up)
        self.audit.write("job_failed", consumer=job.consumer_id, job_id=job_id, kind=job.kind, code=err.code,
                         accepted=job.accepted, rolled_back=(len(job.source_ids) - len(remaining)) if rolled_back else 0,
                         rollback_pending=len(remaining))

    async def _delete_sources(self, notebook_id, ids: list[str]) -> list[str]:
        """Delete each id independently; return the ids that could not be deleted."""
        try:
            client = self.nlm.require()
        except Exception:
            log.warning("could not roll back %d source(s): client unavailable", len(ids))
            return ids
        remaining = []
        for sid in ids:
            try:
                await client.sources.delete(notebook_id, sid)
            except SourceNotFoundError:
                pass  # already gone: upstream delete is idempotent, so this counts as deleted
            except Exception:
                remaining.append(sid)
        if remaining:
            log.warning("could not delete %d of %d source(s); will retry", len(remaining), len(ids))
        return remaining

    async def retry_rollbacks(self) -> None:
        for job in self.store.jobs_pending_rollback():
            if job.kind == "sources":  # never set for add-only jobs; never act on it if it were
                continue
            try:
                remaining = await self._delete_sources(job.notebook_id, list(job.source_ids))
                done = len(job.source_ids) - len(remaining)
                attempts = job.rollback_attempts + (1 if remaining else 0)
                abandon = bool(remaining) and attempts >= ROLLBACK_MAX_ATTEMPTS
                self.store.update_job(job.id, time.time(), source_ids=remaining, rollback_attempts=attempts,
                                      rollback_pending=bool(remaining) and not abandon)
                if job.notebook_title and done:
                    self.notebooks.release(job.notebook_title, done)
                if abandon:
                    self.audit.write("rollback_abandoned", consumer=job.consumer_id, job_id=job.id,
                                     sources=len(remaining), attempts=attempts)
            except Exception as e:
                log.error("rollback retry failed for job %s: %s", job.id, type(e).__name__)

    def _drop_upload(self, up) -> None:
        Path(up.path).unlink(missing_ok=True)
        self.store.delete_upload(up.id)

    # -- restart + housekeeping ------------------------------------------
    async def recover(self) -> None:
        for job in self.store.jobs_with_status("new"):  # submit() was interrupted: nothing was added at Google
            self.store.release_uploads(job.id)
            self.store.update_job(job.id, time.time(), status="failed", error_code="gateway_restarted",
                                  error_message="The gateway restarted while accepting this job; please resubmit.",
                                  urls=[], finished_at=time.time())
            if job.notebook_title:
                self.notebooks.release(job.notebook_title, job.planned_sources)
            self.audit.write("job_failed", consumer=job.consumer_id, job_id=job.id, kind=job.kind, code="gateway_restarted",
                             accepted=False, rolled_back=0, rollback_pending=0)
        for job in self.store.jobs_with_status("running"):
            if job.kind == "sources":  # keep what was added; release the rest
                self._end_sources(job.id, GatewayError(
                    "gateway_restarted", "The gateway restarted while adding sources; the ones already added were kept."))
            elif job.accepted and job.task_id:
                self._spawn_reattach(job.id)
            else:
                await self._fail(job.id, GatewayError("gateway_restarted", "The gateway restarted before generation started; please resubmit."),
                                 rollback=not job.generation_requested)

    def reattach_stalled(self) -> None:
        """Re-attach accepted jobs left running (e.g. auth was down at boot) that nothing here is handling."""
        for job in self.store.jobs_with_status("running"):
            if job.accepted and job.task_id and job.id not in self._active:
                self._spawn_reattach(job.id)

    def _spawn_reattach(self, job_id: str) -> None:
        self._active.add(job_id)
        self._reattach = [t for t in self._reattach if not t.done()]
        self._reattach.append(asyncio.create_task(self._reattach_job(job_id)))

    async def _reattach_job(self, job_id: str) -> None:
        try:
            await self._await_result(job_id)
        finally:
            self._active.discard(job_id)

    async def sweep_uploads(self, now: float) -> None:
        for up in self.store.expired_uploads(now):
            self._drop_upload(up)
        tmp = self.cfg.state_dir / "tmp"
        if tmp.exists():
            for p in tmp.iterdir():
                try:
                    if p.stat().st_mtime < now - 3600:
                        p.unlink(missing_ok=True)
                except FileNotFoundError:
                    pass
