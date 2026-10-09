"""MCP tool handlers as plain async functions (unit-testable without MCP plumbing)."""
from __future__ import annotations

import asyncio
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Awaitable, Callable

from . import quota
from .audit import digest
from .errors import GatewayError, classify
from .links import SOURCE_REF_TTL_S, make_source_ref, share_url

PODCAST_FORMATS = ("deep_dive", "brief", "critique", "debate")
PODCAST_LENGTHS = ("short", "default", "long")
REPORT_KINDS = ("briefing", "study_guide", "blog")
SLIDE_FORMATS = ("pdf", "pptx")
KIND_SCOPE = {"podcast": "podcast", "report": "report", "slides": "studio", "infographic": "studio", "sources": "add"}
LIST_TIMEOUT_S = 50  # keeps list_notebooks inside the 60 s tool budget
READ_TIMEOUT_S = 50  # whole-call bound for the synchronous read/notes tools (60 s tool budget)
ASK_TIMEOUT_S = 50  # whole-call bound for ask_notebook
_LANG = re.compile(r"^[a-z]{2,3}(-[A-Za-z0-9]{2,8})?$")


def _bad(msg: str) -> GatewayError:
    return GatewayError("bad_request", msg)


def _str_list(args: dict, key: str) -> list[str]:
    v = args.get(key, [])
    if v is None:
        return []
    if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
        raise _bad(f"{key} must be a list of strings")
    return v


def _enum(args, key, allowed, default):
    v = args.get(key) or default
    if v not in allowed:
        raise _bad(f"{key} must be one of {', '.join(allowed)}")
    return v


def _name(args, key: str, max_len: int = 100) -> str | None:
    """Optional short human string (notebook or artifact title); the value is never echoed back."""
    v = args.get(key)
    if v is None:
        return None
    if not isinstance(v, str) or not (v := v.strip()) or len(v) > max_len or not v.isprintable():
        raise _bad(f"{key} must be 1-{max_len} printable characters")
    return v


_BIDI = frozenset("\u202a\u202b\u202c\u202d\u202e\u2066\u2067\u2068\u2069")


def _free_text(s: str) -> str | None:
    """Pasted free text: CRLF / lone CR become LF; then refuse only C0/C1 controls other than LF and tab, and the
    bidi embedding/override/isolate controls. Everything else (NBSP, ZWJ emoji, soft hyphen, private use...) passes.
    Returns the normalized text, or None if refused."""
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    for ch in s:
        if ch in _BIDI or (unicodedata.category(ch) == "Cc" and ch not in "\n\t"):
            return None
    return s


def _sources(args, cfg) -> tuple[list[str], list[str]]:
    urls, upload_ids = _str_list(args, "urls"), _str_list(args, "upload_ids")
    if not urls and not upload_ids:
        raise _bad("provide at least one of urls or upload_ids")
    if len(urls) + len(upload_ids) > cfg.notebook_source_cap:
        raise _bad(f"at most {cfg.notebook_source_cap} sources per call")
    for i, u in enumerate(urls):
        if not re.match(r"^https?://", u) or len(u) > 2048:
            raise _bad(f"urls[{i}] must be an http(s) URL under 2048 characters")
    return urls, upload_ids


def _common(args, cfg) -> tuple[list[str], list[str], str, str | None]:
    urls, upload_ids = _sources(args, cfg)
    language = args.get("language") or "en"
    if not isinstance(language, str) or not _LANG.match(language):
        raise _bad("language must be a code like en, es, pt-BR")
    instructions = args.get("instructions")
    if instructions is not None and (not isinstance(instructions, str) or len(instructions) > 2000):
        raise _bad("instructions must be a string of at most 2000 characters")
    return urls, upload_ids, language, instructions or None


async def _share_url(client, notebook_id: str) -> str:
    url = client.notebooks.get_share_url(notebook_id)  # sync in 0.8.4; tolerate an async version
    if hasattr(url, "__await__"):
        url = await url
    return url


async def submit(gw, consumer, kind: str, args: dict) -> dict:
    if KIND_SCOPE[kind] not in consumer.scopes:
        raise GatewayError("not_found", "unknown tool")
    if kind == "sources":  # add only: no generation, no quota, no title
        urls, upload_ids = _sources(args, gw.cfg)
        notebook, params = _name(args, "notebook"), {}
    else:
        urls, upload_ids, language, instructions = _common(args, gw.cfg)
        notebook, title = _name(args, "notebook"), _name(args, "title")
    if kind == "podcast":
        params = {"format": _enum(args, "format", PODCAST_FORMATS, "deep_dive"),
                  "length": _enum(args, "length", PODCAST_LENGTHS, "default"),
                  "language": language, "instructions": instructions, "title": title}
    elif kind == "report":
        params = {"kind": _enum(args, "kind", REPORT_KINDS, "briefing"), "language": language,
                  "instructions": instructions, "return_text": bool(args.get("return_text", False)), "title": title}
    elif kind == "slides":
        params = {"format": _enum(args, "format", SLIDE_FORMATS, "pdf"), "language": language,
                  "instructions": instructions, "title": title}
    elif kind == "infographic":
        params = {"language": language, "instructions": instructions, "title": title}
    gw.nlm.require()  # auth_expired before anything is recorded
    now = gw.now()
    # check + create_job + claim_uploads run with no await in between (atomic on the event loop)
    if kind != "sources":
        quota.check(gw.store, gw.cfg, consumer, kind, now)
    job = gw.store.create_job(consumer.id, kind, params, urls, upload_ids, now)
    try:
        gw.store.claim_uploads(upload_ids, consumer.id, job.id, now)
    except Exception as e:  # any failure must not leave a 'new' job behind
        err = classify(e)
        gw.store.update_job(job.id, now, status="failed", error_code=err.code, error_message=err.message, urls=[])
        raise err from None
    try:
        nb_id, nb_title = await gw.notebooks.resolve(consumer, notebook, len(urls) + len(upload_ids))
        url = await _share_url(gw.nlm.require(), nb_id)
    except Exception as e:  # CancelledError propagates; recover() fails the leftover 'new' job
        err = classify(e)
        gw.store.release_uploads(job.id)
        gw.store.update_job(job.id, gw.now(), status="failed", error_code=err.code, error_message=err.message, urls=[])
        raise err from None
    gw.store.update_job(job.id, gw.now(), status="queued", notebook_id=nb_id, notebook_title=nb_title, notebook_url=url)
    gw.runner.wake()
    if kind == "sources":
        gw.audit.write("submit", consumer=consumer.id, job_id=job.id, kind=kind, urls=len(urls),
                       uploads=len(upload_ids), notebook_id=nb_id)
    else:
        gw.audit.write("submit", consumer=consumer.id, job_id=job.id, kind=kind, url_hashes=[digest(u) for u in urls],
                       uploads=len(upload_ids), notebook_id=nb_id)
    return {"job_id": job.id, "notebook_url": url, "status": "queued"}


async def podcast_from_sources(gw, consumer, args):
    return await submit(gw, consumer, "podcast", args)


async def report_from_sources(gw, consumer, args):
    return await submit(gw, consumer, "report", args)


async def slides_from_sources(gw, consumer, args):
    return await submit(gw, consumer, "slides", args)


async def infographic_from_sources(gw, consumer, args):
    return await submit(gw, consumer, "infographic", args)


async def add_sources(gw, consumer, args):
    return await submit(gw, consumer, "sources", args)


def _own_job(gw, consumer, args):
    job_id = args.get("job_id")
    job = gw.store.get_job(job_id, consumer.id) if isinstance(job_id, str) else None
    if job is None:
        raise GatewayError("not_found", "no such job")
    return job


async def job_status(gw, consumer, args):
    job = _own_job(gw, consumer, args)
    st = "queued" if job.status in ("new", "queued") else job.status
    if job.kind == "sources":  # nothing to download: progress counts only
        out = {"job_id": job.id, "kind": job.kind, "status": st, "notebook_url": job.notebook_url,
               "added": len(job.source_ids), "failed": list(job.failed_indices)}
        if job.status == "failed":
            out["error"] = {"code": job.error_code, "message": job.error_message}
        return out
    out = {"job_id": job.id, "kind": job.kind, "status": st, "notebook_url": job.notebook_url, "title": job.title}
    if job.status == "failed":
        out["error"] = {"code": job.error_code, "message": job.error_message}
    if job.status == "done":
        out.update(download_url=f"{gw.cfg.public_base_url}/jobs/{job.id}/download", bytes=job.size_bytes, mime=job.mime)
        if job.text is not None:
            out["text"] = job.text
        if job.size_bytes and job.size_bytes > gw.cfg.big_file_bytes:
            out["share_url"], out["share_expires_at"] = share_url(gw.cfg, gw.link_key, job, gw.cfg.link_ttl_default_h, gw.now())
    return out


async def quota_status(gw, consumer, args):
    return quota.snapshot(gw.store, gw.cfg, consumer, gw.now())


async def share_link(gw, consumer, args):
    job = _own_job(gw, consumer, args)
    if job.kind == "sources":
        raise _bad("an add_sources job has nothing to download")
    if job.status != "done":
        raise _bad("the job is not done yet; call job_status first")
    ttl = args.get("ttl_hours", gw.cfg.link_ttl_default_h)
    if not isinstance(ttl, int) or isinstance(ttl, bool):
        raise _bad("ttl_hours must be an integer")
    ttl = max(1, min(ttl, gw.cfg.link_ttl_max_h))
    url, exp = share_url(gw.cfg, gw.link_key, job, ttl, gw.now())
    gw.audit.write("share_link", consumer=consumer.id, job_id=job.id, ttl_hours=ttl)
    return {"url": url, "expires_at": exp, "bytes": job.size_bytes}


async def list_notebooks(gw, consumer, args):
    if "list" not in consumer.scopes:
        raise GatewayError("not_found", "unknown tool")
    client = gw.nlm.require()
    try:
        nbs = await asyncio.wait_for(client.notebooks.list(), timeout=LIST_TIMEOUT_S)
    except TimeoutError:
        raise GatewayError("generation_timeout", "NotebookLM did not list the notebooks in time; try again later.") from None
    # newest first when upstream has creation times (stable sort keeps upstream order otherwise; undated last)
    nbs = sorted(nbs, key=lambda nb: (nb.created_at is not None, nb.created_at.timestamp() if nb.created_at else 0.0),
                 reverse=True)
    out = [{"title": nb.title, "sources": int(nb.sources_count or 0), "url": await _share_url(client, nb.id)} for nb in nbs]
    gw.audit.write("list_notebooks", consumer=consumer.id, count=len(out))
    return {"notebooks": out}


def _need_scope(consumer, scope: str) -> None:
    if scope not in consumer.scopes:
        raise GatewayError("not_found", "unknown tool")


LOOKUP_MAX = 1000  # existing note/source titles can be longer than titles we create


def _need_name(args, key: str, max_len: int = 100) -> str:
    v = _name(args, key, max_len)
    if v is None:
        raise _bad(f"{key} is required")
    return v


async def _bounded(coro, timeout: float, message: str):
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except TimeoutError:
        raise GatewayError("generation_timeout", message) from None


_SLOW = "NotebookLM did not answer in time; try again later."


async def find_notebook(client, title: str) -> str:
    """Id of the existing notebook with exactly this title; never creates or reserves one."""
    for nb in await client.notebooks.list():
        if nb.title == title:
            return nb.id
    raise GatewayError("not_found", "no notebook with that title")


def _enum_text(v) -> str:
    return (v.name if isinstance(v, Enum) else str(v)).lower()


async def list_sources(gw, consumer, args):
    _need_scope(consumer, "read")
    title = _need_name(args, "notebook")
    client = gw.nlm.require()

    async def run():
        nb_id = await find_notebook(client, title)
        return nb_id, await client.sources.list(nb_id)
    nb_id, sources = await _bounded(run(), READ_TIMEOUT_S, _SLOW)
    out = [{"title": s.title, "kind": _enum_text(s.kind), "status": _enum_text(s.status)} for s in sources]
    gw.audit.write("list_sources", consumer=consumer.id, notebook_id=nb_id, count=len(out))
    return {"notebook": title, "sources": out}


async def ask_notebook(gw, consumer, args):
    _need_scope(consumer, "read")
    title = _need_name(args, "notebook")
    q = args.get("question")
    if not isinstance(q, str) or (q := _free_text(q.strip())) is None or not q or len(q) > 2000:
        raise _bad("question must be 1-2000 characters without control characters (newlines and tabs allowed)")
    wanted = args.get("source_titles")
    if wanted is not None and (not isinstance(wanted, list) or len(wanted) > 300
                               or not all(isinstance(x, str) and 0 < len(x) <= LOOKUP_MAX for x in wanted)):
        raise _bad(f"source_titles must be a list of at most 300 strings of 1-{LOOKUP_MAX} characters")
    client = gw.nlm.require()
    quota.check(gw.store, gw.cfg, consumer, "chat", gw.now())

    async def run():
        nb_id = await find_notebook(client, title)
        sources = await client.sources.list(nb_id)
        source_ids = None
        if wanted:
            source_ids = []
            for i, t in enumerate(wanted):
                ids = [s.id for s in sources if s.title == t]
                if not ids:
                    raise _bad(f"source_titles[{i}] is not a source in that notebook")
                source_ids += [x for x in ids if x not in source_ids]
        return nb_id, sources, await client.chat.ask(nb_id, q, source_ids=source_ids)
    nb_id, sources, result = await _bounded(run(), ASK_TIMEOUT_S,
                                            "NotebookLM did not answer within 50 s; try again or narrow the question.")
    gw.store.add_event(consumer.id, "chat", gw.now())
    titles = {s.id: s.title for s in sources}
    citations: list[dict] = []
    for ref in result.references or []:
        c = {"source_title": titles.get(ref.source_id)}
        if ref.cited_text:
            c["excerpt"] = ref.cited_text
        if c not in citations:
            citations.append(c)
    gw.audit.write("ask", consumer=consumer.id, notebook_id=nb_id, question_chars=len(q))
    return {"answer": result.answer, "citations": citations}


NOTE_TEXT_MAX = 100_000
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


async def read_note(gw, consumer, args):
    _need_scope(consumer, "notes")
    nb_title = _need_name(args, "notebook")
    title = _name(args, "title", LOOKUP_MAX)
    client = gw.nlm.require()

    async def run():
        nb_id = await find_notebook(client, nb_title)
        return nb_id, await client.notes.list(nb_id)
    nb_id, notes = await _bounded(run(), READ_TIMEOUT_S, _SLOW)
    if title is None:
        gw.audit.write("read_note", consumer=consumer.id, notebook_id=nb_id, count=len(notes))
        return {"notes": [{"title": n.title, "chars": len(n.content or "")} for n in notes]}
    matches = [n for n in notes if n.title == title]
    if not matches:
        raise GatewayError("not_found", "no note with that title")
    n = max(matches, key=lambda m: m.created_at or _EPOCH)  # most recent; first listed wins ties
    text = n.content or ""
    out = {"title": n.title, "text": text[:gw.cfg.inline_text_max]}
    if len(text) > gw.cfg.inline_text_max:
        out["truncated"] = True
    if len(matches) > 1:
        out["ambiguous"] = True
    gw.audit.write("read_note", consumer=consumer.id, notebook_id=nb_id, chars=len(text))
    return out


async def create_note(gw, consumer, args):
    _need_scope(consumer, "notes")
    nb_title = _need_name(args, "notebook")
    title = _need_name(args, "title")
    text = args.get("text")
    if not isinstance(text, str) or (text := _free_text(text)) is None or not text.strip() or len(text) > NOTE_TEXT_MAX:
        raise _bad(f"text must be 1-{NOTE_TEXT_MAX} characters without control characters (newlines and tabs allowed)")
    client = gw.nlm.require()

    async def run():
        nb_id = await find_notebook(client, nb_title)
        await client.notes.create(nb_id, title=title, content=text)
        return nb_id
    # A timeout is ambiguous: upstream may have created the note, or left a blank one. Say so.
    nb_id = await _bounded(run(), READ_TIMEOUT_S,
                           "NotebookLM did not confirm the note within 50 s; check with read_note before retrying.")
    gw.audit.write("create_note", consumer=consumer.id, notebook_id=nb_id, text_chars=len(text))
    return {"title": title, "created": True}


async def source_fulltext(client, nb_title: str, source_title: str) -> tuple[str, str, str, str, bool]:
    """(notebook_id, source_id, source title, full text, ambiguous) for the first source with this exact title."""
    nb_id = await find_notebook(client, nb_title)
    matches = [s for s in await client.sources.list(nb_id) if s.title == source_title]
    if not matches:
        raise GatewayError("not_found", "no source with that title")
    full = await client.sources.get_fulltext(nb_id, matches[0].id, output_format="text")
    return nb_id, matches[0].id, matches[0].title, full.content or "", len(matches) > 1


async def get_source_text(gw, consumer, args):
    _need_scope(consumer, "notes")
    nb_title = _need_name(args, "notebook")
    source_title = _need_name(args, "source_title", LOOKUP_MAX)
    client = gw.nlm.require()
    nb_id, src_id, title, text, ambiguous = await _bounded(source_fulltext(client, nb_title, source_title), READ_TIMEOUT_S, _SLOW)
    out = {"title": title, "chars": len(text)}
    if len(text) <= gw.cfg.inline_text_max:
        out["text"] = text
    else:
        ref = make_source_ref(gw.link_key, nb_id, src_id, consumer.id, int(gw.now()) + SOURCE_REF_TTL_S)
        out.update(truncated=True, text=text[:gw.cfg.inline_text_max], download_url=f"{gw.cfg.public_base_url}/sources/text/{ref}")
    if ambiguous:
        out["ambiguous"] = True
    gw.audit.write("get_source_text", consumer=consumer.id, notebook_id=nb_id, chars=len(text))
    return out


@dataclass(frozen=True)
class ToolSpec:
    name: str
    scope: str | None
    description: str
    input_schema: dict
    handler: Callable[..., Awaitable[dict]]


_SOURCES = {
    "urls": {"type": "array", "items": {"type": "string"}, "description": "Public http(s) links (web pages, YouTube, PDFs) to use as sources."},
    "upload_ids": {"type": "array", "items": {"type": "string"}, "description": "ids returned by PUT {base}/uploads?filename=<name> (curl -T file). Files: pdf, docx, pptx, txt, md, csv, epub, audio, images."},
    "language": {"type": "string", "description": "Output language code, default en."},
    "instructions": {"type": "string", "description": "Optional focus/steering instructions (max 2000 chars)."},
    "notebook": {"type": "string", "minLength": 1, "maxLength": 100,
                 "description": "Exact title of the user's NotebookLM notebook to add the sources to (created if it does not exist). "
                                "Pass ONLY when the user names a notebook; omit it to use the user's default notebook \"To Review\"."},
    "title": {"type": "string", "minLength": 1, "maxLength": 100,
              "description": "ALWAYS pass: a short human title for the result describing the request, e.g. \"Lighthouses — history and tech\". "
                             "Shown in the user's NotebookLM app."},
}
_NOTEBOOK_EXISTING = {"type": "string", "minLength": 1, "maxLength": 100,
                      "description": "Exact title of an existing notebook of the user's (see list_notebooks)."}
_FLOW = ("Sources are added to the user's NotebookLM notebook (default \"To Review\", or the one named in `notebook`). "
         "Returns immediately with job_id and notebook_url — reply to the user with notebook_url; the result appears in "
         "the user's NotebookLM app under `title` when ready, typically 5-20 min. Do NOT wait or poll inside this turn. "
         "If the user wants the FILE: schedule a one-shot task ~10 minutes out that calls job_status; when status is done, "
         "curl -o /workspace/agent/<name> the download_url and send_file it; if not done, reschedule. If bytes > 45 MB "
         "(Telegram limit) do not send_file — post share_url (tap-to-download link) instead. Only links/files you provide are used.")

TOOLS = [
    ToolSpec("podcast_from_sources", "podcast",
             "Create a NotebookLM Audio Overview podcast (two-host audio discussion, M4A) from the given links and uploaded files. " + _FLOW,
             {"type": "object", "properties": {**_SOURCES,
                 "format": {"type": "string", "enum": list(PODCAST_FORMATS), "description": "deep_dive (default), brief, critique, debate"},
                 "length": {"type": "string", "enum": list(PODCAST_LENGTHS), "description": "short, default, long"}}},
             podcast_from_sources),
    ToolSpec("report_from_sources", "report",
             "Create a NotebookLM written report (briefing doc, study guide, or blog post; Markdown) summarizing the given links and uploaded files. "
             "Set return_text=true to get the Markdown inline from job_status (if under 20 KB). " + _FLOW,
             {"type": "object", "properties": {**_SOURCES,
                 "kind": {"type": "string", "enum": list(REPORT_KINDS), "description": "briefing (default), study_guide, blog"},
                 "return_text": {"type": "boolean", "description": "Include the report Markdown in job_status when done."}}},
             report_from_sources),
    ToolSpec("job_status", None,
             "Check a NotebookLM podcast/report/slides/infographic job: queued, running, done or failed. When done returns title, download_url (curl -o), "
             "bytes, mime, text (reports with return_text) and share_url for files over 45 MB.",
             {"type": "object", "properties": {"job_id": {"type": "string"}}, "required": ["job_id"]}, job_status),
    ToolSpec("quota_status", None,
             "Remaining NotebookLM podcast and report jobs for this agent in the last 24 hours, plus the account-wide limits "
             "(podcast, report, slides, infographic, chat) with their use in the last 24 hours.",
             {"type": "object", "properties": {}}, quota_status),
    ToolSpec("share_link", None,
             "Make an expiring tap-to-download link for a finished NotebookLM job's file (for large files the chat app cannot carry). "
             "Default 24 h, max 168 h. Anyone with the link can download until it expires.",
             {"type": "object", "properties": {"job_id": {"type": "string"}, "ttl_hours": {"type": "integer", "minimum": 1, "maximum": 168}},
              "required": ["job_id"]}, share_link),
    ToolSpec("list_notebooks", "list",
             "List the user's NotebookLM notebooks (titles, source counts, links), newest first. Use it to find the exact "
             "notebook name the user means before passing `notebook` to podcast_from_sources or report_from_sources, "
             "or when the user asks which notebooks they have. Source names and contents are not returned.",
             {"type": "object", "properties": {}}, list_notebooks),
    ToolSpec("slides_from_sources", "studio",
             "Create a NotebookLM slide deck (PDF by default, or PPTX) from the given links and uploaded files. " + _FLOW,
             {"type": "object", "properties": {**_SOURCES,
                 "format": {"type": "string", "enum": list(SLIDE_FORMATS), "description": "pdf (default) or pptx"}}},
             slides_from_sources),
    ToolSpec("infographic_from_sources", "studio",
             "Create a NotebookLM infographic (PNG image) from the given links and uploaded files. " + _FLOW,
             {"type": "object", "properties": {**_SOURCES}}, infographic_from_sources),
    ToolSpec("list_sources", "read",
             "List the sources in one of the user's NotebookLM notebooks (title, kind, processing status; no source text). "
             "`notebook` is the exact notebook title (use list_notebooks to find it); the notebook must already exist.",
             {"type": "object", "properties": {"notebook": _NOTEBOOK_EXISTING}, "required": ["notebook"]}, list_sources),
    ToolSpec("ask_notebook", "read",
             "Ask a question about the sources in one of the user's NotebookLM notebooks; returns NotebookLM's answer with "
             "citations (source title and excerpt). Optionally restrict to some sources by exact title (see list_sources). "
             "Answers within 50 s or fails with generation_timeout. Counts against the account-wide daily chat limit.",
             {"type": "object", "properties": {
                 "notebook": _NOTEBOOK_EXISTING,
                 "question": {"type": "string", "minLength": 1, "maxLength": 2000, "description": "The question, max 2000 characters; newlines and tabs allowed, other control characters refused."},
                 "source_titles": {"type": "array", "items": {"type": "string"},
                                   "description": "Optional exact source titles to limit the answer to; omit to use all sources."}},
              "required": ["notebook", "question"]}, ask_notebook),
    ToolSpec("read_note", "notes",
             "Read the notes in one of the user's NotebookLM notebooks. Without `title`: list the notes (title, length). "
             "With an exact `title`: return that note's text (cut at 20 KB with truncated=true; if several notes share the "
             "title, the most recent is returned with ambiguous=true).",
             {"type": "object", "properties": {"notebook": _NOTEBOOK_EXISTING,
                 "title": {"type": "string", "minLength": 1, "maxLength": LOOKUP_MAX, "description": "Exact note title."}},
              "required": ["notebook"]}, read_note),
    ToolSpec("create_note", "notes",
             "Add a new note to one of the user's existing NotebookLM notebooks. Never edits or deletes notes: a title that "
             "already exists makes a second note with that title.",
             {"type": "object", "properties": {"notebook": _NOTEBOOK_EXISTING,
                 "title": {"type": "string", "minLength": 1, "maxLength": 100, "description": "Note title."},
                 "text": {"type": "string", "minLength": 1, "maxLength": NOTE_TEXT_MAX, "description": "Note text, max 100000 characters; newlines and tabs allowed, other control characters refused."}},
              "required": ["notebook", "title", "text"]}, create_note),
    ToolSpec("get_source_text", "notes",
             "Get the full extracted text of one source in one of the user's NotebookLM notebooks, by exact source title "
             "(see list_sources). Text up to 20 KB is returned inline; longer text is cut (truncated=true) and download_url "
             "(curl with the same bearer token) serves the whole text as text/plain.",
             {"type": "object", "properties": {"notebook": _NOTEBOOK_EXISTING,
                 "source_title": {"type": "string", "minLength": 1, "maxLength": LOOKUP_MAX, "description": "Exact source title."}},
              "required": ["notebook", "source_title"]}, get_source_text),
    ToolSpec("add_sources", "add",
             "Add links (web pages, YouTube, PDFs) and uploaded files as sources to the user's NotebookLM notebook WITHOUT "
             "generating anything: no podcast, report, slides or infographic, and no generation quota is used. Use it to "
             "save, collect, import or file material into a notebook for later reading or questions. The notebook is the one "
             "named in `notebook` (exact title; created if it does not exist), default the user's \"To Review\". Upload files "
             "first with curl -T <file> \"{base}/uploads?filename=<name>\" and pass the returned upload_ids. Returns immediately "
             "with job_id and notebook_url (reply with notebook_url). For many sources check job_status later: it reports "
             "added (count) and failed (indices, urls first then upload_ids). A source that fails does not stop the others, "
             "and sources already added are never removed.",
             {"type": "object", "properties": {"urls": _SOURCES["urls"], "upload_ids": _SOURCES["upload_ids"],
                                               "notebook": _SOURCES["notebook"]}}, add_sources),
]
