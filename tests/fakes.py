"""In-memory stand-in for notebooklm.NotebookLMClient (only the methods the gateway uses)."""
from __future__ import annotations

import asyncio
import itertools
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from notebooklm import SourceNotFoundError
from notebooklm.types import AskResult, ChatReference, Note, Source, SourceFulltext, SourceStatus

from nlm_gateway.errors import GatewayError

_ids = itertools.count(1)


def _id(prefix: str) -> str:
    return f"{prefix}{next(_ids)}"


@dataclass
class FakeNotebook:
    id: str
    title: str
    sources_count: int = 0
    created_at: datetime | None = None


@dataclass
class FakeSource:
    id: str
    title: str
    type_code: int = 5  # web_page in notebooklm's source type code map
    status: SourceStatus = SourceStatus.READY
    text: str = ""


@dataclass
class FakeStatus:
    task_id: str
    status: str = "in_progress"
    error: str | None = None
    error_code: str | None = None

    @property
    def is_complete(self) -> bool:
        return self.status == "completed"

    @property
    def is_failed(self) -> bool:
        return self.status == "failed"


class _Notebooks:
    def __init__(self, c):
        self.c = c

    async def list(self):
        await asyncio.sleep(0)  # real client does network I/O; yield to the loop
        return [FakeNotebook(n.id, n.title, len(self.c.source_data.get(n.id, [])), n.created_at) for n in self.c.notebooks_by_id.values()]

    async def create(self, title):
        await asyncio.sleep(0)
        nb = FakeNotebook(_id("nb"), title, created_at=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=len(self.c.creates)))
        self.c.notebooks_by_id[nb.id] = nb
        self.c.creates.append(title)
        return nb

    async def get_or_none(self, notebook_id):
        return self.c.notebooks_by_id.get(notebook_id)

    async def delete(self, notebook_id):
        self.c.notebooks_by_id.pop(notebook_id, None)

    def get_share_url(self, notebook_id, artifact_id=None):
        return f"https://notebooklm.example/notebook/{notebook_id}"


class _Sources:
    def __init__(self, c):
        self.c = c

    async def add_url(self, notebook_id, url, *, wait=False, wait_timeout=120.0, title=None):
        if url in self.c.reject_urls:
            raise RuntimeError(f"could not fetch {url}")
        return self.c._add(notebook_id, url)

    async def add_file(self, notebook_id, file_path, mime_type=None, *, wait=False, wait_timeout=120.0, title=None, on_progress=None):
        assert Path(file_path).exists(), "upload file must still exist when added"
        if title in self.c.reject_files:
            raise RuntimeError("bad file")
        return self.c._add(notebook_id, title or Path(file_path).name)

    async def get(self, notebook_id, source_id):
        if self.c.source_get_error is not None:
            raise self.c.source_get_error
        for src in self.c.source_data.get(notebook_id, []):
            if src.id == source_id:
                return src
        raise RuntimeError("source not found")

    async def delete(self, notebook_id, source_id):
        self.c.deleted_sources.append(source_id)
        self.c.source_data[notebook_id] = [s for s in self.c.source_data.get(notebook_id, []) if s.id != source_id]

    async def list(self, notebook_id, **kw):
        self.c.source_lists.append(notebook_id)
        return [Source(id=s.id, title=s.title, _type_code=s.type_code, status=s.status)
                for s in self.c.source_data.get(notebook_id, [])]

    async def get_fulltext(self, notebook_id, source_id, *, output_format="text"):
        assert output_format in ("text", "markdown")
        self.c.fulltext_calls.append((source_id, output_format))
        for s in self.c.source_data.get(notebook_id, []):
            if s.id == source_id:
                return SourceFulltext(source_id=s.id, title=s.title, content=s.text, char_count=len(s.text))
        raise SourceNotFoundError(source_id)


class _Notes:
    def __init__(self, c):
        self.c = c

    async def list(self, notebook_id):
        return list(self.c.notes_data.get(notebook_id, []))

    async def get(self, notebook_id, note_id):
        for n in self.c.notes_data.get(notebook_id, []):
            if n.id == note_id:
                return n
        raise RuntimeError("note not found")

    async def create(self, notebook_id, title="New Note", content=""):
        if notebook_id not in self.c.notebooks_by_id:
            raise RuntimeError("notebook not found")
        n = Note(id=_id("note"), notebook_id=notebook_id, title=title, content=content,
                 created_at=datetime(2026, 2, 1, tzinfo=timezone.utc) + timedelta(minutes=next(_ids)))
        self.c.notes_data.setdefault(notebook_id, []).append(n)
        self.c.note_creates.append((notebook_id, title, content))
        return n

    async def update(self, *a, **kw):
        self.c.note_edits.append(("update", a, kw))

    async def delete(self, *a, **kw):
        self.c.note_edits.append(("delete", a, kw))


class _Chat:
    def __init__(self, c):
        self.c = c

    async def ask(self, notebook_id, question, source_ids=None, conversation_id=None):
        self.c.asks.append((notebook_id, question, source_ids))
        if self.c.ask_delay:
            await asyncio.sleep(self.c.ask_delay)
        refs = [ChatReference(source_id=sid, citation_number=i + 1, cited_text=text)
                for i, (sid, text) in enumerate(self.c.ask_refs)]
        return AskResult(answer=self.c.ask_answer, conversation_id="conv1", turn_number=1, is_follow_up=False, references=refs)


class _Artifacts:
    def __init__(self, c):
        self.c = c

    async def generate_audio(self, notebook_id, **kw):
        self.c.generated.append(("audio", notebook_id, kw))
        return self.c._kickoff()

    async def generate_report(self, notebook_id, **kw):
        self.c.generated.append(("report", notebook_id, kw))
        return self.c._kickoff()

    async def generate_slide_deck(self, notebook_id, source_ids=None, language="en", instructions=None,
                                  slide_format=None, slide_length=None):
        self.c.generated.append(("slides", notebook_id, {"source_ids": source_ids, "language": language, "instructions": instructions}))
        return self.c._kickoff()

    async def generate_infographic(self, notebook_id, source_ids=None, language="en", instructions=None,
                                   orientation=None, detail_level=None, style=None):
        self.c.generated.append(("infographic", notebook_id, {"source_ids": source_ids, "language": language, "instructions": instructions}))
        return self.c._kickoff()

    async def wait_for_completion(self, notebook_id, task_id, timeout=300.0, **kw):
        if self.c.hang:
            raise TimeoutError("fake timeout")
        if self.c.fail_generation:
            return FakeStatus(task_id, "failed", error="generation failed")
        return FakeStatus(task_id, "completed")

    async def rename(self, notebook_id, artifact_id, new_title, *, return_object=True):
        self.c.renames.append((notebook_id, artifact_id, new_title, return_object))
        if self.c.rename_error is not None:
            raise self.c.rename_error
        return None

    async def download_audio(self, notebook_id, output_path, artifact_id=None):
        self.c.downloads.append(artifact_id)
        Path(output_path).write_bytes(self.c.audio_bytes)
        return str(output_path)

    async def download_report(self, notebook_id, output_path, artifact_id=None):
        self.c.downloads.append(artifact_id)
        Path(output_path).write_text(self.c.report_text)
        return str(output_path)


    async def download_slide_deck(self, notebook_id, output_path, artifact_id=None, output_format="pdf", *, artifacts_data=None):
        if output_format not in ("pdf", "pptx"):
            raise ValueError("bad format")
        self.c.downloads.append(artifact_id)
        self.c.slide_formats.append(output_format)
        Path(output_path).write_bytes(self.c.slides_bytes[output_format])
        return str(output_path)

    async def download_infographic(self, notebook_id, output_path, artifact_id=None, *, artifacts_data=None):
        self.c.downloads.append(artifact_id)
        Path(output_path).write_bytes(self.c.infographic_bytes)
        return str(output_path)


class FakeClient:
    def __init__(self):
        self.notebooks_by_id: dict[str, FakeNotebook] = {}
        self.source_data: dict[str, list[FakeSource]] = {}
        self.creates: list[str] = []
        self.deleted_sources: list[str] = []
        self.generated: list[tuple] = []
        self.downloads: list[str | None] = []
        self.renames: list[tuple] = []
        self.rename_error: BaseException | None = None
        self.source_get_error: BaseException | None = None
        self.notes_data: dict[str, list[Note]] = {}
        self.note_creates: list[tuple] = []
        self.note_edits: list[tuple] = []
        self.source_lists: list[str] = []
        self.fulltext_calls: list[tuple] = []
        self.asks: list[tuple] = []
        self.ask_delay = 0.0
        self.ask_answer = "The answer [1]."
        self.ask_refs: list[tuple[str, str | None]] = []
        self.reject_urls: set[str] = set()
        self.reject_files: set[str] = set()
        self.refuse_generation = False
        self.fail_generation = False
        self.hang = False
        self.audio_bytes = b"\x00\x00\x00\x18ftypM4A " + b"\x00" * 1000
        self.report_text = "# Briefing\n\nHello."
        self.slides_bytes = {"pdf": b"%PDF-1.7 slides", "pptx": b"PK\x03\x04 pptx-slides"}
        self.slide_formats: list[str] = []
        self.infographic_bytes = b"\x89PNG\r\n\x1a\n infographic"
        self.notebooks = _Notebooks(self)
        self.sources = _Sources(self)
        self.artifacts = _Artifacts(self)
        self.chat = _Chat(self)
        self.notes = _Notes(self)

    def _add(self, notebook_id, title):
        if notebook_id not in self.notebooks_by_id:
            raise RuntimeError("notebook not found")
        src = FakeSource(_id("src"), title)
        self.source_data.setdefault(notebook_id, []).append(src)
        return src

    def _kickoff(self):
        if self.refuse_generation:
            return FakeStatus(_id("t"), "failed", error="quota", error_code="USER_DISPLAYABLE_ERROR")
        return FakeStatus(_id("t"), "in_progress")


class FakeNLM:
    def __init__(self, client: FakeClient | None = None):
        self.client = client or FakeClient()
        self.auth_ok = True
        self.last_error = None

    async def start(self):
        pass

    async def stop(self):
        pass

    async def health_check(self):
        return self.auth_ok

    def require(self):
        if not self.auth_ok:
            raise GatewayError("auth_expired", "Google session expired")
        return self.client
