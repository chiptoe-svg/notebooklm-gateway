"""Fetch a finished artifact from NotebookLM into a temp file (no long-term host copies)."""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

_EXT = {"podcast": "m4a", "report": "md", "infographic": "png"}
_MIME = {"m4a": "audio/mp4", "md": "text/markdown; charset=utf-8", "png": "image/png", "pdf": "application/pdf",
         "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation"}


def extension(job) -> str:
    """File extension of a job's artifact; slides carry their chosen format (pdf|pptx)."""
    return (job.params.get("format") or "pdf") if job.kind == "slides" else _EXT[job.kind]


def mime_for(job) -> str:
    return _MIME[extension(job)]


_DOWNLOADERS = {"podcast", "report", "slides", "infographic"}


async def fetch_to_temp(client, job, tmp_dir: Path) -> Path:
    if job.kind not in _DOWNLOADERS:
        raise ValueError("unknown job kind")
    tmp_dir = Path(tmp_dir)
    tmp_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(suffix="." + extension(job), dir=tmp_dir)
    os.close(fd)
    artifact = job.artifact_id or job.task_id
    try:
        if job.kind == "podcast":
            await client.artifacts.download_audio(job.notebook_id, name, artifact)
        elif job.kind == "report":
            await client.artifacts.download_report(job.notebook_id, name, artifact)
        elif job.kind == "slides":
            await client.artifacts.download_slide_deck(job.notebook_id, name, artifact, output_format=extension(job))
        elif job.kind == "infographic":
            await client.artifacts.download_infographic(job.notebook_id, name, artifact)
        else:
            raise ValueError("unknown job kind")
    except BaseException:
        Path(name).unlink(missing_ok=True)
        raise
    return Path(name)
