"""PUT /notebooklm/uploads: stream to state/uploads/<upload_id>, validate type by extension + magic bytes.

The caller's filename is only ever used as the NotebookLM source title, never on disk.
"""
from __future__ import annotations

import os
import uuid

from .errors import GatewayError, TooManyUploads, UploadTooLarge

_ZIP = (b"PK\x03\x04",)
_OLE = (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1",)
_MAGIC = {
    ".pdf": (b"%PDF-",), ".docx": _ZIP, ".pptx": _ZIP, ".odt": _ZIP, ".epub": _ZIP, ".doc": _OLE,
    ".rtf": (b"{\\rtf",), ".mp3": (b"ID3", b"\xff\xfb", b"\xff\xf3", b"\xff\xf2"), ".wav": (b"RIFF",),
    ".png": (b"\x89PNG\r\n\x1a\n",), ".jpg": (b"\xff\xd8\xff",), ".jpeg": (b"\xff\xd8\xff",),
}
_TEXT = frozenset({".txt", ".md", ".csv", ".tsv"})
_FTYP = frozenset({".m4a", ".mp4"})
ALLOWED_EXTS = frozenset(_MAGIC) | _TEXT | _FTYP
_HEAD = 4096
MAX_PENDING = 10  # unclaimed, unexpired uploads per consumer


def safe_filename(raw: str) -> str:
    name = raw.replace("\\", "/").split("/")[-1]
    name = "".join(ch for ch in name if ch.isprintable()).strip().lstrip(".")
    if not name or "." not in name:
        raise GatewayError("bad_request", "a filename with an extension is required (?filename=...)")
    stem, ext = name.rsplit(".", 1)
    if not ext:
        raise GatewayError("bad_request", "a filename with an extension is required (?filename=...)")
    return (stem[:100] or "file") + "." + ext[:10]


def sniff_ok(ext: str, head: bytes) -> bool:
    if ext in _TEXT:
        if b"\x00" in head:
            return False
        try:
            head.decode("utf-8")
        except UnicodeDecodeError as e:
            return e.start >= len(head) - 3  # a multibyte char cut at the head boundary
        return True
    if ext in _FTYP:
        return head[4:8] == b"ftyp"
    return any(head.startswith(m) for m in _MAGIC.get(ext, ()))


async def receive_upload(request, cfg, store, consumer, now):
    name = safe_filename(request.query_params.get("filename", ""))
    ext = "." + name.rsplit(".", 1)[1].lower()
    if ext not in ALLOWED_EXTS:
        raise GatewayError("bad_request", f"file type not accepted; allowed: {', '.join(sorted(ALLOWED_EXTS))}")
    if store.count_pending_uploads(consumer.id, now) >= MAX_PENDING:
        raise TooManyUploads(MAX_PENDING)
    up_dir = cfg.state_dir / "uploads"
    up_dir.mkdir(mode=0o700, exist_ok=True)
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > cfg.upload_max_bytes:
        raise UploadTooLarge()
    upload_id = "up_" + uuid.uuid4().hex
    path = up_dir / upload_id
    size, head = 0, b""
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as f:
            async for chunk in request.stream():
                size += len(chunk)
                if size > cfg.upload_max_bytes:
                    raise UploadTooLarge()
                if len(head) < _HEAD:
                    head += chunk[: _HEAD - len(head)]
                f.write(chunk)
        if size == 0:
            raise GatewayError("bad_request", "empty upload")
        if not sniff_ok(ext, head):
            raise GatewayError("bad_request", f"file content does not look like {ext}")
        if store.count_pending_uploads(consumer.id, now) >= MAX_PENDING:  # re-check: no await from here to the insert
            raise TooManyUploads(MAX_PENDING)
        return store.add_upload(consumer.id, name, path, size, now, cfg.upload_ttl_s, upload_id=upload_id)
    except BaseException:
        path.unlink(missing_ok=True)
        raise
