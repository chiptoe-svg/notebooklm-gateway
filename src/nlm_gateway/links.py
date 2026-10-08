"""HMAC-signed, expiring download links: /notebooklm/dl/<job_id>.<exp>.<mac>/<filename>."""
from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import secrets
import tempfile
from pathlib import Path
from typing import Callable

from .fetch import extension


def _read_key(path: Path) -> bytes:
    key = path.read_bytes()
    if len(key) != 32:
        raise RuntimeError("link key file is corrupt; delete it to regenerate")
    return key


def load_or_create_key(path: Path) -> bytes:
    """Return the 32-byte link key, creating it atomically if absent.

    The key is written to a 0600 temp file in the same directory and published with
    os.link, so the key path never exists with partial content. A lost creation race
    falls back to reading the winner's key.
    """
    path = Path(path)
    try:
        return _read_key(path)
    except FileNotFoundError:
        pass
    key = secrets.token_bytes(32)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            os.fchmod(f.fileno(), 0o600)
            f.write(key)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.link(tmp, path)
        except FileExistsError:
            return _read_key(path)
        return key
    finally:
        os.unlink(tmp)


def download_filename(job) -> str:
    return f"{job.kind}-{job.id[:8]}.{extension(job)}"


def _mac(key: bytes, job_id: str, consumer_id: str, exp: int, filename: str) -> str:
    msg = f"{job_id}|{consumer_id}|{exp}|{filename}".encode()
    return hmac.new(key, msg, hashlib.sha256).hexdigest()[:32]


def make_token(key: bytes, job_id: str, consumer_id: str, exp: int, filename: str) -> str:
    return f"{job_id}.{exp}.{_mac(key, job_id, consumer_id, exp, filename)}"


def verify_token(key: bytes, token: str, filename: str, consumer_for_job: Callable[[str], str | None], now: float) -> str | None:
    # Public, unauthenticated input: any malformed token must return None, never raise.
    if not (token.isascii() and filename.isascii()):
        return None
    parts = token.split(".")
    if len(parts) != 3 or not parts[1].isascii() or not parts[1].isdigit():
        return None
    job_id, exp_s, mac = parts
    if len(exp_s) > 12:
        return None
    try:
        exp = int(exp_s)
        if exp < now:
            return None
        consumer_id = consumer_for_job(job_id)
        if consumer_id is None:
            return None
        expected = _mac(key, job_id, consumer_id, exp, filename)
        return job_id if hmac.compare_digest(expected.encode(), mac.encode()) else None
    except (ValueError, TypeError):
        return None


def share_url(cfg, key: bytes, job, ttl_h: int, now: float) -> tuple[str, int]:
    exp = int(now) + ttl_h * 3600
    fn = download_filename(job)
    return f"{cfg.public_base_url}/dl/{make_token(key, job.id, job.consumer_id, exp, fn)}/{fn}", exp


# -- opaque source-text references: /notebooklm/sources/text/<payload>.<exp>.<mac> ----------
# The MAC message starts with a domain tag that /dl/ tokens never carry, so the two cannot be swapped.
SOURCE_REF_TTL_S = 3600
_SOURCE_DOMAIN = b"nlm-source-text\x00"


def _source_mac(key: bytes, payload: str, consumer_id: str, exp: int) -> str:
    msg = _SOURCE_DOMAIN + json.dumps([payload, consumer_id, exp], separators=(",", ":")).encode()
    return hmac.new(key, msg, hashlib.sha256).hexdigest()[:32]


def make_source_ref(key: bytes, notebook_id: str, source_id: str, consumer_id: str, exp: int) -> str:
    payload = base64.urlsafe_b64encode(json.dumps([notebook_id, source_id]).encode()).rstrip(b"=").decode()
    return f"{payload}.{exp}.{_source_mac(key, payload, consumer_id, exp)}"


def verify_source_ref(key: bytes, ref: str, consumer_id: str, now: float) -> tuple[str, str] | None:
    """(notebook_id, source_id) if ref was minted for this consumer and is unexpired; never raises."""
    try:
        if not ref.isascii():
            return None
        parts = ref.split(".")
        if len(parts) != 3 or not parts[1].isdigit() or len(parts[1]) > 12:
            return None
        payload, exp_s, mac = parts
        exp = int(exp_s)
        if exp < now or not hmac.compare_digest(_source_mac(key, payload, consumer_id, exp).encode(), mac.encode()):
            return None
        ids = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        if not (isinstance(ids, list) and len(ids) == 2 and all(isinstance(x, str) and x for x in ids)):
            return None
        return ids[0], ids[1]
    except (ValueError, TypeError, binascii.Error):
        return None
