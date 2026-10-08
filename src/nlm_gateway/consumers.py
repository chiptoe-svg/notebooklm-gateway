"""Per-consumer bearer tokens.

Only sha256 hashes are stored. The registry file is re-read on every authenticate()
so pairing and revocation take effect without a restart. Writers serialize on an
exclusive flock over a sibling lock file.
"""
from __future__ import annotations

import base64
import fcntl
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import tempfile
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

SCOPES = frozenset({"podcast", "report", "list", "read", "studio", "notes"})
TOKEN_PREFIX = "nlm_"
_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,40}$")
log = logging.getLogger("nlm_gateway.consumers")


def _check_scopes(scopes) -> frozenset[str]:
    scopes = frozenset(scopes)
    if not scopes or not scopes <= SCOPES:
        raise ValueError(f"scopes must be a non-empty subset of {sorted(SCOPES)}")
    return scopes


def _check_label(label: str) -> str:
    """notebook_label is the consumer's default notebook name (exact NotebookLM title)."""
    label = label.strip()
    if not label or len(label) > 100 or not label.isprintable():
        raise ValueError("notebook_label (default notebook name) must be 1-100 printable characters")
    return label


def new_token() -> str:
    return TOKEN_PREFIX + base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


@dataclass(frozen=True)
class Consumer:
    id: str
    token_hash: str
    scopes: frozenset[str]
    notebook_label: str
    daily_audio_cap: int
    daily_report_cap: int
    note: str = ""
    created_at: str = ""


class Registry:
    def __init__(self, path: Path):
        self.path = Path(path)

    def load(self) -> list[Consumer]:
        if not self.path.exists():
            return []
        raw = json.loads(self.path.read_text())
        return [Consumer(**{**c, "scopes": frozenset(c["scopes"])}) for c in raw.get("consumers", [])]

    @contextmanager
    def _locked(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(self.path.parent, 0o700)
        lock_path = self.path.with_name(self.path.name + ".lock")
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            os.close(fd)  # closing the descriptor releases the flock

    def _save(self, consumers: list[Consumer]) -> None:
        data = {"consumers": [{**asdict(c), "scopes": sorted(c.scopes)} for c in consumers]}
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=self.path.name + ".", suffix=".tmp")
        try:
            try:
                f = os.fdopen(fd, "w")
            except BaseException:
                os.close(fd)  # fdopen did not take ownership; close here
                raise
            with f:
                os.fchmod(f.fileno(), 0o600)
                json.dump(data, f, indent=2)
            os.replace(tmp, self.path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def authenticate(self, token: str | None) -> Consumer | None:
        if not token or not token.startswith(TOKEN_PREFIX):
            return None
        presented = hash_token(token)
        try:
            match = None
            for c in self.load():
                if hmac.compare_digest(c.token_hash, presented):
                    match = c
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            log.error("consumer registry unreadable; refusing all tokens")
            return None
        return match

    def get(self, consumer_id: str) -> Consumer | None:
        try:
            return next((c for c in self.load() if c.id == consumer_id), None)
        except (OSError, ValueError, KeyError, TypeError, AttributeError):  # fail closed, like authenticate()
            log.error("consumer registry unreadable; treating consumer as unknown")
            return None

    def pair(self, id: str, scopes, notebook_label: str, daily_audio_cap: int, daily_report_cap: int, note: str = "") -> str:
        if not _ID_RE.fullmatch(id):
            raise ValueError("id must be lowercase letters, digits, '-' or '_' (max 41 chars)")
        scopes = _check_scopes(scopes)
        notebook_label = _check_label(notebook_label)
        token = new_token()
        with self._locked():
            consumers = self.load()
            if any(c.id == id for c in consumers):
                raise ValueError(f"consumer {id} already exists; revoke it first")
            consumers.append(Consumer(id=id, token_hash=hash_token(token), scopes=scopes, notebook_label=notebook_label,
                                      daily_audio_cap=int(daily_audio_cap), daily_report_cap=int(daily_report_cap), note=note,
                                      created_at=datetime.now(timezone.utc).isoformat(timespec="seconds")))
            self._save(consumers)
        return token

    def update(self, consumer_id: str, scopes=None, notebook_label: str | None = None,
               daily_audio_cap: int | None = None, daily_report_cap: int | None = None) -> bool:
        """Change a consumer's fields in place; the token (hash) is kept."""
        changes: dict = {}
        if scopes is not None:
            changes["scopes"] = _check_scopes(scopes)
        if notebook_label is not None:
            changes["notebook_label"] = _check_label(notebook_label)
        if daily_audio_cap is not None:
            changes["daily_audio_cap"] = int(daily_audio_cap)
        if daily_report_cap is not None:
            changes["daily_report_cap"] = int(daily_report_cap)
        with self._locked():
            consumers = self.load()
            if not any(c.id == consumer_id for c in consumers):
                return False
            self._save([replace(c, **changes) if c.id == consumer_id else c for c in consumers])
        return True

    def revoke(self, consumer_id: str) -> bool:
        with self._locked():
            consumers = self.load()
            kept = [c for c in consumers if c.id != consumer_id]
            if len(kept) == len(consumers):
                return False
            self._save(kept)
        return True
