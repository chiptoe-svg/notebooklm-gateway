"""Append-only JSONL audit. Callers pass counts and digest()s, never URLs or filenames."""
from __future__ import annotations

import hashlib
import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:16]


class Audit:
    def __init__(self, path: Path):
        self.path = Path(path)
        self._lock = threading.Lock()

    def write(self, event: str, **fields) -> None:
        line = json.dumps({"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"), "event": event, **fields},
                          separators=(",", ":"))
        with self._lock:
            fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            with os.fdopen(fd, "a") as f:
                f.write(line + "\n")
