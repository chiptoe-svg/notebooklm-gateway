"""Wires the components together; used by main and by tests."""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .audit import Audit
from .config import Config
from .consumers import Registry
from .jobs import JobRunner
from .links import load_or_create_key
from .store import Store
from .notebooks import NamedNotebooks


@dataclass
class Gateway:
    cfg: Config
    store: Store
    registry: Registry
    nlm: Any
    notebooks: NamedNotebooks
    runner: JobRunner
    audit: Audit
    link_key: bytes
    now: Callable[[], float] = field(default=time.time)


def build_gateway(cfg: Config, nlm, registry: Registry | None = None) -> Gateway:
    cfg.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(cfg.state_dir, 0o700)
    store = Store(cfg.state_dir / "gateway.db")
    audit = Audit(cfg.state_dir / "audit.jsonl")
    notebooks = NamedNotebooks(store, nlm, cfg.notebook_source_cap)
    runner = JobRunner(cfg, store, nlm, notebooks, audit)
    return Gateway(cfg=cfg, store=store, registry=registry or Registry(cfg.state_dir / "consumers.json"), nlm=nlm,
                   notebooks=notebooks, runner=runner, audit=audit, link_key=load_or_create_key(cfg.state_dir / "link.key"))
