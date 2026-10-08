"""Notebooks by exact title: find the operator's notebook with that name, else create it.

The default name is the consumer's notebook_label ("To Review"). When a notebook would
exceed the source cap, the next is "<name> (2)", "<name> (3)"...
"""
from __future__ import annotations

import asyncio
import time
from collections import defaultdict

from .errors import GatewayError


def numbered(name: str, seq: int) -> str:
    return name if seq == 1 else f"{name} ({seq})"


class NamedNotebooks:
    def __init__(self, store, nlm, source_cap: int):
        self.store = store
        self.nlm = nlm
        self.source_cap = source_cap
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def resolve(self, consumer, name: str | None, adding: int) -> tuple[str, str]:
        if adding > self.source_cap:
            raise GatewayError("bad_request", f"at most {self.source_cap} sources per call")
        name = name or consumer.notebook_label
        client = self.nlm.require()
        async with self._locks[name]:
            seq = 1
            while True:
                title = numbered(name, seq)
                row = self.store.get_notebook(title)
                if row is not None and await client.notebooks.get_or_none(row[0]) is None:
                    self.store.forget_notebook(title)  # deleted in the app; re-find or recreate below
                    row = None
                if row is None:
                    nb_id, count = await self._find_or_create(client, title)
                    self.store.put_notebook(title, nb_id, consumer.id, count, time.time())
                    row = (nb_id, count)
                nb_id, count = row
                if count + adding <= self.source_cap:
                    self.store.adjust_sources(title, adding)
                    return nb_id, title
                seq += 1

    async def _find_or_create(self, client, title: str) -> tuple[str, int]:
        for nb in await client.notebooks.list():
            if nb.title == title:
                return nb.id, int(getattr(nb, "sources_count", 0) or 0)
        return (await client.notebooks.create(title)).id, 0

    def release(self, title: str, n: int) -> None:
        self.store.adjust_sources(title, -n)
