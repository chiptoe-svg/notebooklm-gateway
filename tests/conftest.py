from __future__ import annotations

import pytest

from nlm_gateway.config import Config
from nlm_gateway.consumers import Consumer


@pytest.fixture
def cfg(tmp_path):
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    return Config(host="127.0.0.1", port=0, public_base_url="https://gw.example/notebooklm", state_dir=state,
                  timezone="America/New_York", account_audio_daily=6, account_audio_reserve=2, notebook_source_cap=50)


@pytest.fixture
def make_consumer():
    def _make(id="research", scopes=("podcast", "report"), label=None, audio=3, report=10):
        return Consumer(id=id, token_hash="unused", scopes=frozenset(scopes), notebook_label=label or id,
                        daily_audio_cap=audio, daily_report_cap=report)
    return _make


from nlm_gateway.store import Store


@pytest.fixture
def store(cfg):
    return Store(cfg.state_dir / "gateway.db")


from nlm_gateway.gateway import build_gateway
from tests.fakes import FakeNLM


@pytest.fixture
def gw(cfg):
    return build_gateway(cfg, FakeNLM())


@pytest.fixture
def queue_job(gw):
    """Create a queued job the way the tools layer will (Task 10), then claim it."""
    async def _q(consumer, urls=("https://a.example/1",), upload_ids=(), kind="podcast", params=None):
        params = params or ({"format": "brief", "length": "short", "language": "en", "instructions": None}
                            if kind == "podcast" else {"kind": "briefing", "language": "en", "instructions": None, "return_text": False})
        now = gw.now()
        job = gw.store.create_job(consumer.id, kind, params, list(urls), list(upload_ids), now)
        gw.store.claim_uploads(list(upload_ids), consumer.id, job.id, now)
        nb, title = await gw.notebooks.resolve(consumer, None, len(urls) + len(upload_ids))
        gw.store.update_job(job.id, now, status="queued", notebook_id=nb, notebook_title=title, notebook_url="u")
        return gw.store.claim_next(now)
    return _q
