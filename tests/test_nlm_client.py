import pytest

from nlm_gateway.errors import GatewayError
from nlm_gateway.nlm_client import NLM
from tests.fakes import FakeClient


async def test_require_before_start_is_auth_expired():
    with pytest.raises(GatewayError) as e:
        NLM().require()
    assert e.value.code == "auth_expired"


async def test_health_check_marks_ok_and_failed():
    n = NLM()
    n.client = FakeClient()
    assert await n.health_check() is True and n.require() is n.client

    async def boom():
        raise RuntimeError("401")
    n.client.notebooks.list = boom
    assert await n.health_check() is False and n.last_error == "upstream_changed"
    with pytest.raises(GatewayError):
        n.require()


async def test_start_without_credentials_does_not_raise(tmp_path, monkeypatch):
    monkeypatch.setenv("NOTEBOOKLM_HOME", str(tmp_path))  # empty home: no storage_state
    n = NLM(profile="does-not-exist")
    await n.start()
    assert n.auth_ok is False and n.client is None
    await n.stop()


class _Ctx:
    def __init__(self, fail_exit=False):
        self.entered = 0
        self.exited = 0
        self.fail_exit = fail_exit

    async def __aenter__(self):
        import asyncio
        self.entered += 1
        await asyncio.sleep(0.01)
        return FakeClient()

    async def __aexit__(self, *a):
        self.exited += 1
        if self.fail_exit:
            raise RuntimeError("close failed")


async def test_concurrent_start_enters_once(monkeypatch):
    import asyncio

    import notebooklm
    ctx = _Ctx()
    monkeypatch.setattr(notebooklm.NotebookLMClient, "from_storage", staticmethod(lambda **k: ctx))
    n = NLM()
    await asyncio.gather(n.start(), n.start(), n.health_check())
    assert ctx.entered == 1 and n.auth_ok is True
    await n.stop()
    assert ctx.exited == 1


async def test_stop_resets_state_even_if_exit_raises(monkeypatch):
    import notebooklm
    ctx = _Ctx(fail_exit=True)
    monkeypatch.setattr(notebooklm.NotebookLMClient, "from_storage", staticmethod(lambda **k: ctx))
    n = NLM()
    await n.start()
    assert n.auth_ok is True
    with pytest.raises(RuntimeError):
        await n.stop()
    assert n.client is None and n._ctx is None and n.auth_ok is False
