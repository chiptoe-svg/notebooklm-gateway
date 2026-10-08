from types import SimpleNamespace

import pytest

from nlm_gateway.fetch import fetch_to_temp, mime_for
from tests.fakes import FakeClient


async def test_fetch_audio_and_report(tmp_path):
    c = FakeClient()
    j = SimpleNamespace(kind="podcast", notebook_id="nb1", artifact_id="t9", task_id="t9")
    p = await fetch_to_temp(c, j, tmp_path / "tmp")
    assert p.read_bytes() == c.audio_bytes and p.suffix == ".m4a" and c.downloads == ["t9"]
    r = await fetch_to_temp(c, SimpleNamespace(kind="report", notebook_id="nb1", artifact_id=None, task_id="t8"), tmp_path / "tmp")
    assert r.read_text() == c.report_text and c.downloads[-1] == "t8"
    assert mime_for(j) == "audio/mp4"


async def test_fetch_studio_kinds(tmp_path):
    c = FakeClient()
    for fmt in ("pdf", "pptx"):
        j = SimpleNamespace(kind="slides", params={"format": fmt}, notebook_id="nb1", artifact_id="s1", task_id="s1")
        p = await fetch_to_temp(c, j, tmp_path / "tmp")
        assert p.read_bytes() == c.slides_bytes[fmt] and p.suffix == "." + fmt
    j = SimpleNamespace(kind="infographic", params={}, notebook_id="nb1", artifact_id="i1", task_id="i1")
    p = await fetch_to_temp(c, j, tmp_path / "tmp")
    assert p.read_bytes() == c.infographic_bytes and p.suffix == ".png" and mime_for(j) == "image/png"
    assert c.slide_formats == ["pdf", "pptx"]


async def test_fetch_cleans_up_on_error(tmp_path):
    c = FakeClient()

    async def boom(*a, **k):
        raise RuntimeError("x")
    c.artifacts.download_audio = boom
    with pytest.raises(RuntimeError):
        await fetch_to_temp(c, SimpleNamespace(kind="podcast", notebook_id="n", artifact_id="a", task_id="a"), tmp_path)
    assert list(tmp_path.iterdir()) == []


async def test_fetch_unknown_kind_raises(tmp_path):
    c = FakeClient()
    with pytest.raises(ValueError):
        await fetch_to_temp(c, SimpleNamespace(kind="video", params={}, notebook_id="n", artifact_id="a", task_id="a"), tmp_path)
    assert c.downloads == [] and list(tmp_path.iterdir()) == []
