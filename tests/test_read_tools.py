"""list_sources / ask_notebook (scope read)."""
import asyncio
import json
import logging
import time
from dataclasses import replace

import pytest
from notebooklm.types import SourceStatus

from nlm_gateway import tools
from nlm_gateway.errors import GatewayError

SECRET = "zq-secret-zq"
READ = ("read",)


async def notebook_with_sources(gw, title="Lighthouses"):
    cl = gw.nlm.client
    nb = await cl.notebooks.create(title)
    a = cl._add(nb.id, "Fresnel lens")
    b = cl._add(nb.id, "paper.pdf")
    b.type_code, b.status, b.text = 3, SourceStatus.PROCESSING, "full text " + SECRET
    cl.creates.clear()
    return nb, a, b


def audit_text(gw):
    p = gw.cfg.state_dir / "audit.jsonl"
    return p.read_text() if p.exists() else ""


async def test_list_sources(gw, make_consumer):
    nb, a, b = await notebook_with_sources(gw)
    out = await tools.list_sources(gw, make_consumer(scopes=READ), {"notebook": " Lighthouses "})
    assert out == {"notebook": "Lighthouses", "sources": [
        {"title": "Fresnel lens", "kind": "web_page", "status": "ready"},
        {"title": "paper.pdf", "kind": "pdf", "status": "processing"}]}
    assert SECRET not in json.dumps(out)
    last = json.loads(audit_text(gw).splitlines()[-1])
    assert last == {"ts": last["ts"], "event": "list_sources", "consumer": "research", "notebook_id": nb.id, "count": 2}


@pytest.mark.parametrize("tool,args", [
    ("list_sources", {"notebook": "Nope"}),
    ("ask_notebook", {"notebook": "Nope", "question": "why?"}),
])
async def test_read_tools_never_create_notebooks(gw, make_consumer, tool, args):
    await notebook_with_sources(gw)
    with pytest.raises(GatewayError) as e:
        await getattr(tools, tool)(gw, make_consumer(scopes=READ), args)
    assert e.value.code == "not_found" and e.value.message == "no notebook with that title"
    assert gw.nlm.client.creates == [] and gw.store.get_notebook("Nope") is None
    assert gw.store.count_events("chat", 0) == 0 and gw.nlm.client.asks == []


@pytest.mark.parametrize("tool,args", [
    ("list_sources", {"notebook": "Lighthouses"}),
    ("ask_notebook", {"notebook": "Lighthouses", "question": "why?"}),
])
async def test_read_tools_need_read_scope(gw, make_consumer, tool, args):
    await notebook_with_sources(gw)
    with pytest.raises(GatewayError) as e:
        await getattr(tools, tool)(gw, make_consumer(scopes=("podcast", "report", "list", "studio", "notes")), args)
    assert e.value.code == "not_found" and e.value.message == "unknown tool"
    assert gw.nlm.client.source_lists == [] and gw.nlm.client.asks == []


@pytest.mark.parametrize("tool,extra", [("list_sources", {}), ("ask_notebook", {"question": "why?"})])
async def test_read_tools_require_notebook(gw, make_consumer, tool, extra):
    for bad in (None, "", SECRET + "x" * 100, 5):
        args = {**extra} if bad is None else {**extra, "notebook": bad}
        with pytest.raises(GatewayError) as e:
            await getattr(tools, tool)(gw, make_consumer(scopes=READ), args)
        assert e.value.code == "bad_request" and SECRET not in e.value.message


async def test_list_sources_bounded(gw, make_consumer, monkeypatch):
    async def stuck():
        await asyncio.Event().wait()
    gw.nlm.client.notebooks.list = stuck
    monkeypatch.setattr(tools, "READ_TIMEOUT_S", 0.05)
    with pytest.raises(GatewayError) as e:
        await tools.list_sources(gw, make_consumer(scopes=READ), {"notebook": "A"})
    assert e.value.code == "generation_timeout"


async def test_ask_notebook(gw, make_consumer):
    nb, a, b = await notebook_with_sources(gw)
    cl = gw.nlm.client
    cl.ask_refs = [(a.id, "lens excerpt"), (b.id, None), (a.id, "lens excerpt"), ("gone-id", "x")]
    out = await tools.ask_notebook(gw, make_consumer(scopes=READ), {"notebook": "Lighthouses", "question": "How do lenses work?"})
    assert out == {"answer": "The answer [1].", "citations": [
        {"source_title": "Fresnel lens", "excerpt": "lens excerpt"},
        {"source_title": "paper.pdf"},
        {"source_title": None, "excerpt": "x"}]}
    assert cl.asks == [(nb.id, "How do lenses work?", None)]
    assert gw.store.count_events("chat", 0, "research") == 1
    last = json.loads(audit_text(gw).splitlines()[-1])
    assert last == {"ts": last["ts"], "event": "ask", "consumer": "research", "notebook_id": nb.id, "question_chars": 19}


async def test_ask_source_titles_mapping(gw, make_consumer):
    nb, a, b = await notebook_with_sources(gw)
    c = make_consumer(scopes=READ)
    await tools.ask_notebook(gw, c, {"notebook": "Lighthouses", "question": "q", "source_titles": ["paper.pdf"]})
    assert gw.nlm.client.asks[-1][2] == [b.id]
    with pytest.raises(GatewayError) as e:
        await tools.ask_notebook(gw, c, {"notebook": "Lighthouses", "question": "q", "source_titles": ["paper.pdf", SECRET]})
    assert e.value.code == "bad_request" and "source_titles[1]" in e.value.message and SECRET not in e.value.message
    for bad in ("paper.pdf", [5], [""]):
        with pytest.raises(GatewayError) as e:
            await tools.ask_notebook(gw, c, {"notebook": "Lighthouses", "question": "q", "source_titles": bad})
        assert e.value.code == "bad_request"
    assert len(gw.nlm.client.asks) == 1 and gw.store.count_events("chat", 0) == 1


@pytest.mark.parametrize("q", ["", "   ", "x" * 2001, 5, None, SECRET + "\x07"])
async def test_ask_question_validated(gw, make_consumer, q):
    await notebook_with_sources(gw)
    args = {"notebook": "Lighthouses"} if q is None else {"notebook": "Lighthouses", "question": q}
    with pytest.raises(GatewayError) as e:
        await tools.ask_notebook(gw, make_consumer(scopes=READ), args)
    assert e.value.code == "bad_request" and SECRET not in e.value.message and gw.nlm.client.asks == []


async def test_ask_timeout(gw, make_consumer, monkeypatch):
    await notebook_with_sources(gw)
    gw.nlm.client.ask_delay = 5
    monkeypatch.setattr(tools, "ASK_TIMEOUT_S", 0.1)
    t0 = time.monotonic()
    with pytest.raises(GatewayError) as e:
        await tools.ask_notebook(gw, make_consumer(scopes=READ), {"notebook": "Lighthouses", "question": "q"})
    assert time.monotonic() - t0 < 1
    assert e.value.code == "generation_timeout"
    assert e.value.message == "NotebookLM did not answer within 50 s; try again or narrow the question."
    assert gw.store.count_events("chat", 0) == 0


def test_ask_bound_under_60s():
    assert tools.ASK_TIMEOUT_S == 50 and tools.READ_TIMEOUT_S <= 50


async def test_chat_quota_exceeded(gw, make_consumer):
    await notebook_with_sources(gw)
    gw.cfg = replace(gw.cfg, account_chat_daily=1)
    await tools.ask_notebook(gw, make_consumer(scopes=READ), {"notebook": "Lighthouses", "question": "q"})
    with pytest.raises(GatewayError) as e:
        await tools.ask_notebook(gw, make_consumer(id="other", scopes=READ), {"notebook": "Lighthouses", "question": "q"})
    assert e.value.code == "quota_exceeded" and len(gw.nlm.client.asks) == 1


async def test_ask_auth_expired(gw, make_consumer):
    gw.nlm.auth_ok = False
    with pytest.raises(GatewayError) as e:
        await tools.ask_notebook(gw, make_consumer(scopes=READ), {"notebook": "Lighthouses", "question": "q"})
    assert e.value.code == "auth_expired"


async def test_read_tools_leak_nothing(gw, make_consumer, caplog):
    nb, a, b = await notebook_with_sources(gw, title="Nb " + SECRET)
    a.title = "Src " + SECRET
    gw.nlm.client.ask_answer = "Answer " + SECRET
    gw.nlm.client.ask_refs = [(a.id, "Excerpt " + SECRET)]
    c = make_consumer(scopes=READ)
    with caplog.at_level(logging.DEBUG):
        await tools.list_sources(gw, c, {"notebook": "Nb " + SECRET})
        await tools.ask_notebook(gw, c, {"notebook": "Nb " + SECRET, "question": "Q " + SECRET,
                                         "source_titles": ["Src " + SECRET]})
    assert SECRET not in caplog.text and SECRET not in audit_text(gw)


def test_read_tool_specs():
    specs = {t.name: t for t in tools.TOOLS}
    assert specs["list_sources"].scope == "read" and specs["ask_notebook"].scope == "read"
    assert specs["list_sources"].input_schema["required"] == ["notebook"]
    assert specs["ask_notebook"].input_schema["required"] == ["notebook", "question"]
    assert "source_titles" in specs["ask_notebook"].input_schema["properties"]


async def test_ask_multiline_question_accepted(gw, make_consumer):
    await notebook_with_sources(gw)
    await tools.ask_notebook(gw, make_consumer(scopes=READ), {"notebook": "Lighthouses", "question": "Line one\n\tline two?"})
    assert gw.nlm.client.asks[-1][1] == "Line one\n\tline two?"


@pytest.mark.parametrize("bad", ["\x00", "\x07", "\x1b", "\x7f", "\u202e", "\u202a", "\u2067", "\u2069"])
async def test_ask_control_chars_rejected(gw, make_consumer, bad):
    await notebook_with_sources(gw)
    with pytest.raises(GatewayError) as e:
        await tools.ask_notebook(gw, make_consumer(scopes=READ), {"notebook": "Lighthouses", "question": f"a{bad}b {SECRET}"})
    assert e.value.code == "bad_request" and SECRET not in e.value.message and gw.nlm.client.asks == []


PASTED = ["a\u00a0b", "\U0001F469\u200d\U0001F4BB", "co\u00adop", "a\u3000b", "a\u2028b", "\U000F0000", "\U0001FAE8 new emoji"]


@pytest.mark.parametrize("text", PASTED)
async def test_ask_pasted_text_accepted(gw, make_consumer, text):
    await notebook_with_sources(gw)
    await tools.ask_notebook(gw, make_consumer(scopes=READ), {"notebook": "Lighthouses", "question": text})
    assert gw.nlm.client.asks[-1][1] == text


async def test_ask_crlf_normalized(gw, make_consumer):
    await notebook_with_sources(gw)
    await tools.ask_notebook(gw, make_consumer(scopes=READ), {"notebook": "Lighthouses", "question": "one\r\ntwo\rthree"})
    assert gw.nlm.client.asks[-1][1] == "one\ntwo\nthree"
