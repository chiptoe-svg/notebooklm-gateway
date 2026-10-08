"""read_note / create_note / get_source_text (scope notes)."""
import asyncio
import json
import logging
from datetime import datetime, timezone
from urllib.parse import urlsplit

import pytest
from notebooklm.types import Note

from nlm_gateway import tools
from nlm_gateway.errors import GatewayError

SECRET = "zq-secret-zq"
NOTES = ("notes",)


def note(nb_id, title, content, day):
    return Note(id=f"n-{title}-{day}", notebook_id=nb_id, title=title, content=content,
                created_at=datetime(2026, 3, day, tzinfo=timezone.utc))


async def setup_nb(gw, title="Lighthouses"):
    cl = gw.nlm.client
    nb = await cl.notebooks.create(title)
    cl.notes_data[nb.id] = [note(nb.id, "Plan", "old plan", 1), note(nb.id, "Ideas", "some ideas", 2),
                            note(nb.id, "Plan", "new plan", 3)]
    src = cl._add(nb.id, "Fresnel lens")
    src.text = "Lens text."
    cl.creates.clear()
    return nb, src


def audit_lines(gw):
    p = gw.cfg.state_dir / "audit.jsonl"
    return [json.loads(x) for x in p.read_text().splitlines()] if p.exists() else []


async def test_read_note_list_and_get(gw, make_consumer):
    nb, _ = await setup_nb(gw)
    c = make_consumer(scopes=NOTES)
    out = await tools.read_note(gw, c, {"notebook": "Lighthouses"})
    assert out == {"notes": [{"title": "Plan", "chars": 8}, {"title": "Ideas", "chars": 10}, {"title": "Plan", "chars": 8}]}
    assert await tools.read_note(gw, c, {"notebook": "Lighthouses", "title": "Ideas"}) == {"title": "Ideas", "text": "some ideas"}
    assert audit_lines(gw)[-1]["event"] == "read_note" and audit_lines(gw)[-1]["notebook_id"] == nb.id


async def test_read_note_ambiguous_returns_most_recent(gw, make_consumer):
    nb, _ = await setup_nb(gw)
    out = await tools.read_note(gw, make_consumer(scopes=NOTES), {"notebook": "Lighthouses", "title": "Plan"})
    assert out == {"title": "Plan", "text": "new plan", "ambiguous": True}
    gw.nlm.client.notes_data[nb.id].reverse()  # order must not matter, created_at does
    out = await tools.read_note(gw, make_consumer(scopes=NOTES), {"notebook": "Lighthouses", "title": "Plan"})
    assert out["text"] == "new plan"


async def test_read_note_truncates(gw, make_consumer):
    nb, _ = await setup_nb(gw)
    big = "y" * (gw.cfg.inline_text_max + 5)
    gw.nlm.client.notes_data[nb.id].append(note(nb.id, "Big", big, 4))
    out = await tools.read_note(gw, make_consumer(scopes=NOTES), {"notebook": "Lighthouses", "title": "Big"})
    assert out == {"title": "Big", "text": big[:gw.cfg.inline_text_max], "truncated": True}


async def test_read_note_unknown_title(gw, make_consumer):
    await setup_nb(gw)
    with pytest.raises(GatewayError) as e:
        await tools.read_note(gw, make_consumer(scopes=NOTES), {"notebook": "Lighthouses", "title": SECRET})
    assert e.value.code == "not_found" and SECRET not in e.value.message


async def test_create_note(gw, make_consumer):
    nb, _ = await setup_nb(gw)
    out = await tools.create_note(gw, make_consumer(scopes=NOTES), {"notebook": "Lighthouses", "title": "Plan", "text": "third plan"})
    assert out == {"title": "Plan", "created": True}
    cl = gw.nlm.client
    assert cl.note_creates == [(nb.id, "Plan", "third plan")] and cl.note_edits == []  # same title: a new note, never an edit
    assert [n.content for n in cl.notes_data[nb.id] if n.title == "Plan"] == ["old plan", "new plan", "third plan"]
    last = audit_lines(gw)[-1]
    assert last == {"ts": last["ts"], "event": "create_note", "consumer": "research", "notebook_id": nb.id, "text_chars": 10}


@pytest.mark.parametrize("args", [
    {"title": "T"}, {"title": "T", "text": ""}, {"title": "T", "text": "x" * 100001}, {"title": "T", "text": 5},
    {"text": "x"}, {"title": "", "text": "x"}, {"title": SECRET + "x" * 100, "text": "x"}, {"title": SECRET + "\na", "text": "x"},
])
async def test_create_note_validation(gw, make_consumer, args):
    await setup_nb(gw)
    with pytest.raises(GatewayError) as e:
        await tools.create_note(gw, make_consumer(scopes=NOTES), {"notebook": "Lighthouses", **args})
    assert e.value.code == "bad_request" and SECRET not in e.value.message and gw.nlm.client.note_creates == []


async def test_create_note_max_text_ok(gw, make_consumer):
    await setup_nb(gw)
    await tools.create_note(gw, make_consumer(scopes=NOTES), {"notebook": "Lighthouses", "title": "T", "text": "x" * 100000})
    assert len(gw.nlm.client.note_creates) == 1


async def test_get_source_text_inline(gw, make_consumer):
    nb, src = await setup_nb(gw)
    out = await tools.get_source_text(gw, make_consumer(scopes=NOTES), {"notebook": "Lighthouses", "source_title": "Fresnel lens"})
    assert out == {"title": "Fresnel lens", "chars": 10, "text": "Lens text."}
    assert gw.nlm.client.fulltext_calls == [(src.id, "text")]
    last = audit_lines(gw)[-1]
    assert last == {"ts": last["ts"], "event": "get_source_text", "consumer": "research", "notebook_id": nb.id, "chars": 10}


async def test_get_source_text_truncated_with_download_url(gw, make_consumer):
    nb, src = await setup_nb(gw, title="Light & houses")
    src.text = "z" * (gw.cfg.inline_text_max + 7)
    out = await tools.get_source_text(gw, make_consumer(scopes=NOTES), {"notebook": "Light & houses", "source_title": "Fresnel lens"})
    assert out["truncated"] is True and out["chars"] == gw.cfg.inline_text_max + 7
    assert out["text"] == src.text[:gw.cfg.inline_text_max]
    u = urlsplit(out["download_url"])
    assert f"{u.scheme}://{u.netloc}" == "https://gw.example" and u.path.startswith("/notebooklm/sources/text/") and not u.query
    from urllib.parse import unquote
    for leak in ("Light", "houses", "Fresnel", "lens"):
        assert leak not in unquote(out["download_url"])


async def test_get_source_text_ambiguous_and_unknown(gw, make_consumer):
    nb, src = await setup_nb(gw)
    dup = gw.nlm.client._add(nb.id, "Fresnel lens")
    dup.text = "second"
    c = make_consumer(scopes=NOTES)
    out = await tools.get_source_text(gw, c, {"notebook": "Lighthouses", "source_title": "Fresnel lens"})
    assert out == {"title": "Fresnel lens", "chars": 10, "text": "Lens text.", "ambiguous": True}
    with pytest.raises(GatewayError) as e:
        await tools.get_source_text(gw, c, {"notebook": "Lighthouses", "source_title": SECRET})
    assert e.value.code == "not_found" and SECRET not in e.value.message


@pytest.mark.parametrize("tool,args", [
    ("read_note", {"notebook": "Nope"}),
    ("create_note", {"notebook": "Nope", "title": "T", "text": "x"}),
    ("get_source_text", {"notebook": "Nope", "source_title": "S"}),
])
async def test_notes_tools_never_create_notebooks(gw, make_consumer, tool, args):
    await setup_nb(gw)
    with pytest.raises(GatewayError) as e:
        await getattr(tools, tool)(gw, make_consumer(scopes=NOTES), args)
    assert e.value.code == "not_found" and gw.nlm.client.creates == [] and gw.nlm.client.note_creates == []


@pytest.mark.parametrize("tool,args", [
    ("read_note", {"notebook": "Lighthouses"}),
    ("create_note", {"notebook": "Lighthouses", "title": "T", "text": "x"}),
    ("get_source_text", {"notebook": "Lighthouses", "source_title": "Fresnel lens"}),
])
async def test_notes_tools_need_notes_scope(gw, make_consumer, tool, args):
    await setup_nb(gw)
    with pytest.raises(GatewayError) as e:
        await getattr(tools, tool)(gw, make_consumer(scopes=("podcast", "report", "list", "read", "studio")), args)
    assert e.value.code == "not_found" and e.value.message == "unknown tool"
    assert gw.nlm.client.note_creates == [] and gw.nlm.client.fulltext_calls == []


@pytest.mark.parametrize("tool,args", [
    ("read_note", {"notebook": "A"}),
    ("create_note", {"notebook": "A", "title": "T", "text": "x"}),
    ("get_source_text", {"notebook": "A", "source_title": "S"}),
])
async def test_notes_tools_bounded(gw, make_consumer, monkeypatch, tool, args):
    async def stuck():
        await asyncio.Event().wait()
    gw.nlm.client.notebooks.list = stuck
    monkeypatch.setattr(tools, "READ_TIMEOUT_S", 0.05)
    with pytest.raises(GatewayError) as e:
        await getattr(tools, tool)(gw, make_consumer(scopes=NOTES), args)
    assert e.value.code == "generation_timeout"


async def test_notes_tools_leak_nothing(gw, make_consumer, caplog):
    nb, src = await setup_nb(gw, title="Nb " + SECRET)
    src.title, src.text = "Src " + SECRET, "Body " + SECRET
    gw.nlm.client.notes_data[nb.id].append(note(nb.id, "Note " + SECRET, "Text " + SECRET, 9))
    c = make_consumer(scopes=NOTES)
    with caplog.at_level(logging.DEBUG):
        await tools.read_note(gw, c, {"notebook": "Nb " + SECRET})
        await tools.read_note(gw, c, {"notebook": "Nb " + SECRET, "title": "Note " + SECRET})
        await tools.create_note(gw, c, {"notebook": "Nb " + SECRET, "title": "New " + SECRET, "text": "Body " + SECRET})
        await tools.get_source_text(gw, c, {"notebook": "Nb " + SECRET, "source_title": "Src " + SECRET})
    assert SECRET not in caplog.text and SECRET not in json.dumps(audit_lines(gw))


def test_notes_tool_specs():
    specs = {t.name: t for t in tools.TOOLS}
    for name, req in (("read_note", ["notebook"]), ("create_note", ["notebook", "title", "text"]),
                      ("get_source_text", ["notebook", "source_title"])):
        assert specs[name].scope == "notes" and specs[name].input_schema["required"] == req


async def test_long_existing_titles_can_be_looked_up(gw, make_consumer):
    nb, src = await setup_nb(gw)
    src.title = "L" * 300
    gw.nlm.client.notes_data[nb.id].append(note(nb.id, "N" * 300, "long-titled", 5))
    c = make_consumer(scopes=NOTES)
    assert (await tools.get_source_text(gw, c, {"notebook": "Lighthouses", "source_title": "L" * 300}))["text"] == "Lens text."
    assert (await tools.read_note(gw, c, {"notebook": "Lighthouses", "title": "N" * 300}))["text"] == "long-titled"
    with pytest.raises(GatewayError):
        await tools.read_note(gw, c, {"notebook": "Lighthouses", "title": "N" * 1001})


async def test_create_note_multiline_accepted(gw, make_consumer):
    await setup_nb(gw)
    await tools.create_note(gw, make_consumer(scopes=NOTES), {"notebook": "Lighthouses", "title": "T", "text": "a\n\tb\n"})
    assert gw.nlm.client.note_creates[-1][2] == "a\n\tb\n"


@pytest.mark.parametrize("bad", ["\x00", "\x07", "\x1b", "\x7f", "\u202e", "\u202a", "\u2067", "\u2069"])
async def test_create_note_control_chars_rejected(gw, make_consumer, bad):
    await setup_nb(gw)
    with pytest.raises(GatewayError) as e:
        await tools.create_note(gw, make_consumer(scopes=NOTES), {"notebook": "Lighthouses", "title": "T", "text": f"a{bad}b {SECRET}"})
    assert e.value.code == "bad_request" and SECRET not in e.value.message and gw.nlm.client.note_creates == []


async def test_create_note_timeout_is_reported_as_unconfirmed(gw, make_consumer, monkeypatch):
    await setup_nb(gw)

    async def stuck(*a, **kw):
        await asyncio.Event().wait()
    gw.nlm.client.notes.create = stuck
    monkeypatch.setattr(tools, "READ_TIMEOUT_S", 0.05)
    with pytest.raises(GatewayError) as e:
        await tools.create_note(gw, make_consumer(scopes=NOTES), {"notebook": "Lighthouses", "title": "T", "text": "x"})
    assert e.value.code == "generation_timeout"
    assert e.value.message == "NotebookLM did not confirm the note within 50 s; check with read_note before retrying."


PASTED = ["a\u00a0b", "\U0001F469\u200d\U0001F4BB", "co\u00adop", "a\u3000b", "a\u2028b", "\U000F0000", "\U0001FAE8 new emoji"]


@pytest.mark.parametrize("text", PASTED)
async def test_create_note_pasted_text_accepted(gw, make_consumer, text):
    await setup_nb(gw)
    await tools.create_note(gw, make_consumer(scopes=NOTES), {"notebook": "Lighthouses", "title": "T", "text": text})
    assert gw.nlm.client.note_creates[-1][2] == text


async def test_create_note_crlf_normalized(gw, make_consumer):
    await setup_nb(gw)
    await tools.create_note(gw, make_consumer(scopes=NOTES), {"notebook": "Lighthouses", "title": "T", "text": "one\r\ntwo\rthree\r\n"})
    assert gw.nlm.client.note_creates[-1][2] == "one\ntwo\nthree\n"
