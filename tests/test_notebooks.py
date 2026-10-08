import asyncio

import pytest

from nlm_gateway.errors import GatewayError
from nlm_gateway.notebooks import NamedNotebooks
from tests.fakes import FakeNLM


def nbs(store, nlm=None, cap=50):
    return NamedNotebooks(store, nlm or FakeNLM(), cap)


async def test_default_name_is_the_consumer_label_create_then_reuse(store, make_consumer):
    nlm = FakeNLM()
    w = nbs(store, nlm)
    c = make_consumer(label="To Review")
    nb1, t1 = await w.resolve(c, None, 2)
    nb2, t2 = await w.resolve(c, None, 1)
    assert nb1 == nb2 and t1 == t2 == "To Review" and nlm.client.creates == ["To Review"]
    assert store.get_notebook("To Review") == (nb1, 3)


async def test_existing_hand_made_notebook_is_reused(store, make_consumer):
    nlm = FakeNLM()
    mine = await nlm.client.notebooks.create("Reading list")  # made by the operator in the app
    nlm.client.creates.clear()
    nlm.client._add(mine.id, "already here")
    nb, title = await nbs(store, nlm).resolve(make_consumer(label="To Review"), "Reading list", 1)
    assert (nb, title) == (mine.id, "Reading list") and nlm.client.creates == []
    assert store.get_notebook("Reading list") == (mine.id, 2)  # upstream count seeded the cache


async def test_missing_name_is_created(store, make_consumer):
    nlm = FakeNLM()
    nb, title = await nbs(store, nlm).resolve(make_consumer(label="To Review"), "Lighthouses", 1)
    assert title == "Lighthouses" and nlm.client.creates == ["Lighthouses"] and nlm.client.notebooks_by_id[nb].title == "Lighthouses"


async def test_match_is_exact_and_case_sensitive(store, make_consumer):
    nlm = FakeNLM()
    await nlm.client.notebooks.create("to review")
    await nlm.client.notebooks.create("To Review (old)")
    nlm.client.creates.clear()
    await nbs(store, nlm).resolve(make_consumer(label="To Review"), None, 1)
    assert nlm.client.creates == ["To Review"]


async def test_concurrent_same_name_creates_once(store, make_consumer):
    nlm = FakeNLM()
    w = nbs(store, nlm)
    calls = [w.resolve(make_consumer(id=f"c{i % 2}", label="To Review"), None if i % 2 else "To Review", 1) for i in range(6)]
    results = await asyncio.gather(*calls)
    assert len({nb for nb, _ in results}) == 1 and nlm.client.creates == ["To Review"]


async def test_deleted_notebook_is_recreated(store, make_consumer):
    nlm = FakeNLM()
    w = nbs(store, nlm)
    c = make_consumer()
    nb1, title = await w.resolve(c, "Topic", 1)
    await nlm.client.notebooks.delete(nb1)
    nb2, title2 = await w.resolve(c, "Topic", 1)
    assert nb2 != nb1 and title2 == title == "Topic" and store.get_notebook(title) == (nb2, 1)


async def test_overflow_rolls_to_numbered_name(store, make_consumer):
    w = nbs(store, cap=3)
    c = make_consumer(label="To Review")
    _, t1 = await w.resolve(c, None, 2)
    _, t2 = await w.resolve(c, None, 2)
    _, t3 = await w.resolve(c, None, 2)
    assert (t1, t2, t3) == ("To Review", "To Review (2)", "To Review (3)")


async def test_overflow_reuses_existing_numbered_notebook(store, make_consumer):
    nlm = FakeNLM()
    full = await nlm.client.notebooks.create("Topic")
    for i in range(3):
        nlm.client._add(full.id, f"s{i}")
    second = await nlm.client.notebooks.create("Topic (2)")
    nlm.client.creates.clear()
    nb, title = await nbs(store, nlm, cap=3).resolve(make_consumer(), "Topic", 1)
    assert (nb, title) == (second.id, "Topic (2)") and nlm.client.creates == []


async def test_release(store, make_consumer):
    w = nbs(store)
    _, t = await w.resolve(make_consumer(), None, 3)
    w.release(t, 3)
    assert store.get_notebook(t)[1] == 0


async def test_different_labels_different_notebooks(store, make_consumer):
    w = nbs(store)
    a, _ = await w.resolve(make_consumer(id="a"), None, 1)
    b, _ = await w.resolve(make_consumer(id="b"), None, 1)
    assert a != b


async def test_adding_more_than_cap_is_rejected(store, make_consumer):
    nlm = FakeNLM()
    w = nbs(store, nlm, cap=3)
    with pytest.raises(GatewayError) as exc:
        await asyncio.wait_for(w.resolve(make_consumer(), None, 4), timeout=1)
    assert exc.value.code == "bad_request" and nlm.client.creates == []
