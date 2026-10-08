import pytest

from nlm_gateway import quota
from nlm_gateway.errors import GatewayError

T0 = 1_760_000_000.0


def test_per_consumer_audio_cap(store, cfg, make_consumer):
    c = make_consumer(audio=2)
    for _ in range(2):
        quota.check(store, cfg, c, "podcast", T0)
        store.create_job(c.id, "podcast", {}, ["u"], [], T0)
    with pytest.raises(GatewayError) as e:
        quota.check(store, cfg, c, "podcast", T0)
    assert e.value.code == "quota_exceeded"
    quota.check(store, cfg, c, "podcast", T0 + quota.WINDOW_S + 1)  # rolled out of window


def test_account_cap_spans_consumers(store, cfg, make_consumer):
    # cfg: daily 6, reserve 2 -> account cap 4
    for i in range(4):
        store.create_job(f"c{i}", "podcast", {}, ["u"], [], T0)
    with pytest.raises(GatewayError, match="account"):
        quota.check(store, cfg, make_consumer(id="fresh", audio=5), "podcast", T0)


def test_failed_before_accept_not_counted(store, cfg, make_consumer):
    c = make_consumer(audio=1)
    j = store.create_job(c.id, "podcast", {}, ["u"], [], T0)
    store.update_job(j.id, T0, status="failed")
    quota.check(store, cfg, c, "podcast", T0)


def test_report_cap_independent(store, cfg, make_consumer):
    c = make_consumer(audio=0, report=1)
    quota.check(store, cfg, c, "report", T0)
    store.create_job(c.id, "report", {}, ["u"], [], T0)
    with pytest.raises(GatewayError):
        quota.check(store, cfg, c, "report", T0)


def test_snapshot(store, cfg, make_consumer):
    c = make_consumer(audio=3, report=10)
    store.create_job(c.id, "podcast", {}, ["u"], [], T0)
    store.create_job(c.id, "slides", {}, ["u"], [], T0)
    store.add_event(c.id, "chat", T0)
    assert quota.snapshot(store, cfg, c, T0) == {
        "audio": {"used": 1, "cap": 3}, "report": {"used": 0, "cap": 10},
        "account_audio": {"used": 1, "cap": 4, "reserve": 2},
        "account_report": {"used": 0, "cap": 95}, "account_slides": {"used": 1, "cap": 10},
        "account_infographic": {"used": 0, "cap": 10}, "account_chat": {"used": 1, "cap": 480}}


def test_report_account_cap_spans_consumers(store, cfg, make_consumer):
    from dataclasses import replace
    cfg = replace(cfg, account_report_daily=2)
    for i in range(2):
        store.create_job(f"c{i}", "report", {}, ["u"], [], T0)
    with pytest.raises(GatewayError, match="account") as e:
        quota.check(store, cfg, make_consumer(id="fresh", report=50), "report", T0)
    assert e.value.code == "quota_exceeded"


@pytest.mark.parametrize("kind,field", [("slides", "account_slides_daily"), ("infographic", "account_infographic_daily")])
def test_studio_kinds_account_cap_only(store, cfg, make_consumer, kind, field):
    from dataclasses import replace
    cfg = replace(cfg, **{field: 2})
    c = make_consumer(audio=0, report=0)  # per-consumer caps do not apply to these kinds
    for _ in range(2):
        quota.check(store, cfg, c, kind, T0)
        store.create_job(c.id, kind, {}, ["u"], [], T0)
    with pytest.raises(GatewayError, match="account") as e:
        quota.check(store, cfg, c, kind, T0)
    assert e.value.code == "quota_exceeded"
    j = store.create_job(c.id, kind, {}, ["u"], [], T0)  # failed-before-accept is not billable
    store.update_job(j.id, T0, status="failed")
    quota.check(store, cfg, c, kind, T0 + quota.WINDOW_S + 1)


def test_chat_cap_counts_events_rolling(store, cfg, make_consumer):
    from dataclasses import replace
    cfg = replace(cfg, account_chat_daily=2)
    c = make_consumer(audio=0, report=0)
    store.add_event("other", "chat", T0)
    quota.check(store, cfg, c, "chat", T0)
    store.add_event(c.id, "chat", T0)
    with pytest.raises(GatewayError, match="account") as e:
        quota.check(store, cfg, c, "chat", T0)
    assert e.value.code == "quota_exceeded"
    quota.check(store, cfg, c, "chat", T0 + quota.WINDOW_S + 1)
    assert store.count_events("chat", T0 - 1) == 2 and store.count_events("chat", T0 - 1, c.id) == 1
