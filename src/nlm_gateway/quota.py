"""Rolling-24h quotas computed from the job table (generation kinds) and the events table (chat).

Callers must call check() and Store.create_job() with no await in between, so that on
the single event loop the check-then-insert pair is atomic.
"""
from __future__ import annotations

from .errors import GatewayError

WINDOW_S = 24 * 3600
# kind -> (snapshot key, human label); per-consumer caps exist only for podcast and report
_ACCOUNT = {"podcast": ("account_audio", "podcast"), "report": ("account_report", "report"),
            "slides": ("account_slides", "slide deck"), "infographic": ("account_infographic", "infographic"),
            "chat": ("account_chat", "chat")}


def _account_cap(cfg, kind) -> int:
    return {"podcast": cfg.account_audio_cap, "report": cfg.account_report_daily, "slides": cfg.account_slides_daily,
            "infographic": cfg.account_infographic_daily, "chat": cfg.account_chat_daily}[kind]


def _used(store, kind, since, consumer_id=None) -> int:
    if kind == "chat":
        return store.count_events("chat", since, consumer_id)
    return store.count_billable(kind, since, consumer_id)


def check(store, cfg, consumer, kind, now) -> None:
    since = now - WINDOW_S
    if kind in ("podcast", "report"):
        cap = consumer.daily_audio_cap if kind == "podcast" else consumer.daily_report_cap
        if store.count_billable(kind, since, consumer.id) >= cap:
            raise GatewayError("quota_exceeded", f"daily {kind} limit for this consumer reached ({cap} per 24 h)")
    if _used(store, kind, since) >= _account_cap(cfg, kind):
        raise GatewayError("quota_exceeded", f"account-wide daily {_ACCOUNT[kind][1]} limit reached; try again later")


def snapshot(store, cfg, consumer, now) -> dict:
    since = now - WINDOW_S
    out = {
        "audio": {"used": store.count_billable("podcast", since, consumer.id), "cap": consumer.daily_audio_cap},
        "report": {"used": store.count_billable("report", since, consumer.id), "cap": consumer.daily_report_cap},
    }
    for kind, (key, _) in _ACCOUNT.items():
        out[key] = {"used": _used(store, kind, since), "cap": _account_cap(cfg, kind)}
    out["account_audio"]["reserve"] = cfg.account_audio_reserve
    return out
