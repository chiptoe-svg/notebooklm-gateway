"""Configuration from .env plus process environment (environment wins)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


def parse_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip().strip('"').strip("'")
    return out


@dataclass(frozen=True)
class Config:
    host: str
    port: int
    public_base_url: str
    state_dir: Path
    timezone: str
    account_audio_daily: int
    account_audio_reserve: int
    notebook_source_cap: int
    workers: int = 2
    upload_max_bytes: int = 200 * 1024 * 1024
    upload_ttl_s: int = 2 * 3600
    generation_timeout_s: int = 30 * 60
    big_file_bytes: int = 45 * 1024 * 1024
    inline_text_max: int = 20 * 1024
    link_ttl_default_h: int = 24
    link_ttl_max_h: int = 168
    mcp_body_max: int = 1024 * 1024
    account_chat_daily: int = 480
    account_report_daily: int = 95
    account_slides_daily: int = 10
    account_infographic_daily: int = 10

    @property
    def account_audio_cap(self) -> int:
        return max(0, self.account_audio_daily - self.account_audio_reserve)


def load_config(env: Mapping[str, str] | None = None, env_file: Path | None = None) -> Config:
    merged: dict[str, str] = {}
    if env_file is not None:
        merged.update(parse_env_file(env_file))
    merged.update(os.environ if env is None else env)

    def need(key: str) -> str:
        if not merged.get(key):
            raise SystemExit(f"config: {key} is required")
        return merged[key]

    return Config(
        host=merged.get("NLM_HOST", "127.0.0.1"),
        port=int(merged.get("NLM_PORT", "8773")),
        public_base_url=need("NLM_PUBLIC_BASE_URL").rstrip("/"),
        state_dir=Path(merged.get("NLM_STATE_DIR", "state")).expanduser().resolve(),
        timezone=merged.get("NLM_TIMEZONE", "America/New_York"),
        account_audio_daily=int(need("NLM_ACCOUNT_AUDIO_DAILY")),
        account_audio_reserve=int(merged.get("NLM_ACCOUNT_AUDIO_RESERVE", "2")),
        notebook_source_cap=int(merged.get("NLM_NOTEBOOK_SOURCE_CAP", "50")),
        workers=int(merged.get("NLM_WORKERS", "2")),
        account_chat_daily=int(merged.get("NLM_ACCOUNT_CHAT_DAILY", "480")),
        account_report_daily=int(merged.get("NLM_ACCOUNT_REPORT_DAILY", "95")),
        account_slides_daily=int(merged.get("NLM_ACCOUNT_SLIDES_DAILY", "10")),
        account_infographic_daily=int(merged.get("NLM_ACCOUNT_INFOGRAPHIC_DAILY", "10")),
    )
