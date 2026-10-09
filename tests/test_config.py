from pathlib import Path

import pytest

from nlm_gateway.config import load_config, parse_env_file

BASE = {"NLM_PUBLIC_BASE_URL": "https://gw.example/notebooklm/", "NLM_ACCOUNT_AUDIO_DAILY": "20"}


def test_defaults_and_trailing_slash(tmp_path):
    cfg = load_config(env={**BASE, "NLM_STATE_DIR": str(tmp_path)})
    assert cfg.host == "127.0.0.1" and cfg.port == 8773
    assert cfg.public_base_url == "https://gw.example/notebooklm"
    assert cfg.timezone == "America/New_York"
    assert cfg.notebook_source_cap == 50 and cfg.workers == 2
    assert cfg.account_audio_cap == 18  # 20 - default reserve 2


def test_reserve_never_negative(tmp_path):
    cfg = load_config(env={**BASE, "NLM_ACCOUNT_AUDIO_DAILY": "1", "NLM_ACCOUNT_AUDIO_RESERVE": "5", "NLM_STATE_DIR": str(tmp_path)})
    assert cfg.account_audio_cap == 0


def test_missing_required_fails_closed():
    with pytest.raises(SystemExit, match="NLM_PUBLIC_BASE_URL"):
        load_config(env={"NLM_ACCOUNT_AUDIO_DAILY": "3"})


def test_env_file_parsing_and_env_override(tmp_path):
    f = tmp_path / ".env"
    f.write_text('# comment\nNLM_PUBLIC_BASE_URL="https://a.example/notebooklm"\n\nNLM_ACCOUNT_AUDIO_DAILY=6\nNLM_PORT=9999\n')
    assert parse_env_file(f)["NLM_PUBLIC_BASE_URL"] == "https://a.example/notebooklm"
    cfg = load_config(env={"NLM_PORT": "8773", "NLM_STATE_DIR": str(tmp_path)}, env_file=f)
    assert cfg.port == 8773 and cfg.account_audio_daily == 6


def test_missing_env_file_is_empty(tmp_path):
    assert parse_env_file(tmp_path / "nope") == {}


def test_account_caps_defaults_and_env(tmp_path):
    cfg = load_config(env={**BASE, "NLM_STATE_DIR": str(tmp_path)})
    assert (cfg.account_chat_daily, cfg.account_report_daily, cfg.account_slides_daily, cfg.account_infographic_daily) == (480, 95, 10, 10)
    cfg = load_config(env={**BASE, "NLM_STATE_DIR": str(tmp_path), "NLM_ACCOUNT_CHAT_DAILY": "5", "NLM_ACCOUNT_REPORT_DAILY": "6",
                           "NLM_ACCOUNT_SLIDES_DAILY": "7", "NLM_ACCOUNT_INFOGRAPHIC_DAILY": "8"})
    assert (cfg.account_chat_daily, cfg.account_report_daily, cfg.account_slides_daily, cfg.account_infographic_daily) == (5, 6, 7, 8)


def test_max_pending_uploads(tmp_path):
    assert load_config(env={**BASE, "NLM_STATE_DIR": str(tmp_path)}).max_pending_uploads == 10
    cfg = load_config(env={**BASE, "NLM_STATE_DIR": str(tmp_path), "NLM_MAX_PENDING_UPLOADS": "100"})
    assert cfg.max_pending_uploads == 100
