"""Entry point: uv run python -m nlm_gateway (reads ./.env)."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import uvicorn

from .config import load_config
from .consumers import Registry
from .gateway import build_gateway
from .http_app import create_app
from .nlm_client import NLM


class UpstreamRedactor(logging.Filter):
    """Rewrites every notebooklm.* record to a fixed text. Upstream log lines can carry caller content
    (answer previews, raw responses, source metadata); only level, module and line survive."""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name == "notebooklm" or record.name.startswith("notebooklm."):
            record.msg = f"upstream {record.levelname} at {record.module}:{record.lineno}"
            record.args = ()
            record.exc_info = record.exc_text = record.stack_info = None
        return True


def _add_once(target) -> None:
    if not any(isinstance(f, UpstreamRedactor) for f in target.filters):
        target.addFilter(UpstreamRedactor())


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    for noisy in ("httpx", "httpcore"):  # they log full request URLs at INFO
        logging.getLogger(noisy).setLevel(logging.WARNING)
    # Logger filters see only records logged on that exact logger; child records (notebooklm._chat...)
    # reach handlers by propagation, so the redactor also sits on notebooklm's own handlers and on root's.
    upstream = logging.getLogger("notebooklm")
    _add_once(upstream)
    for h in upstream.handlers + logging.getLogger().handlers:
        _add_once(h)


def main() -> int:
    setup_logging()
    cfg = load_config(env_file=Path(".env"))
    registry = Registry(cfg.state_dir / "consumers.json")
    if not registry.load():
        print("refusing to start: no consumers paired (run scripts/consumers.py pair)", file=sys.stderr)
        return 1
    gw = build_gateway(cfg, NLM(), registry)
    uvicorn.run(create_app(gw), host=cfg.host, port=cfg.port, log_level="info", proxy_headers=False, access_log=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
