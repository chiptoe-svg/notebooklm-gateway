"""Upstream (notebooklm.*) log records are reduced to a fixed text: no message, args, exception or stack."""
import io
import logging

import pytest

from nlm_gateway import __main__ as entry

SENTINEL = "zq-secret-zq"


@pytest.fixture
def clean_logging():
    root, nlm = logging.getLogger(), logging.getLogger("notebooklm")
    saved_root, saved_level = root.handlers[:], root.level
    saved = {h: h.filters[:] for h in nlm.handlers}
    saved_nlm_filters = nlm.filters[:]
    root.handlers = []
    yield
    for h in root.handlers:
        h.close()
    root.handlers, root.level = saved_root, saved_level
    for h, f in saved.items():
        h.filters = f
    nlm.filters = saved_nlm_filters


def test_upstream_records_redacted(clean_logging):
    # stand-ins for basicConfig's root handler and notebooklm's own import-time handler
    root_buf, nlm_buf = io.StringIO(), io.StringIO()
    fmt = logging.Formatter("%(levelname)s %(name)s %(message)s")
    root_h, nlm_h = logging.StreamHandler(root_buf), logging.StreamHandler(nlm_buf)
    root_h.setFormatter(fmt)
    nlm_h.setFormatter(fmt)
    logging.getLogger().addHandler(root_h)
    logging.getLogger("notebooklm").addHandler(nlm_h)
    try:
        entry.setup_logging()
        try:
            raise RuntimeError(f"boom {SENTINEL}")
        except RuntimeError:
            logging.getLogger("notebooklm._chat").error("answer preview %s " + SENTINEL, SENTINEL, exc_info=True, stack_info=True)
        logging.getLogger("notebooklm").warning("raw %r", {"src": SENTINEL})
        logging.getLogger("nlm_gateway.jobs").warning("ours %s", "kept")
    finally:
        logging.getLogger("notebooklm").removeHandler(nlm_h)
    for out in (root_buf.getvalue(), nlm_buf.getvalue()):
        assert SENTINEL not in out and "boom" not in out and "Traceback" not in out and "Stack" not in out
        assert "upstream ERROR at test_main_logging:" in out and "upstream WARNING at test_main_logging:" in out
    assert "ours kept" in root_buf.getvalue()  # our own loggers are not rewritten


def test_setup_logging_idempotent(clean_logging):
    entry.setup_logging()
    entry.setup_logging()
    for h in logging.getLogger().handlers + logging.getLogger("notebooklm").handlers:
        assert sum(isinstance(f, entry.UpstreamRedactor) for f in h.filters) == 1


class _Keep(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


def test_root_path_alone_redacts_and_clears_record(clean_logging):
    """Without notebooklm's own handler, root's handler still rewrites the record and clears args/exc/stack."""
    nlm = logging.getLogger("notebooklm")
    saved = nlm.handlers[:]
    keep = _Keep()
    nlm.handlers = []
    logging.getLogger().addHandler(keep)
    try:
        entry.setup_logging()
        try:
            raise RuntimeError(SENTINEL)
        except RuntimeError:
            logging.getLogger("notebooklm._web.chat").error("raw %r", SENTINEL, exc_info=True, stack_info=True)
    finally:
        nlm.handlers = saved
    (rec,) = [r for r in keep.records if r.name.startswith("notebooklm")]
    assert rec.getMessage() == f"upstream ERROR at test_main_logging:{rec.lineno}"
    assert rec.args == () and rec.exc_info is None and rec.exc_text is None and rec.stack_info is None
