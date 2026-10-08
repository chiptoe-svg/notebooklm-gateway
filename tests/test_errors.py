import pytest
import notebooklm

from nlm_gateway.errors import CODES, GatewayError, classify


def test_codes_are_exactly_the_spec_set():
    assert CODES == {"auth_expired", "quota_exceeded", "rate_limited", "source_rejected", "generation_timeout",
                     "generation_failed", "upstream_changed", "gateway_restarted", "bad_request", "not_found"}


def test_unknown_code_rejected():
    with pytest.raises(ValueError):
        GatewayError("nope", "x")


def test_classify_passthrough_and_fallbacks():
    g = GatewayError("bad_request", "x")
    assert classify(g) is g
    assert classify(TimeoutError()).code == "generation_timeout"
    assert classify(RuntimeError("weird")).code == "upstream_changed"


@pytest.mark.parametrize("name,code", [("AuthError", "auth_expired"), ("RateLimitError", "rate_limited"), ("SourceError", "source_rejected")])
def test_classify_upstream(name, code):
    cls = getattr(notebooklm, name)
    exc = cls.__new__(cls)  # bypass unknown constructor signatures
    assert classify(exc).code == code


def test_messages_never_echo_exception_text():
    assert "secret.example" not in classify(RuntimeError("https://secret.example/x")).message
