"""Gateway error codes and the mapping from notebooklm-py exceptions.

Messages are fixed strings: upstream exception text can contain URLs or titles and
must never reach a caller, a log line, or the audit file.
"""
from __future__ import annotations

from notebooklm import AuthError, RateLimitError, SourceError

CODES = frozenset({
    "auth_expired", "quota_exceeded", "rate_limited", "source_rejected", "generation_timeout",
    "generation_failed", "upstream_changed", "gateway_restarted", "bad_request", "not_found",
})


class GatewayError(Exception):
    def __init__(self, code: str, message: str):
        if code not in CODES:
            raise ValueError(f"unknown error code: {code}")
        super().__init__(message)
        self.code = code
        self.message = message

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message}


class UploadTooLarge(GatewayError):
    def __init__(self, message: str = "upload exceeds the 200 MB cap"):
        super().__init__("bad_request", message)


class TooManyUploads(GatewayError):
    def __init__(self, limit: int):
        super().__init__("bad_request", f"too many pending uploads; use or let them expire (max {limit})")


def classify(exc: BaseException) -> GatewayError:
    if isinstance(exc, GatewayError):
        return exc
    if isinstance(exc, AuthError):
        return GatewayError("auth_expired", "Google session expired; the operator must re-run `notebooklm login --master-token`.")
    if isinstance(exc, RateLimitError):
        return GatewayError("rate_limited", "Google rate-limited the request; try again later.")
    if isinstance(exc, SourceError):  # before TimeoutError: SourceTimeoutError is both
        return GatewayError("source_rejected", "Google could not process a source.")
    if isinstance(exc, TimeoutError):
        return GatewayError("generation_timeout", "Generation did not finish within 30 minutes.")
    return GatewayError("upstream_changed", f"Unexpected NotebookLM error ({type(exc).__name__}); notebooklm-py may need upgrading.")
