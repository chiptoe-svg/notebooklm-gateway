"""Owns the single long-lived NotebookLMClient.

The master token (master_token.json beside storage_state.json, written by
`notebooklm login --master-token`) lets the library re-mint cookies by itself; this
module never reads it. If start() fails, the gateway still serves /healthz and tools
return auth_expired until a later health check succeeds.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging

from .errors import GatewayError, classify

log = logging.getLogger("nlm_gateway.nlm")


class NLM:
    def __init__(self, profile: str | None = None):
        self.profile = profile
        self.client = None
        self._ctx = None
        self.auth_ok = False
        self.last_error: str | None = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lock:
            if self.client is not None:
                return
            from notebooklm import NotebookLMClient
            from notebooklm.options import ClientConfig, RetryOptions, WebBackendConfig, WebSessionOptions
            config = ClientConfig(
                backend=WebBackendConfig(session=WebSessionOptions(keepalive_interval=600)),
                retry=RetryOptions(rate_limit_max_retries=2),
            )
            ctx = None
            try:
                ctx = NotebookLMClient.from_storage(profile=self.profile, config=config)
                self.client = await ctx.__aenter__()
                self._ctx = ctx
            except Exception as e:  # missing/expired storage: stay up, report auth_failed
                if ctx is not None:
                    with contextlib.suppress(Exception):
                        await ctx.__aexit__(type(e), e, e.__traceback__)
                self._ctx, self.client = None, None
                self.auth_ok, self.last_error = False, classify(e).code
                log.warning("NotebookLM client did not start: %s", type(e).__name__)
                return
        await self.health_check()

    async def health_check(self) -> bool:
        if self.client is None:  # start() only calls health_check() once a client exists: bounded
            await self.start()
            return self.auth_ok
        try:
            await self.client.notebooks.list()
            self.auth_ok, self.last_error = True, None
        except Exception as e:
            self.auth_ok, self.last_error = False, classify(e).code
            log.warning("NotebookLM health check failed: %s", self.last_error)
        return self.auth_ok

    async def stop(self) -> None:
        try:
            if self._ctx is not None:
                await self._ctx.__aexit__(None, None, None)
        finally:
            self._ctx, self.client = None, None
            self.auth_ok = False

    def require(self):
        if self.client is None or not self.auth_ok:
            raise GatewayError("auth_expired", "NotebookLM login is not available; the operator must re-run `notebooklm login --master-token`.")
        return self.client
