"""Smoke-probe a running gateway: healthz + MCP initialize + tools/list + quota_status (no jobs created).
Usage: NLM_PROBE_TOKEN=nlm_... uv run python scripts/mcp_probe.py [base_url]"""
from __future__ import annotations

import asyncio
import os
import sys

import httpx
import httpx2
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client


async def main(base: str) -> int:
    print("healthz:", httpx.get(f"{base}/healthz").json())
    headers = {"Authorization": f"Bearer {os.environ['NLM_PROBE_TOKEN']}"}
    async with httpx2.AsyncClient(headers=headers, timeout=30) as hc:
        async with streamable_http_client(f"{base}/mcp", http_client=hc) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                print("tools:", [t.name for t in (await s.list_tools()).tools])
                res = await s.call_tool("quota_status", {})
                print("quota:", res.content[0].text)
                return 1 if res.is_error else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8773/notebooklm")))
