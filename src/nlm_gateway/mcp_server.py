"""Low-level MCP server whose tool list and calls are filtered by the authenticated consumer.

The consumer is placed in the ASGI scope state by the front door (http_app.py) and read
back through the SDK's request context (ctx.request is the Starlette Request of the current HTTP call).

Written against mcp 2.x (handler-callback API: Server(on_list_tools=..., on_call_tool=...));
the 1.x decorator API (@server.list_tools / @server.call_tool) does not exist there.
"""
from __future__ import annotations

import json

import mcp.types as types
from mcp.server.lowlevel import Server

from .errors import GatewayError, classify
from .tools import TOOLS


def build_server(gw) -> Server:
    by_name = {t.name: t for t in TOOLS}

    def consumer(ctx):
        return ctx.request.state.consumer

    def visible(c):
        return [t for t in TOOLS if t.scope is None or t.scope in c.scopes]

    async def list_tools(ctx, params) -> types.ListToolsResult:
        return types.ListToolsResult(
            tools=[types.Tool(name=t.name, description=t.description, inputSchema=t.input_schema) for t in visible(consumer(ctx))])

    async def call_tool(ctx, params: types.CallToolRequestParams) -> types.CallToolResult:
        c = consumer(ctx)
        spec = by_name.get(params.name)
        try:
            if spec is None or spec not in visible(c):
                raise GatewayError("not_found", "unknown tool")
            result = await spec.handler(gw, c, params.arguments or {})
        except Exception as e:
            return types.CallToolResult(isError=True, content=[types.TextContent(
                type="text", text=json.dumps(classify(e).to_dict(), separators=(",", ":")))])
        return types.CallToolResult(content=[types.TextContent(type="text", text=json.dumps(result, separators=(",", ":")))])

    return Server("notebooklm-gateway", on_list_tools=list_tools, on_call_tool=call_tool)
